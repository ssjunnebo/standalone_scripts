#!/usr/bin/env python
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.utils.slurm import get_slurm_job_status
from ngi_pipeline.utils.slurm import get_slurm_job_status
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
import os
import re
import glob
import click
import subprocess


@click.command()
@click.option("--genome", type=click.Choice(["GRCh37", "GRCh38"]),
              required=True,
              help="Run Sarek with GRCh37 or GRCh38")
@click.option("--no_submit_jobs", is_flag=True,
              help="Only generate the tsv files and sbatch scripts")
@click.argument("project_id")


def start_sarek(project_id, genome, no_submit_jobs):
    """Given a project ID, launch Sarek analysis for all samples in that project
    with analysis status set to 'TO_ANALYZE' in Charon.
    """
    try:
        charon_connection = CharonSession()
        sample_entries = charon_connection.project_get_samples(project_id).get("samples", {}) # This should chek if the project exists
    except:
        print("There was an issue connecting to charon, stopping.")
    if not sample_entries:
        print("Could not find any samples for project " + project_id + ". Stopping")
        return

    try:
        config = load_yaml_config(locate_ngi_config())
        project_base_path = config["start_sarek"]["project_base_path"]
        template_path = config["start_sarek"]["sbatch_template"]
    except:
        print("Issue reading the ngi config file, stopping")

    for sample_entry in sample_entries:
        sample_name = sample_entry.get("sampleid")
        sample_status = sample_entry.get("analysis_status")
        if sample_status == "TO_ANALYZE":
            sample_data_paths = get_fastq_files(project_base_path, project_id, sample_name)

            if sample_data_paths:
                try:
                    analysis_path = os.path.join(project_base_path, "ANALYSIS", project_id, "sarek_ngi", sample_name)
                    make_analysis_dir(analysis_path)

                    tsv_file_path = os.path.join(analysis_path, sample_name + ".tsv")
                    make_tsv(sample_name, project_id, sample_data_paths, tsv_file_path)

                    sbatch_file_path = os.path.join(analysis_path, "run_germline" + sample_name + ".sbatch")
                    make_sbatch_script(sample_name, project_id, genome, sbatch_file_path, template_path)
                except:
                    print("Issue setting up Sarek run for sample " + sample_name + ". Stopping.")

                if no_submit_jobs:
                    print("Generated files. Not submitting jobs.")
                    break
                else:
                    try:
                        job_id = submit_sbatch_job(sample_name, analysis_path, sbatch_file_path)
                        print("Sarek job " + str(job_id) + " submitted for sample " + sample_name)
                    except:
                        print("Issue submitting Sarek job for sample " + sample_name)
                        break

                    try:
                        charon_connection.sample_update(project_id, sample_name, analysis_status="UNDER_ANALYSIS")
                        print("Updated analysis status in charon for " + sample_name)
                    except:
                        print("Issue updating charon with job status.")
                        break
            else:
                print("Issue locating fastq files - Not analyzing sample: " + sample_name)
        else:
            print("Sample status not 'TO_ANALYZE' - Not analysing sample: " + sample_name)


def get_fastq_files(project_path, project_id, sample_id):
    """Given a project and sample ID, return a list of paths to the fastq files.
    If the files don't exist, an empty list is returned.
    """
    path_pattern = os.path.join(project_path, "DATA", project_id, sample_id, "*/*/*R1*.gz")
    sample_fastq_paths = glob.glob(path_pattern)

    return sample_fastq_paths


def make_tsv(sample, project, sample_fastq_paths, tsv_file_path):
    """Given sample information, generate a tsv file for input to Sarek"""
    if os.path.isfile(tsv_file_path):
        os.remove(tsv_file_path)
    for index, frw_fastq in enumerate(sample_fastq_paths, 1):
        rev_fastq = frw_fastq.replace("_R1_","_R2_")
        lane_nr = str(index)
        if os.path.isfile(frw_fastq) and os.path.isfile(rev_fastq):
            with open(tsv_file_path, "a") as f:
                f.write(sample + "\t"
                        + "ZZ" + "\t"   # TODO: implement gender option for somatic
                        + "0" + "\t"
                        + sample + "\t"
                        + sample + "_" + lane_nr + "\t"
                        + frw_fastq + "\t"
                        + rev_fastq + "\n")
        else:
            print("Issue locating one or more fastq files: \n" + frw_fastq + "\n" + rev_fastq + "\nSkipping lane.")
    print("Writing tsv file in " + tsv_file_path)

    return


def make_analysis_dir(analysis_path):
    """Check if the given path exists and creat it if it doesn't"""
    if not os.path.exists(analysis_path):
        os.makedirs(analysis_path)

    return


def make_sbatch_script(sample, project_id, reference, sbatch_path, template):
    """Given a sample, make the sbatch script to start a Sarek run"""
    placeholders = ("PROJECT_ID", "SAMPLE_ID", "REFERENCE")
    replacements = (project_id, sample, reference)
    with open(template, "r") as infile:
        with open(sbatch_path, "w") as outfile:
            for line in infile:
                for placeholder, replacement in zip(placeholders, replacements):
                    line = line.replace(placeholder, replacement)
                outfile.write(line)
    print("Writing sbatch script " + sbatch_path)

    return


def submit_sbatch_job(sample, analysis_dir, sbatch_path):
    """Given a sample, script and project, submit the Sarek run"""
    process_handle = subprocess.Popen(["sbatch",
                     "-J", sample + "_sarek",
                     "-e", os.path.join(analysis_dir, sample + "_sarek.err"),
                     "-o", os.path.join(analysis_dir, sample + "_sarek.out"),
                    sbatch_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    process_out, process_err = process_handle.communicate()
    try:
        slurm_job_id = re.match(r"Submitted batch job (\d+)", process_out).groups()[0]
    except AttributeError:
        raise RuntimeError("Could not submit sbatch file '{}': "
                           "{}".format(sbatch_path, process_err))
    return int(slurm_job_id)


if __name__ == "__main__":
    start_sarek()
