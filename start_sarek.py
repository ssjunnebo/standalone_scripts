#!/usr/bin/env python
from ngi_pipeline.database.classes import CharonSession, CharonError
import os
import glob
import click
import subprocess

@click.command()
@click.option('--gender', type=click.Choice(['XX', 'XY']),
              help="Gender of sample")
@click.option('--sample_list', type=click.Path(exists=True), default=None,
              help="Optional list of sample ids to include in the analysis run")
@click.option('--no_submit_jobs', is_flag=True,
              help="Only generate the tsv files and sbatch scripts")
@click.option('--mode', type=click.Choice(['germline', 'somatic']),
              help="Run Sarek germline or somatic")
@click.argument('project_id')


def start_sarek(project_id, gender, sample_list, no_submit_jobs, mode):
    """Given a project ID, launch Sarek analysis for all samples in that project
    with analysis status set to 'TO_ANALYZE' in Charon.
    """
    charon_connection = CharonSession()
    sample_entries = charon_connection.project_get_samples(project_id).get("samples", {})   # Returns a list with one dict per sample
 
#TODO: add option sample_list that can filter out specific samples to run analysis for from a list provided. if sample_name in samples_to_analyse
#    if sample_list:
#        with open(sample_list, 'r') as sample_input_file:
#            samples_to_analyse = sample_input_file.read().splitlines()
#    else:
#        samples_to_analyse = get_samples_from_charon(sample_entries)

    for sample_entry in sample_entries:
        sample_name = sample_entry.get("sampleid")
        sample_status = sample_entry.get("analysis_status")
        if sample_status == 'TO_ANALYZE':
            sample_data_paths = get_fastq_files(project_id, sample_name)

            if sample_data_paths: # TODO: this will only indicate if the R1 files exsist. Should also look for the R2 files somewhere
                analysis_path = os.path.join('/Users/sara.sjunnebo/code/scratch/ANALYSIS', project_id, 'sarek_ngi', sample_name) # TODO: get path from config file?
                make_analysis_dir(analysis_path)

                tsv_file_path = os.path.join(analysis_path, sample_name + '.tsv')
                make_tsv(sample_name, gender, project_id, sample_data_paths, tsv_file_path)

                sbatch_file_path = os.path.join(analysis_path, "run_germline" + sample_name + ".sbatch")
                make_sbatch_script(sample_name, project_id, sbatch_file_path)

                if no_submit_jobs:
                    print("Generated files. Not submitting jobs.")
                    break
                else:
                    submit_sbatch_job(sample_name, sbatch_file_path)   # TODO: Should catch the output
                    charon_connection.sample_update(project_id, sample_name, analysis_status='UNDER_ANALYSIS')
                    print("Updated analysis status in charon for " + sample_name) # TODO: Log this and add check that the status was updated

                # TODO: When the sbatch job is done, get the status (success/fail) and update charon (ANALYZED)
            else:
                print("Issue locating fastq files - Not analyzing sample: " + sample_name) # TODO: Log/warn this
        else:
            print("Sample status not 'TO_ANALYZE' - Not analysing sample: " + sample_name)


#def get_samples_from_charon(sample_entries_from_charon):
#    """Given a project ID, get a list of samples from Charon"""
#    samples = []
#    for sample_entry in sample_entries_from_charon:
#        sample_name = sample_entry.get("sampleid")
#        samples.append(sample_name)
#
#    return samples


def get_fastq_files(pid, sampleid):
    """Given a project and sample ID, return a list of paths to the fastq files.
    If the files don't exist, an empty list is returned.
    """
    path_pattern = os.path.join('/Users/sara.sjunnebo/code/scratch/DATA', pid, sampleid, '*/*/*R1*.gz') # TODO: get path from config file? 
    sample_fastq_paths = glob.glob(path_pattern)

    return sample_fastq_paths


def make_tsv(sample, gender, project, sample_fastq_paths, tsv_file_path):
    """Given sample information, generate a tsv file for input to Sarek"""
    for index, frw_fastq in enumerate(sample_fastq_paths, 1):
        rev_fastq = frw_fastq.replace('_R1_','_R2_')
        lane_nr = str(index)
        if os.path.isfile(frw_fastq) and os.path.isfile(frw_fastq):
            with open(tsv_file_path, 'a') as f:                       # TODO: If the file exists, it will just get appended to. Remove first?
                f.write(sample + '\t'
                        + gender + '\t'
                        + '0' + '\t'
                        + sample + '\t'
                        + sample + '_' + lane_nr + '\t'
                        + frw_fastq + '\t'
                        + rev_fastq + '\n')
    print('Writing tsv file in ' + tsv_file_path) # TODO: log instead

    return


def make_analysis_dir(analysis_path):
    """Check if the given path exists and creat it if it doesn't"""
    if not os.path.exists(analysis_path):
        os.makedirs(analysis_path)

    return


def make_sbatch_script(sample, project_id, sbatch_path):
    """Given a sample, make the sbatch script to start a Sarek run"""
    placeholders = ("PROJECT_ID", "SAMPLE_ID")
    replacements = (project_id, sample)
    with open("/Users/sara.sjunnebo/code/scratch/run_germline_template.sbatch", "r") as infile:   # TODO: Get file path from config?
        with open(sbatch_path, 'w') as outfile:
            for line in infile:
                for placeholder, replacement in zip(placeholders, replacements):
                    line = line.replace(placeholder, replacement)
                outfile.write(line)
    print('Writing sbatch script for ' + sample)

    return


def submit_sbatch_job(sample, sbatch_path):
    """Given a sample, script and project, submit the Sarek run"""
#    subprocess.call(["sbatch",
#                     "-J", sample + "_sarek",
#                     "-e", sample + "_sarek.err",
#                     "-o", sample + "_sarek.out",
#                     sbatch_path) # sbatch script already contains pid and sample id
    subprocess.call(["echo", "sbatch -J {0}_sarek -e {0}_sarek.err -o {0}_sarek.out {1}".format(sample, sbatch_path)]) # TODO: run job instead of echo, see above line
    print("Submitting sbatch job for " + sample) # TODO: log this and also print the job id

    return


if __name__ == '__main__':
    start_sarek()
