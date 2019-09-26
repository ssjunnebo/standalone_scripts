#!/usr/bin/env python
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.utils.slurm import get_slurm_job_status
from ngi_pipeline.utils.slurm import get_slurm_job_status
from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from ngi_pipeline.engines.sarek.process import ProcessConnector
from ngi_pipeline.engines.piper_ngi.database import get_db_session, SampleAnalysis
import os
import glob
import click
import subprocess
import time
import contextlib


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
    config = load_yaml_config(locate_ngi_config())
    project_base_path = config['start_sarek']['project_base_path'] # TODO: should this be the path for the uppmax project or the analysis project?
    project_id = config['environment']['project_id']
    template_path = config['start_sarek']['sbatch_template']

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
            sample_data_paths = get_fastq_files(project_base_path, project_id, sample_name)

            if sample_data_paths: # TODO: this will only indicate if the R1 files exsist. Should also look for the R2 files somewhere
                analysis_path = os.path.join(project_base_path, 'ANALYSIS', project_id, 'sarek_ngi', sample_name)
                make_analysis_dir(analysis_path)

                tsv_file_path = os.path.join(analysis_path, sample_name + '.tsv')
                make_tsv(sample_name, gender, project_id, sample_data_paths, tsv_file_path)

                sbatch_file_path = os.path.join(analysis_path, "run_germline" + sample_name + ".sbatch")
                make_sbatch_script(sample_name, project_id, sbatch_file_path, template_path)

                if no_submit_jobs:
                    print("Generated files. Not submitting jobs.")
                    break
                else:
                    job_id = submit_sbatch_job(sample_name, sbatch_file_path)
                    charon_connection.sample_update(project_id, sample_name, analysis_status='UNDER_ANALYSIS')
                    print("Updated analysis status in charon for " + sample_name) # TODO: Log this and add check that the status was updated

                    # Update local tracking database with jobinfo
                    time.sleep(3)

                    db_obj = SampleAnalysis(
                        project_id=project_id,
                        project_name=projec_tid,
                        sample_id=sample_name,
                        project_base_path=project_base_path,
                        workflow='SarekGermlineAnalysis',  # TODO: implement for somatic later
                        engine='sarek',
                        analysis_dir=analysis_path,
                        **{'slurm_job_id': job_id})

                    with db_session(config) as db_session:
                        db_session.add(db_obj)
                        db_session.commit()

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


def get_fastq_files(project_path, project_id, sample_id):
    """Given a project and sample ID, return a list of paths to the fastq files.
    If the files don't exist, an empty list is returned.
    """
    path_pattern = os.path.join(project_path, 'DATA', project_id, sample_id, '*/*/*R1*.gz')
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


def make_sbatch_script(sample, project_id, sbatch_path, template):
    """Given a sample, make the sbatch script to start a Sarek run"""
    placeholders = ("PROJECT_ID", "SAMPLE_ID")
    replacements = (project_id, sample)
    with open(template, "r") as infile:
        with open(sbatch_path, 'w') as outfile:
            for line in infile:
                for placeholder, replacement in zip(placeholders, replacements):
                    line = line.replace(placeholder, replacement)
                outfile.write(line)
    print('Writing sbatch script for ' + sample)

    return


def submit_sbatch_job(sample, sbatch_path):
    """Given a sample, script and project, submit the Sarek run"""
    process_handle = subprocess.Popen(["sbatch",
                     "-J", sample + "_sarek",
                     "-e", sample + "_sarek.err",
                     "-o", sample + "_sarek.out",
                     sbatch_path]) # sbatch script already contains pid and sample id

    process_out, process_err = process_handle.communicate()
    print("Submitting sbatch job for " + sample) # TODO: log this and also print the job id
    try:
        slurm_job_id = re.match(r'Submitted batch job (\d+)', process_out).groups()[0]
    except AttributeError:
        raise RuntimeError('Could not submit sbatch file "{}": '
                           '{}'.format(sbatch_path, process_err))
    return int(slurm_job_id)

@contextlib.contextmanager
def db_session(conf):
"""
Context manager for the database session
"""
    with get_db_session(config=conf) as db_session:
        tracking_session = db_session
        yield tracking_session


if __name__ == '__main__':
    start_sarek()
