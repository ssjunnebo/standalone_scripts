#!/usr/bin/env python
from ngi_pipeline.database.classes import CharonSession, CharonError
import os
import glob
import click

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

#TODO: add option sample_list that can filter out specific samples to run analysis for from a list provided
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

                sbatch_file = make_sbatch_script(sample_name, tsv_file_path)

                if not no_submit_jobs:
                    submit_sbatch_job(sample_name)   # TODO: Should catch the output
                    update_analysis_status(sample_name)
                else:
                    print("Generated files. Not submitting jobs.")
                    break
            else:
                print("Issue locating fastq files - Not analyzing sample: " + sample_name) # TODO: Log/warn this
        else:
            print("Sample status not 'TO_ANALYZE' - Not analysing sample: " + sample_name)


def get_samples_from_charon(sample_entries_from_charon):
    """TODO: Given a project ID, get a list of samples from Charon"""
    samples = []
    for sample_entry in sample_entries_from_charon:
        sample_name = sample_entry.get("sampleid")
        samples.append(sample_name)

    return samples


def check_analysis_status(sample):
    """TODO: Given a sample id, connect to charon and
    return the analysis status
    """
    if sample == 'P9451_401' or sample == 'P9451_402':
        analysis_status = "TO_ANALYZE"
    else:
        analysis_status = "ANALYSED"

    return analysis_status


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


def make_sbatch_script(sample, tsv):
    """TODO: Given a sample, make the sbatch script to start a Sarek run"""
    print('Writing sbatch script for ' + sample)

    return


def submit_sbatch_job(sample):
    """TODO: Given a sample, submit the Sarek run"""
    print("Submitting sbatch job for " + sample)
    # cd <analysis_path>
    # sbatch -J sample_id_sarek -e <sample_id>_sarek.err -o <sample_id>_sarek.out /proj/ngi2016003/nobackup/start_sarek/run_germline.sbatch <project_id> <sample_id>

    return


def update_analysis_status(sample):
    """TODO: Given a sample, update the analysis status in Charon"""
    print("Updated the analys status in Charon for " + sample)

    return


if __name__ == '__main__':
    start_sarek()
