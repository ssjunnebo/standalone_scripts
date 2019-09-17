#!/usr/bin/env python
#from ngi_pipeline.database.classes import CharonSession, CharonError
import os
import glob
import click

@click.command()
@click.option('--gender', type=click.Choice(['XX', 'XY']), help="Gender of sample")
@click.option('--sample_list', type=click.Path(exists=True), default=None,
              help='Optional list of sample ids to include in the analysis run')
@click.option('--no_submit_jobs', is_flag=True, help='Only generate the tsv files and sbatch scripts, dont submit the jobs')
@click.argument('project_id')

def start_sarek(project_id, gender, sample_list, no_submit_jobs):
    """Given a project ID, launch Sarek analysis for all samples in that project with analysis status set to 'TO_ANALYZE' in Charon."""
    if sample_list:
        with open(sample_list, 'r') as sample_input_file:
            samples_to_analyse = sample_input_file.read().splitlines()
    else:
        samples_to_analyse = get_samples_from_charon(project_id)

    for sample in samples_to_analyse:
        sample_status = check_analysis_status(sample)
        if sample_status == 'TO_ANALYZE':
            sample_data_paths = get_fastq_files(project_id, sample)
            if fastq_files_exist(sample_data_paths):
                make_tsv(sample, gender, project_id, sample_data_paths)
                make_sbatch_script(sample)
                if not no_submit_jobs:
                    submit_sbatch_job(sample)   #Should catch the output
                    update_analysis_status(sample)
                else:
                    print("Generated files. Not submitting jobs.")
                    break
            else:
                print("Issue locating fastq files - Not analyzing sample: " + sample) #TODO: Log/warn this
        else:
            print("Sample status not 'TO_ANALYZE' - Not analysing sample: " + sample)


def get_samples_from_charon(pid):
    """TODO: Given a project ID, get a list of samples from Charon"""
    samples = ['P9451_401', 'P9451_402']
    return samples

def check_analysis_status(sample):
    """TODO: Given a sample id, connect to charon and return the analysis status"""
    if sample == 'P9451_401':
        analysis_status = "TO_ANALYZE"
    else:
        analysis_status = "ANALYSED"
    return analysis_status

def get_fastq_files(pid, sampleid):
    """Given a project and sample ID, return a list of paths to the fastq files. If the files don't exist, an empty list is returned."""
    path_pattern = os.path.join('/Users/sara.sjunnebo/code/scratch/DATA', pid, sampleid, '*/*/*.gz') # Assuming R1 and R2 are the only .gz files in the data dir
    sample_fastq_paths = glob.glob(path_pattern)
    return sample_fastq_paths

def fastq_files_exist(files):
    """Given a list of paths, check if there are two paths"""
    if len(files) != 2:
        return False
    else:
        return True

def make_tsv(sample, gender, project_id, sample_fastq_paths):
    """TODO: Given a sample and a project ID, generate a tsv file for input to Sarek"""
    print('Writing tsvfile for ' + sample)
    return

def make_sbatch_script(sample):
    """TODO: Given a sample, make the sbatch script to start a Sarek run"""
    print('Writing sbatch script for ' + sample)
    return

def submit_sbatch_job(sample):
    """TODO: Given a sample, submit the Sarek run"""
    print("Submitting sbatch job for " + sample)
    return

def update_analysis_status(sample):
    """TODO: Given a sample, update the analysis status in Charon"""
    print("Updated the analys status in Charon for " + sample)
    return



if __name__ == '__main__':
    start_sarek()
