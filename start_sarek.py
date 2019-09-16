#!/usr/bin/env python
#from ngi_pipeline.database.classes import CharonSession, CharonError
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
            make_tsv(sample, gender, project_id)
            make_sbatch_script(sample)
            if not no_submit_jobs:
                submit_sbatch_job(sample)   #Should catch the output
                update_analysis_status(sample)
            else:
                print("Generated files. Not submitting jobs.")
                break
        else:
            print("NOT ANALYZING: " + sample)


def get_samples_from_charon(pid):
    """Given a project ID, get a list of samples from Charon"""
    samples = ['sample1', 'sample4', 'sample5']
    return samples

def check_analysis_status(sample):
    """Given a sample id, connect to charon and return the analysis status"""
    if sample == 'sample1' or sample == 'sample4':
        analysis_status = "TO_ANALYZE"
    else:
        analysis_status = "ANALYSED"
    return analysis_status

def make_tsv(sample, gender, project_id):
    """Given a sample and a project ID, generate a tsv file for input to Sarek"""
    print('Writing tsvfile for ' + sample)
    return

def make_sbatch_script(sample):
    print('Writing sbatch script for ' + sample)
    return

def submit_sbatch_job(sample):
    print("Submitting sbatch job for " + sample)
    return

def update_analysis_status(sample):
    print("Updated the analys status in Charon for " + sample)
    return





if __name__ == '__main__':
    start_sarek()
