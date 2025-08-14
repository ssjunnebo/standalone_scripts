import click as cli
import pandas as pd

NT_COMPLIMENT = {
    'A': 'T',
    'T': 'A',
    'C': 'G',
    'G': 'C'
}

def load_manifest(path):
    """Load the manifest from the given path."""
    with open(path, 'r') as file:
        manifest_content = file.read()
    header = manifest_content.split("[SAMPLES]")[0]
    samples = manifest_content.split("[SAMPLES]")[1].strip().split("\n")
    return header, samples

def load_sample_dataframe(manifest_data):
    """Extract the sample data from the manifest and convert it to a DataFrame."""
    header = manifest_data[0]
    sample_rows = manifest_data[1:]

    sample_dicts = []
    for row in sample_rows:
        row_dict = dict(zip(header.split(","), row.split(",")))
        sample_dicts.append(row_dict)

    samples_info = pd.DataFrame.from_dict(sample_dicts)
    return samples_info

def reverse_complement_index(index):
    """Return the reverse complement of a given index."""
    return ''.join(NT_COMPLIMENT[nuc] for nuc in reversed(index))


@cli.command()
@cli.option('--manifest_path', required=True, help='Path to the sample manifest. e.g. ~/fc/AVITI_run_manifest_2450545934_24-1214961_250722_154957_EunkyoungChoi_untrimmed.csv')
@cli.option('--project', required=False, help='Project ID, e.g. P10001. Only the indexes of samples with this specific project ID will be changed')
@cli.option('--swap', is_flag=True, help='Swaps index 1 and 2.')
@cli.option('--rc1', is_flag=True, help='Exchanges index 1 for its reverse compliment.')
@cli.option('--rc2', is_flag=True, help='Exchanges index 2 for its reverse compliment.')
# TODO: Add option to include additional samples

def main(manifest_path, project, swap, rc1, rc2):
    """Main function to fix the samplesheet indexes for AVITI runs."""
    # Read the samplesheet
    manifest_header, manifest_data = load_manifest(manifest_path)
    # Read the sample data into a data frame (look at Element_runs.py for an example)
    samples_info = load_sample_dataframe(manifest_data)
    # Process the indexes based on the options provided
    if rc1:
        samples_info['Index1'] = samples_info['Index1'].apply(reverse_complement_index)
    if rc2:
        samples_info['Index2'] = samples_info['Index2'].apply(reverse_complement_index)
    if swap:
        samples_info[['Index1', 'Index2']] = samples_info[['Index2', 'Index1']]
    print(samples_info)
    # Generate the updated samplesheet

if __name__ == '__main__':
    main()
