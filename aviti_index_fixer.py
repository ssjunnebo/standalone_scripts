import click as cli
import pandas as pd
import os

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
    header_section = manifest_content.split("[SAMPLES]")[0]
    samples_section = manifest_content.split("[SAMPLES]")[1].strip().split("\n")
    all_samples = load_sample_dataframe(samples_section)
    samples_info = all_samples[all_samples["Project"] != "Control"].copy()
    controls_info = all_samples[all_samples["Project"] == "Control"].copy()
    return header_section, samples_info, controls_info

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
@cli.option('--add_sample', multiple=True, help='Include additional sample(s). Use multiple times for multiple samples, or provide a file. Each new sample should have the same format as in the existing manifest. Example: --add_sample P12345,ATCG,CGTA,1,A__Project_25_16,301-10-10-301,ATCG-CGTA,')

def main(manifest_path, project, swap, rc1, rc2, add_sample):
    """Main function to fix the samplesheet indexes for AVITI runs."""
    # Read the samplesheet
    manifest_header, samples_info, controls_info = load_manifest(manifest_path)    

    # Process the indexes based on the options provided
    if project:
        mask = samples_info['SampleName'].apply(lambda x: x.split("_")[0] == project)
    else:
        mask = pd.Series([True] * len(samples_info))

    if rc1:
        samples_info.loc[mask, 'Index1'] = samples_info.loc[mask, 'Index1'].apply(reverse_complement_index)
        print("Reverse complementing Index1")
    if rc2:
        samples_info.loc[mask, 'Index2'] = samples_info.loc[mask, 'Index2'].apply(reverse_complement_index)
        print("Reverse complementing Index2")
    if swap:
        samples_info.loc[mask, ['Index1', 'Index2']] = samples_info.loc[mask, ['Index2', 'Index1']].values
        print("Swapping Index1 and Index2")
    if rc1 or rc2 or swap:
        # Update lims_label if any changes were made unless the "Project" column is not "Control"
        samples_info.loc[mask, 'lims_label'] = samples_info.loc[mask, 'Index1'] + '-' + samples_info.loc[mask, 'Index2']
    for additional_sample in add_sample:
        if os.path.isfile(additional_sample):
            # If a file is provided, read it and append to the samples_info DataFrame
            additional_samples = pd.read_csv(additional_sample, header=None)
            additional_samples.columns = samples_info.columns
        else:
            # If a sample is provided directly, create a DataFrame from it
            additional_samples = pd.DataFrame([additional_sample.split(',')], columns=samples_info.columns)
        samples_info = pd.concat([samples_info, additional_samples], ignore_index=True)
        if len(additional_samples) == 1:
            print("Adding additional sample:", additional_samples['SampleName'].tolist()[0])
        else:
            print("Adding additional samples:", (", ").join(additional_samples['SampleName'].tolist()))

    # Sort the samples by lane and SampleName
    samples_info['Lane'] = samples_info['Lane'].astype(int)
    samples_info.sort_values(by=['Lane', 'SampleName'], inplace=True)
   
    # Generate the updated samplesheet
    updated_samplesheet = manifest_header + "\n[SAMPLES]\n" + samples_info.to_csv(index=False, header=True) + controls_info.to_csv(index=False, header=False)
   
    # Write the updated samplesheet to a new file
    output_path = manifest_path.replace('.csv', '_updated.csv')
    with open(output_path, 'w') as output_file:
        output_file.write(updated_samplesheet)

if __name__ == '__main__':
    main()
