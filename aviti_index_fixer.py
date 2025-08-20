import os
import click as cli
import pandas as pd

NT_COMPLIMENT = {
    "A": "T",
    "T": "A",
    "C": "G",
    "G": "C",
}


def load_manifest(path):
    """Load the manifest from the given path."""
    with open(path, "r") as file:
        manifest_content = file.read()

    header_section = manifest_content.split("[SAMPLES]")[0]
    samples_section = manifest_content.split("[SAMPLES]")[1].strip().split("\n")

    all_samples = load_sample_dataframe(samples_section)
    samples_info = all_samples[all_samples["Project"] != "Control"].copy()
    controls_info = all_samples[
        all_samples["Project"] == "Control"
    ].copy()  # So that we don't apply any changes to control samples

    return header_section, samples_info, controls_info


def load_sample_dataframe(manifest_data):
    """Load the sample data into a DataFrame."""
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
    return "".join(NT_COMPLIMENT[nuc] for nuc in reversed(index))


def print_running_note(project, change_type):
    """Print a note about the changes being made."""
    if project:
        project_text = "project " if len(project) == 1 else "projects "
        project_text += (
            project[0]
            if len(project) == 1
            else " and ".join(project)
            if len(project) == 2
            else ", ".join(project)
        )
    else:
        project_text = "all samples"
    if change_type == "swap":
        print(
            f"Index 1 and 2 in {project_text} were switched prior to re-demultiplexing."
        )
    elif change_type == "rc1":
        print(
            f"Index 1 in {project_text} was converted to reverse complement prior to re-demultiplexing."
        )
    elif change_type == "rc2":
        print(
            f"Index 2 in {project_text} was converted to reverse complement prior to re-demultiplexing",
        )


@cli.command()
@cli.option(
    "--manifest_path",
    required=True,
    help="Path to the sample manifest. e.g. ~/fc/AVITI_run_manifest_2450545934_24-1214961_250722_154957_EunkyoungChoi_untrimmed.csv",
)
@cli.option(
    "--project",
    multiple=True,
    required=False,
    help="Project ID, e.g. P10001. Only the indexes of samples with this specific project ID will be changed. Use multiple times for multiple projects.",
)
@cli.option("--swap", is_flag=True, help="Swaps index 1 and 2.")
@cli.option("--rc1", is_flag=True, help="Exchanges index 1 for its reverse compliment.")
@cli.option("--rc2", is_flag=True, help="Exchanges index 2 for its reverse compliment.")
@cli.option(
    "--add_sample",
    multiple=True,
    help="Include additional sample(s). Use multiple times for multiple samples, or provide a file. Each new sample should have the same format as in the existing manifest. Example: --add_sample P12345,ATCG,CGTA,1,A__Project_25_16,301-10-10-301,ATCG-CGTA,",
)
def main(manifest_path, project, swap, rc1, rc2, add_sample):
    """Main function to fix the samplesheet indexes for AVITI runs."""
    manifest_header, samples_info, controls_info = load_manifest(manifest_path)

    if project:
        mask = samples_info["SampleName"].apply(lambda x: x.split("_")[0] in project)
    else:
        mask = pd.Series([True] * len(samples_info))

    if rc1:
        samples_info.loc[mask, "Index1"] = samples_info.loc[mask, "Index1"].apply(
            reverse_complement_index
        )
        print_running_note(project, "rc1")
    if rc2:
        samples_info.loc[mask, "Index2"] = samples_info.loc[mask, "Index2"].apply(
            reverse_complement_index
        )
        print_running_note(project, "rc2")
    if swap:
        samples_info.loc[mask, ["Index1", "Index2"]] = samples_info.loc[
            mask, ["Index2", "Index1"]
        ].values
        print_running_note(project, "swap")

    if rc1 or rc2 or swap:
        # Update lims_label if any changes were made
        samples_info.loc[mask, "lims_label"] = (
            samples_info.loc[mask, "Index1"] + "-" + samples_info.loc[mask, "Index2"]
        )

    additional_samples_table = {}
    for additional_sample in add_sample:
        if os.path.isfile(additional_sample):
            additional_samples = pd.read_csv(additional_sample, header=None)
            additional_samples.columns = samples_info.columns
            for _, row in additional_samples.iterrows():
                additional_samples_table[row["SampleName"]] = {
                    "index": f"{row['Index1']}-{row['Index2']}",
                    "lane": row["Lane"],
                }
        else:
            additional_samples = pd.DataFrame(
                [additional_sample.split(",")], columns=samples_info.columns
            )
            additional_samples_table[additional_samples["SampleName"].tolist()[0]] = {
                "index": f"{additional_samples['Index1'].tolist()[0]}-{additional_samples['Index2'].tolist()[0]}",
                "lane": additional_samples["Lane"].tolist()[0],
            }
        samples_info = pd.concat([samples_info, additional_samples], ignore_index=True)
    if len(additional_samples_table) == 1:
        sample_name = list(additional_samples_table.keys())[0]
        print(
            f"Sample {sample_name} with indexes",
            f"{additional_samples_table[sample_name]['index']}",
            f"was added to lane {additional_samples_table[sample_name]['lane']}",
            "prior to re-demultiplexing.",
        )
    elif len(additional_samples_table) > 1:
        print(
            "The following samples were added to the manifest prior to re-demultiplexing:\n"
            "SampleName, Index1-Index2, Lane\n"
            + "\n".join(
                [
                    f"{name}, {info['index']}, {info['lane']}"
                    for name, info in additional_samples_table.items()
                ]
            )
        )

    samples_info["Lane"] = samples_info["Lane"].astype(int)
    samples_info.sort_values(by=["Lane", "SampleName"], inplace=True)

    updated_samplesheet = (
        manifest_header
        + "\n[SAMPLES]\n"
        + samples_info.to_csv(index=False, header=True)
        + controls_info.to_csv(index=False, header=False)
    )
    output_path = manifest_path.replace(".csv", "_updated.csv")
    with open(output_path, "w") as output_file:
        output_file.write(updated_samplesheet)


if __name__ == "__main__":
    main()
