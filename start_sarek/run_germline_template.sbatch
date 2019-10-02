
#!/bin/bash -l
 
#SBATCH -A ngi2016003
#SBATCH -p core
#SBATCH -n 1
#SBATCH -t 10-00:00:00
set -e
 
cd /proj/ngi2016003/nobackup/NGI/ANALYSIS/PROJECT_ID/sarek_ngi/SAMPLE_ID
out_dir=/proj/ngi2016003/nobackup/NGI/ANALYSIS/PROJECT_ID/sarek_ngi/SAMPLE_ID
work_dir=/proj/ngi2016003/nobackup/NGI/ANALYSIS/PROJECT_ID/sarek_ngi/SAMPLE_ID/work
 
echo STARTING_M
date
sarek main.nf --containerPath /sw/data/uppnex/ToolBox/sarek --sample SAMPLE_ID.tsv --genome REFERENCE -w ${work_dir} -resume
touch ${out_dir}/main.done
cat ${out_dir}/.nextflow.log| grep "Exit status" > ${out_dir}/main.status
 
echo STARTING_GERM
date
sarek germlineVC.nf --containerPath /sw/data/uppnex/ToolBox/sarek --sample Preprocessing/Recalibrated/recalibrated.tsv --genome REFERENCE --tools HaplotypeCaller,Manta,Strelka -w ${work_dir} -resume
touch ${out_dir}/germline.done
cat ${out_dir}/.nextflow.log| grep "Exit status" > ${out_dir}/germline.status
 
echo STARTING_ANN
date
sarek annotate.nf --containerPath /sw/data/uppnex/ToolBox/sarek --annotateTools HaplotypeCaller --tools VEP,snpEff --genome REFERENCE -w ${work_dir} -resume
touch ${out_dir}/annotate.done
cat ${out_dir}/.nextflow.log| grep "Exit status" > ${out_dir}/annotation.status
 
date
echo STARTING_MUTLTQC
sarek runMultiQC.nf --containerPath /sw/data/uppnex/ToolBox/sarek
touch ${out_dir}/multiqc.done
cat ${out_dir}/.nextflow.log| grep "Exit status" > ${out_dir}/multiqc.status
