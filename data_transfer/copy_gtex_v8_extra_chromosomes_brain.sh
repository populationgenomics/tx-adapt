#!/usr/bin/env bash

set -ex

# copy a new (small) chromosome from whole blood on main to identify any TA candidates
gsutil -m cp gs://cpg-gtex-main/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr21.parquet gs://cpg-gtex-test/v8/whole_blood/
# get one non-immune cell type and identify TA candidates using the same chromosomes
gsutil -u tx-adapt -m cp "gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_eQTL_all_associations/Brain_Amygdala.v8.EUR.allpairs.*" gs://cpg-gtex-main/v8/whole_blood/
gsutil -m cp gs://cpg-gtex-main/v8/whole_blood/Brain_Amygdala.v8.EUR.allpairs.chr21.parquet gs://cpg-gtex-test/v8/whole_blood/
gsutil -m cp gs://cpg-gtex-main/v8/whole_blood/Brain_Amygdala.v8.EUR.allpairs.chr22.parquet gs://cpg-gtex-test/v8/whole_blood/
