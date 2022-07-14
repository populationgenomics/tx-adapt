#!/usr/bin/env bash

set -ex

gsutil -u tx-adapt -m cp "gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_eQTL_all_associations/Whole_Blood.v8.EUR.allpairs.*" gs://cpg-gtex-main/v8/whole_blood/
gsutil -m cp gs://cpg-gtex-main/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet gs://cpg-gtex-test/v8/whole_blood/
