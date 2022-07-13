#!/usr/bin/env bash

set -ex

gsutil -u tx-adapt -m cp "gs://gtex-resources/GTEx_Analysis_v8_QTLs/GTEx_Analysis_v8_EUR_eQTL_all_associations/Whole_Blood.v8.EUR.allpairs.*" gs://cpg-gtex-main/v8/whole_blood/
