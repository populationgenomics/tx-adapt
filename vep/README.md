# Run VEP using the analysis runner

This runs a Hail query script in Dataproc using Hail Batch in order to run VEP on parquet files from the GTEx dataset. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset tx-adapt --description "run vep" --output-dir "vep/v1" --access-level test main.py --gtex-file gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr21.parquet --tissue-type 'whole_blood' --chromosome 21
```
