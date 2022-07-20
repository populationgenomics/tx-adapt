# Run VEP using the analysis runner

This runs a Hail query script in Dataproc using Hail Batch in order to run VEP on parquet files from the GTEx dataset. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset tx-adapt --description "run vep" --output-dir "v8/vep/v0" --access-level standard main.py --script run_vep.py --input-path 'gs://cpg-gtex-main/v8/whole_blood'
```
