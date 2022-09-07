# Run VEP using the analysis runner

This runs a Hail query script in Dataproc using Hail Batch in order to run VEP on parquet files from the GTEx dataset. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner --dataset tob-wgs --description "run vep" --output-dir "tob_wgs_vep/v0" --access-level main main.py --script run_vep.py --vep-version '88'
```
