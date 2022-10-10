# Test VEP using the analysis runner

This runs a Hail query script in Dataproc using Hail Batch in order to run VEP on a hail matrix table. To run, use conda to install the analysis-runner, then execute the following command:

```sh
analysis-runner \
    --dataset tx-adapt \
    --description "testing for TA" \
    --output-dir "ta/v0" \
    --access-level test \
    --image australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-r:1 get_ta_candidates.R
```
