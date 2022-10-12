# Test VEP using the analysis runner

This runs an R script to find candidates for [transcriptional adaptation](https://www.nature.com/articles/s41586-019-1064-z). To run, use mamba to [install the analysis-runner](https://github.com/populationgenomics/team-docs/blob/main/getting_started.md#analysis-runner), then execute the following command:

```sh
analysis-runner \
    --dataset tx-adapt \
    --description "testing for TA" \
    --output-dir "ta/v0" \
    --access-level test \
    --image australia-southeast1-docker.pkg.dev/analysis-runner/images/driver-r:1.2 get_ta_candidates.R
```
