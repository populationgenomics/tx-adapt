#!/usr/bin/env python3

"""
Use VEP using a dataproc cluster.
"""


from analysis_runner import dataproc
from cpg_utils.workflows.batch import get_batch

# create a hail batch
batch = get_batch('run_vep_in_dataproc_cluster')

dataproc.hail_dataproc_job(
    batch,
    f'run_vep.py',
    max_age='4h',
    # num_secondary_workers=20,
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    job_name='run_vep',
    cluster_name='run vep',
)

batch.run(wait=False)
