"""Entry point for the analysis runner."""

import hailtop.batch as hb
from analysis_runner import dataproc
from cpg_utils.config import get_config
from cpg_utils.hail_batch import (
    remote_tmpdir,
    copy_common_env,
)

backend = hb.ServiceBackend(
    billing_project=get_config()['hail']['billing_project'],
    remote_tmpdir=remote_tmpdir(),
)

batch = hb.Batch(
    name='mt_to_parquet',
    backend=backend,
    default_python_image=get_config()['workflow']['driver_image'],
)

job = dataproc.hail_dataproc_job(
    batch,
    f'ht_to_parquet.py',
    max_age='1h',
    init=['gs://cpg-reference/hail_dataproc/install_common.sh'],
    job_name=f'mt_to_parquet',
)
copy_common_env(job)

batch.run(wait=False)
