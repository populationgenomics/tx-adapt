#!/usr/bin/env python3

"""
Use VEP using a dataproc cluster.
Taken from Matt Welland's script, run_vep_help.py
"""


import hailtop.batch as hb
from analysis_runner import dataproc
from cpg_utils.config import get_config
from cpg_utils.hail_batch import (
    remote_tmpdir,
    copy_common_env,
)
import click


@click.command()
@click.option('--script', 'script', help='path to VEP main script')
def main(script: str):
    """
    runs a script inside dataproc to execute VEP
    :param script: str, the path to the VEP main script
    """

    backend = hb.ServiceBackend(
        billing_project=get_config()['hail']['billing_project'],
        remote_tmpdir=remote_tmpdir(),
    )

    # create a hail batch
    batch = hb.Batch(
        name='run_vep_in_dataproc_cluster',
        backend=backend,
        default_python_image=get_config()['workflow']['driver_image'],
    )

    job = dataproc.hail_dataproc_job(
        batch=batch,
        worker_machine_type='n1-highmem-8',
        worker_boot_disk_size=200,
        secondary_worker_boot_disk_size=200,
        script=f'{script}',
        max_age='12h',
        init=[
            'gs://cpg-reference/hail_dataproc/install_common.sh',
            'gs://cpg-reference/vep/vep-GRCh38.sh',
        ],
        job_name='run_vep',
        num_workers=2,
        cluster_name='run vep',
    )
    copy_common_env(job)
    job.cpu(2)
    job.memory('standard')
    job.storage('20G')

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
