#!/usr/bin/env python3

"""
Use VEP using a dataproc cluster.
"""


import click
from analysis_runner import dataproc
from cpg_utils.workflows.batch import get_batch


@click.command()
@click.option('--script', 'script', help='path to VEP main script')
@click.option('--vep-version', help='Version of VEP', default='104.3')
def main(script: str, vep_version: str):
    """
    runs a script inside dataproc to execute VEP
    :param script: str, the path to the VEP main script
    """

    # create a hail batch
    batch = get_batch('run_vep_in_dataproc_cluster')

    dataproc.hail_dataproc_job(
        batch=batch,
        worker_machine_type='n1-highmem-8',
        script=f'{script} --vep-version {vep_version}',
        max_age='2h',
        init=[
            f'gs://cpg-reference/hail_dataproc/install_common.sh',
            f'gs://cpg-reference/vep/{vep_version}/dataproc/init.sh',
        ],
        job_name='run_vep',
        cluster_name='run vep',
    )

    batch.run(wait=False)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
