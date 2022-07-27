#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import hail as hl
from hail.utils.java import Env
import hailtop.batch as hb
from cpg_utils.hail_batch import (
    copy_common_env,
    remote_tmpdir,
    init_batch,
)
from cpg_utils.config import get_config

GTEX_FILE = (
    'gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet'
)


def run_vep():
    """
    Run vep on GTEx data
    """

    init_batch()

    spark = Env.spark_session()
    gtex = spark.read.parquet(GTEX_FILE)
    ht = hl.Table.from_spark(gtex)
    # add in necessary VEP annotation
    ht = ht.annotate(
        chromosome=ht.variant_id.split('_')[0],
        position=ht.variant_id.split('_')[1],
        alleles=ht.variant_id.split('_')[2:4],
    )
    ht = ht.annotate(locus=hl.locus(ht.chromosome, hl.int32(ht.position)))
    # 'vep' requires the key to be two fields: 'locus' (type 'locus<any>') and 'alleles' (type 'array<str>')
    ht = ht.key_by('locus', 'alleles')
    # filter to biallelic loci only
    ht = ht.filter(hl.len(ht.alleles) == 2)
    ht = ht.filter(ht.alleles[1] != '*')
    vep = hl.vep(ht, config='gs://cpg-reference/vep/vep105-GRCh38-loftee-gcloud.json')
    vep_path = 'gs://cpg-gtex-test/vep/v1/vep105_GRCh38.mt'
    vep.write(vep_path)


def main():
    """
    Runs vep using Hail Batch
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

    job = batch.new_python_job('run_vep')
    copy_common_env(job)
    job.call(run_vep)

    batch.run(wait=False)


if __name__ == '__main__':
    # pylint: disable=no-value-for-parameter
    main()
