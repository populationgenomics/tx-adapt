#!/usr/bin/env python3


"""
Run VEP on the hail mt
"""


import click
import hail as hl
from hail.utils.java import Env
from cpg_utils.hail_batch import output_path

GTEX_FILE = (
    'gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet'
)


@click.command()
@click.option('--vep-version', help='Version of VEP', default='104.3')
def main(vep_version: str):
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    spark = Env.spark_session()

    gtex = spark.read.parquet(GTEX_FILE)
    ht = hl.Table.from_spark(gtex)
    ht = ht.head(100)
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
    # remove starred alleles, as this results in an error in VEP
    # see https://discuss.hail.is/t/vep-output-variant-not-found-in-original-variants/1148
    ht = ht.filter(ht.alleles[1] != '*')
    vep = hl.vep(ht, config='file:///vep_data/vep-gcloud.json')
    vep_path = output_path(f'vep{vep_version}_GRCh38.ht')
    vep.write(vep_path, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
