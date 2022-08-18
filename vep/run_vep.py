#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import hail as hl
from hail.utils.java import Env

# from cloudpathlib import AnyPath
GTEX_FILE = (
    'gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet'
)
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'


def main():
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

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
    vep = hl.vep(ht, config='file:///vep_data/vep-gcloud.json')
    # only keep the most severe consequences
    vep = vep.select(vep.vep.most_severe_consequence)
    # add CADD annotation
    cadd_ht = hl.read_table(CADD_HT)
    vep = vep.annotate(
        cadd=cadd_ht[vep.key].cadd,
    )
    vep_path = 'gs://cpg-gtex-test/vep/v0/vep105_cadd_GRCh38.tsv.bgz'
    vep.export(vep_path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
