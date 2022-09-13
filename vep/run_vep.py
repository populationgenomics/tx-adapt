#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import click
import hail as hl
from hail.utils.java import Env
from cpg_utils.hail_batch import output_path

# from cloudpathlib import AnyPath
GTEX_FILE = (
    'gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet'
)
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'
GENCODE_GTF = 'gs://cpg-gtex-test/reference/gencode.v26.annotation.gtf.gz'


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

    # only select necessary columns
    ht = ht.select('phenotype_id', 'variant_id')
    # 'pval_nominal', 'slope', 'slope_se'
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
    # save ht
    ht_path = output_path('gtex_ht_filtered.ht')
    ht.write(ht_path, overwrite=True)
    vep = hl.vep(ht, config='file:///vep_data/vep-gcloud.json')
    # only keep GTEx annotation and the most severe consequences from VEP annotation
    # gtex_entries = list(ht.row)
    # keys = list(ht.key)
    # gtex_entries = [name for name in gtex_entries if name not in keys]
    # vep = vep.select(*gtex_entries, vep.vep.most_severe_consequence)
    # # add CADD annotation
    # cadd_ht = hl.read_table(CADD_HT)
    # vep = vep.annotate(
    #     cadd=cadd_ht[vep.key].cadd,
    # )
    # # add in ensembl ids
    # gtf = hl.experimental.import_gtf(
    #     GENCODE_GTF, reference_genome='GRCh38', skip_invalid_contigs=True, force=True
    # )
    # vep = vep.annotate(gene_id=gtf[vep.locus].gene_id)
    vep_path = output_path(f'vep{vep_version}_cadd_GRCh38_annotation.tsv.bgz')
    vep.export(vep_path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
