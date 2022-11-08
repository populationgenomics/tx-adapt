#!/usr/bin/env python3


"""
Run VEP on GTEx trans-qtl dataset
"""

import click
import hail as hl
from cpg_utils.hail_batch import output_path

# VEP 95 (GENCODE 29), which is closest to GENCODE 26
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'
GTEX_FILE = 'gs://cpg-gtex-test/v8/trans_qtls/GTEx_Analysis_v8_trans_eGenes_fdr05.txt'


@click.command()
@click.option('--vep-version', help='Version of VEP', default='104.3')
def main(vep_version: str):
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    gtex = hl.import_table(
        GTEX_FILE,
        types={
            'gene_mappability': hl.tfloat64,
            'tissue_af': hl.tfloat64,
            'slope': hl.tfloat64,
            'slope_se': hl.tfloat64,
            'pval_nominal': hl.tfloat64,
            'fdr': hl.tfloat64,
        },
    )

    # prepare ht for VEP annotation
    gtex = gtex.annotate(
        chromosome=gtex.variant_id.split('_')[0],
        position=gtex.variant_id.split('_')[1],
        alleles=gtex.variant_id.split('_')[2:4],
    )
    gtex = gtex.annotate(locus=hl.locus(gtex.chromosome, hl.int32(gtex.position)))
    # 'vep' requires the key to be two fields: 'locus' (type 'locus<any>') and 'alleles' (type 'array<str>')
    gtex = gtex.key_by('locus', 'alleles')

    # add in VEP annotation
    vep = hl.vep(gtex, config='file:///vep_data/vep-gcloud.json')
    # export as ht
    vep_path_ht = output_path(f'trans_qtl_variants_vep{vep_version}.ht')
    vep.write(vep_path_ht, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
