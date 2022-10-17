#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import click
import hail as hl
from cpg_utils.hail_batch import output_path


GTEX_VEP = 'gs://cpg-tx-adapt-test/vep/v8/gtex_association_all_positions_maf01_vep95_cadd_annotated.ht/'


@click.command()
@click.option('--vep-version', help='Version of VEP', default='104.3')
def main(vep_version: str):
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    gtex = hl.read_table(GTEX_VEP)

    # get INDELs only
    gtex_indels = gtex.filter(hl.is_indel(gtex.alleles[0], gtex.alleles[1]), keep=True)
    # Run VEP on INDELs
    gtex_indels = hl.vep(gtex_indels, config='file:///vep_data/vep-gcloud.json')
    # save ht
    gtex_indels_path_ht = output_path(f'gtex_variants_indels_vep{vep_version}.ht')
    gtex_indels.write(gtex_indels_path_ht, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
