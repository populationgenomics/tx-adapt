#!/usr/bin/env python3


"""
Generate parquet file from ht
"""

import hail as hl

VEP_FILE = 'gs://cpg-gtex-test/vep/v0/vep105_GRCh38.mt/'


def main():
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    vep = hl.read_table(VEP_FILE)
    gtex_keys = [
        'phenotype_id',
        'variant_id',
        'tss_distance',
        'maf',
        'ma_samples',
        'ma_count',
        'pval_nominal',
        'slope',
        'slope_se',
    ]
    vep = vep.select(*gtex_keys, vep.vep.most_severe_consequence)
    vep.export('gs://cpg-gtex-test/vep/v0/vep105_GRCh38_keyed.tsv.bgz')


if __name__ == '__main__':
    main()  # pylint: disable=E1120
