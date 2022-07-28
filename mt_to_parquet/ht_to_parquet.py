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
    vep = vep.key_by(vep.vep.most_severe_consequence)
    vep = vep.drop(vep.vep)
    vep.export('gs://cpg-gtex-test/vep/v0/vep105_GRCh38_keyed.tsv.bgz')


if __name__ == '__main__':
    main()  # pylint: disable=E1120
