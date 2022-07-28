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
    vep = vep.to_pandas()
    vep.to_csv('gs://cpg-gtex-test/vep/v0/vep105_GRCh38.csv')


if __name__ == '__main__':
    main()  # pylint: disable=E1120
