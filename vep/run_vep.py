#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import click
import hail as hl
import pandas as pd

# from cloudpathlib import AnyPath
GTEX_FILE = (
    'gs://cpg-gtex-test/v8/whole_blood/Whole_Blood.v8.EUR.allpairs.chr22.parquet'
)


@click.command()
def main():
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    # # read in all GTEX chromosome files
    # all_chromosome_files = list(
    #     AnyPath(input_path).glob('*')  # pylint: disable=no-member
    # )
    # gtex = pd.concat((pd.read_parquet(f) for f in all_chromosome_files))
    gtex = pd.read_parquet(GTEX_FILE)
    gtex = gtex.head(100)
    print(gtex.head())
    # add in necessary VEP annotation
    variant_id_info = gtex.variant_id.str.split('_').str[0:4]
    gtex['chr'], gtex['position'], gtex['alleles'] = (
        variant_id_info.str[0],
        variant_id_info.str[1],
        variant_id_info.str[2:4],
    )
    print(gtex.head())
    # convert to hail table and add the required locus key (the required alleles key is already inside the ht)
    ht = hl.Table.from_pandas(gtex)
    ht = ht.annotate(locus=hl.locus(ht.chr, hl.int32(ht.position)))
    # 'vep' requires key to be two fields 'locus' (type 'locus<any>') and 'alleles' (type 'array<str>')
    ht = ht.key_by('locus', 'alleles')
    ht.show()
    vep = hl.vep(ht, config='file:///vep_data/vep-gcloud.json')
    vep_path = 'gs://cpg-tob-wgs-test/vep/v0/vep105_GRCh38.mt'
    vep.write(vep_path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
