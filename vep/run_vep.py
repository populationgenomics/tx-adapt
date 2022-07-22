#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import click
import hail as hl
import pandas as pd
from cpg_utils.hail_batch import (
    output_path,
)

# from cloudpathlib import AnyPath


@click.command()
@click.option(
    '--input-path', required=True, help='Path to GTEx parquet files to run VEP on'
)
def main(input_path: str):
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    # # read in all GTEX chromosome files
    # all_chromosome_files = list(
    #     AnyPath(input_path).glob('*')  # pylint: disable=no-member
    # )
    # gtex = pd.concat((pd.read_parquet(f) for f in all_chromosome_files))
    gtex = pd.read_parquet(input_path)
    # add in necessary VEP annotation
    variant_id_info = gtex.variant_id.str.split('_').str[0:4]
    gtex['chr'], gtex['position'], gtex['alleles'] = (
        variant_id_info.str[0],
        variant_id_info.str[1],
        variant_id_info.str[2:4],
    )
    # convert to hail table and add the required locus key (the required alleles key is already inside the ht)
    ht = hl.Table.from_pandas(gtex)
    ht = ht.annotate(locus=hl.locus(ht.chr, hl.int32(ht.position)))
    vep = hl.vep(ht, config='file:///vep_data/vep-gcloud.json')
    vep_path = output_path('vep105_GRCh38.mt')
    vep.write(vep_path)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
