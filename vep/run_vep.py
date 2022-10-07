#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import os
import click
import pandas as pd
import hail as hl
from cpg_utils.hail_batch import init_batch, output_path

# VEP 95 (GENCODE 29), which is closest to GENCODE 26
VEP_HT = (
    'gs://gcp-public-data--gnomad/resources/context/grch38_context_vep_annotated.ht/'
)
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'
GENCODE_GTF = 'gs://cpg-gtex-test/reference/gencode.v26.annotation.gtf.gz'


@click.command()
@click.option('--gtex-file', help='gtex file to perform VEP annotation on')
def main(gtex_file: str):
    """
    Run vep using main.py wrapper
    """

    init_batch()

    gtex = pd.read_parquet(gtex_file)
    gtex = hl.Table.from_pandas(gtex)

    # prepare ht for VEP annotation
    gtex = gtex.annotate(
        chromosome=gtex.variant_id.split('_')[0],
        position=gtex.variant_id.split('_')[1],
        alleles=gtex.variant_id.split('_')[2:4],
    )
    gtex = gtex.annotate(locus=hl.locus(gtex.chromosome, hl.int32(gtex.position)))
    # 'vep' requires the key to be two fields: 'locus' (type 'locus<any>') and 'alleles' (type 'array<str>')
    gtex = gtex.key_by('locus', 'alleles')

    # add in VEP annotation and match with gtex association data
    vep = hl.read_table(VEP_HT)
    vep = vep[gtex.key].vep
    # only keep VEP annotation that's relevant for TA analysis
    gtex = gtex.annotate(
        most_severe_consequence=vep.most_severe_consequence,
        consequence_terms=vep.transcript_consequences.consequence_terms,
        transcript_id=vep.transcript_consequences.transcript_id,
    )

    # add CADD annotation
    cadd = hl.read_table(CADD_HT)
    gtex = vep.annotate(
        cadd=cadd[vep.key].cadd,
    )
    # add in ensembl ids
    gtf = hl.experimental.import_gtf(
        GENCODE_GTF, reference_genome='GRCh38', skip_invalid_contigs=True, force=True
    )
    gtex = gtex.annotate(gene_id=gtf[gtex.locus].gene_id)
    # get tissue type and chromosome for annotating output file
    tissue_type = os.path.basename(gtex_file).split('.')[0].lower()
    chromosome = os.path.basename(gtex_file).split('.')[-2]
    gtex_path = output_path(
        f'gtex_association_{tissue_type}_{chromosome}_vep95_cadd_annotated.tsv.bgz'
    )
    gtex.write(gtex_path, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
