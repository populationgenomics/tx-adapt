#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

import click
from hail.utils.java import Env
import hail as hl
from cpg_utils.hail_batch import output_path

# VEP 95 (GENCODE 29), which is closest to GENCODE 26
VEP_HT = (
    'gs://gcp-public-data--gnomad/resources/context/grch38_context_vep_annotated.ht/'
)
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'
GENCODE_GTF = 'gs://cpg-gtex-test/reference/gencode.v26.annotation.gtf.gz'
GTEX_FILE = 'gs://cpg-gtex-test/v8/reference_data/WGS_Feature_overlap_collapsed_VEP_short_4torus.MAF01.txt'


@click.command()
@click.option('--vep-version', help='Version of VEP', default='104.3')
def main(vep_version: str):
    """
    Run vep using main.py wrapper
    """

    hl.init(default_reference='GRCh38')

    spark = Env.spark_session()

    gtex = spark.read.csv(GTEX_FILE, sep='\t', header=True)
    gtex = hl.Table.from_spark(gtex)

    # prepare ht for VEP annotation
    gtex = gtex.annotate(
        chromosome=gtex.SNP.split('_')[0],
        position=gtex.SNP.split('_')[1],
        alleles=gtex.SNP.split('_')[2:4],
    )
    gtex = gtex.annotate(locus=hl.locus(gtex.chromosome, hl.int32(gtex.position)))
    # 'vep' requires the key to be two fields: 'locus' (type 'locus<any>') and 'alleles' (type 'array<str>')
    gtex = gtex.key_by('locus', 'alleles')
    # drop all columns except keys and SNP columns
    gtex = gtex.select('SNP')

    # Intersect SNVs (not INDELs, for which there is no data) with the gnomAD VEP context ht
    gtex_snv = gtex.filter(hl.is_indel(gtex.alleles[0], gtex.alleles[1]), keep=False)
    # add in VEP annotation and match with gtex association SNV data
    vep = hl.read_table(VEP_HT)
    vep = vep[gtex_snv.key].vep
    # only keep VEP annotation that's relevant for TA analysis
    gtex_snv = gtex_snv.annotate(
        most_severe_consequence=vep.most_severe_consequence,
        consequence_terms=vep.transcript_consequences.consequence_terms,
        transcript_id=vep.transcript_consequences.transcript_id,
    )
    # run VEP 88 (note mismatch) on INDELs
    gtex_indels = gtex.filter(hl.is_indel(gtex.alleles[0], gtex.alleles[1]), keep=True)
    gtex_indels = hl.vep(gtex_indels, config='file:///vep_data/vep-gcloud.json')
    # rename columns to match SNV table
    gtex_indels = gtex_indels.annotate(
        most_severe_consequence=gtex_indels.vep.most_severe_consequence,
        consequence_terms=gtex_indels.vep.transcript_consequences.consequence_terms,
        transcript_id=gtex_indels.vep.transcript_consequences.transcript_id,
    )
    gtex_indels = gtex_indels.select(
        'SNP', 'most_severe_consequence', 'consequence_terms', 'transcript_id'
    )
    # combine SNV and indel table
    gtex_union = gtex_snv.union(gtex_indels)

    # add CADD annotation
    cadd = hl.read_table(CADD_HT)
    gtex_union = gtex_union.annotate(
        cadd=cadd[gtex_union.key].cadd,
    )
    # add in ensembl ids
    gtf = hl.experimental.import_gtf(
        GENCODE_GTF, reference_genome='GRCh38', skip_invalid_contigs=True, force=True
    )
    gtex_union = gtex_union.annotate(gene_id=gtf[gtex_union.locus].gene_id)
    # export as ht
    gtex_path_ht = output_path(
        f'gtex_variants_maf01_vep{vep_version}_gnomADcontext95_cadd_annotated.ht'
    )
    gtex_union.write(gtex_path_ht, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
