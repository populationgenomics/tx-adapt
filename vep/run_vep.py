#!/usr/bin/env python3


"""
Run VEP on GTEx dataset
"""

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


def main():
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
    gtex = gtex.annotate(
        cadd=cadd[gtex.key].cadd,
    )
    # add in ensembl ids
    gtf = hl.experimental.import_gtf(
        GENCODE_GTF, reference_genome='GRCh38', skip_invalid_contigs=True, force=True
    )
    gtex = gtex.annotate(gene_id=gtf[gtex.locus].gene_id)
    # export as both ht and tsv
    gtex_path_ht = output_path(
        f'gtex_association_all_positions_maf01_vep95_cadd_annotated.ht'
    )
    gtex_path_tsv = output_path(
        f'gtex_association_all_positions_maf01_vep95_cadd_annotated.tsv.bgz'
    )
    gtex.write(gtex_path_ht, overwrite=True)
    gtex.export(gtex_path_tsv)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
