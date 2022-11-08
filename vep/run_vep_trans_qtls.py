#!/usr/bin/env python3


"""
Run VEP on GTEx trans-qtl dataset
"""

import hail as hl
from cpg_utils.hail_batch import output_path, init_batch

# VEP 95 (GENCODE 29), which is closest to GENCODE 26
VEP_HT = (
    'gs://gcp-public-data--gnomad/resources/context/grch38_context_vep_annotated.ht/'
)
CADD_HT = 'gs://cpg-reference/seqr/v0-1/combined_reference_data_grch38-2.0.4.ht'
GTEX_FILE = 'gs://cpg-gtex-test/v8/trans_qtls/GTEx_Analysis_v8_trans_eGenes_fdr05.txt'
GENCODE_GTF = 'gs://cpg-gtex-test/reference/gencode.v26.annotation.gtf.gz'


def main():
    """
    Run vep using main.py wrapper
    """

    init_batch()

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
    # add in VEP annotation and match with gtex association SNV data
    vep = hl.read_table(VEP_HT)
    vep = vep[gtex.key].vep
    # only keep VEP annotation that's relevant for TA analysis
    gtex = gtex.annotate(
        most_severe_consequence=vep.most_severe_consequence,
        consequence_terms=vep.transcript_consequences.consequence_terms,
        transcript_id=vep.transcript_consequences.transcript_id,
    )
    gtex_path = output_path(f'trans_qtl_vep.ht')
    gtex = gtex.checkpoint(gtex_path, overwrite=True)
    print(gtex.show())

    # # add CADD annotation
    # cadd = hl.read_table(CADD_HT)
    # gtex = gtex.annotate(
    #     cadd=cadd[gtex.key].cadd,
    # )
    # # add in ensembl ids
    # gtf = hl.experimental.import_gtf(
    #     GENCODE_GTF, reference_genome='GRCh38', skip_invalid_contigs=True, force=True
    # )
    # gtex = gtex.annotate(variant_gene_id=gtf[gtex.locus].gene_id)
    # # export as ht
    # gtex_path_ht = output_path(f'trans_qtl_vep_gnomADcontext95_cadd_annotated.ht')
    # gtex.write(gtex_path_ht, overwrite=True)


if __name__ == '__main__':
    main()  # pylint: disable=E1120
