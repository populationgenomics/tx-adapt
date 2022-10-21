#!/usr/bin/env python3


"""
Add annotation to INDEL table
"""

import hail as hl
from cpg_utils.hail_batch import init_batch, output_path


GTEX_INDELS = 'gs://cpg-tx-adapt-test/vep/v10/gtex_indels.ht'
VEP_INDELS = 'gs://cpg-tob-wgs-test/kat/vep_annotations.ht'
SNV_HT = 'gs://cpg-tx-adapt-test/vep/v8/gtex_association_all_positions_maf01_vep95_cadd_annotated.ht/'


def main():
    """
    Add INDEL annotation
    """

    init_batch()

    indels = hl.read_table(GTEX_INDELS)
    ht = hl.read_table(VEP_INDELS)

    ht = ht.annotate(split_input=ht.vep.input.split('\t'))
    ht = ht.annotate(alleles=[ht.split_input[3], ht.split_input[4]])
    ht = ht.key_by(ht.locus, ht.alleles)

    # add VEP annotation to indel ht
    indels = indels.annotate(
        most_severe_consequence=ht[indels.key].vep.most_severe_consequence,
        consequence_terms=ht[indels.key].vep.transcript_consequences.consequence_terms,
        transcript_id=ht[indels.key].vep.transcript_consequences.transcript_id,
    )

    # load in SNVs and make union on tables
    snvs = hl.read_table(SNV_HT)
    gtex_union = snvs.union(indels)
    gtex_union.write(output_path('union_snv_indels.ht'))


if __name__ == '__main__':
    main()  # pylint: disable=E1120
