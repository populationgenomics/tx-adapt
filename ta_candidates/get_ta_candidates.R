#!/usr/bin/env Rscript

# Investigate evidence of TA in humans using GTEx data

# Library setup and loading data ---------------------------

install.packages("googleCloudStorageR", repos = "http://cran.csiro.au")
install.packages("viridis", repos = "http://cran.csiro.au/")
library(googleCloudStorageR)
library(gargle)
library(tidyverse)
library(viridis)

# Google cloud setup/token authorisation (to get files from GCP)
scope <- c("https://www.googleapis.com/auth/cloud-platform")
token <- gargle::token_fetch(scopes = scope)
googleCloudStorageR::gcs_auth(token = token)

# set bucket
# googleCloudStorageR::gcs_global_bucket("gs://cpg-tx-adapt-test")

# Use VEP v88/Gencode 26
whole_blood_associations <- googleCloudStorageR::gcs_get_object(
  "vep/v7/vep88.10_cadd_GRCh38_annotation.tsv.bgz"
)
paralogous_gene_file <- googleCloudStorageR::gcs_get_object(
  "mohamed_data/paralogs.txt"
)

association_data <- read.table(whole_blood_associations,
  sep = "\t", header = TRUE
)
paralogous_gene_df <- read.csv(
  paste0(input_folder, paralogous_gene_file),
  na.strings = ""
)

# Data cleaning and qc -----------------------

# remove version numbers from association_data$phenotype_id and gene_id
# https://www.biostars.org/p/302441/
association_data$phenotype_id <- gsub(
  "\\..*", "", association_data$phenotype_id
)
association_data$gene_id <- gsub(
  "\\..*", "", association_data$gene_id
)

# make sure all gene IDs in the paralog file and eqtl association file match
matching_genes <- which(
  association_data$phenotype_id %in% paralogous_gene_df$Gene.stable.ID
)
association_data <- association_data[matching_genes, ]

# label variants as being LoF, missense, or nonsynonymous
lof <- c(
  "transcript_ablation", "splice_acceptor_variant",
  "splice_donor_variant", "stop_gained", "frameshift_variant",
  "start_lost", "transcript_amplification"
)
association_data <- association_data %>% mutate(variant_category = case_when(
  most_severe_consequence == "missense_variant" ~ "missense",
  most_severe_consequence == "synonymous_variant" ~ "synonymous",
  most_severe_consequence %in% lof ~ "lof",
  TRUE ~ as.character(most_severe_consequence)
))

# Only keep variant categories of interest
association_data <- association_data %>% filter(
  variant_category %in% c(
    "missense", "synonymous", "lof"
  )
)

# Define missense variants based on their deleteriousness
# First, clean cadd column so that it is numeric
association_data$cadd <- gsub(
  "\\{PHRED:|\\}", "", association_data$cadd
) %>% as.numeric(.)
# Assign deleteriousness. Make cutoff 15, as "this also happens to be the
# median value for all possible canonical splice site changes and
# non-synonymous variants in CADD v1.0": see https://cadd.gs.washington.edu/info
association_data <- association_data %>% mutate(variant_category = case_when(
  cadd <= 15 & variant_category == "missense" ~ "missense_benign",
  cadd > 15 & variant_category == "missense" ~ "missense_deleterious",
  TRUE ~ as.character(variant_category)
))
# do this for the most severe consequence column as well
association_data <- association_data %>%
  mutate(most_severe_consequence = case_when(
    cadd < 15 &
      most_severe_consequence == "missense_variant" ~ "missense_benign",
    cadd > 15 &
      most_severe_consequence == "missense_variant" ~ "missense_deleterious",
    TRUE ~ as.character(most_severe_consequence)
  ))

# Assign gene paralogs (and self-expression) --------------------------

# Here, we want to match the gene_id in the association_data df to the
# Gene.stable.ID in the paralogous gene df. Once these are linked, check whether
# the Human.paralogue.gene.stable.ID of the paralogous gene df is in the
# phenotype ID column the association_data df

# Collapse duplicated Gene.stable.IDs and aggregate
# Human.paralogue.gene.stable.IDs into a vector
collapsed_paralog_df <- paralogous_gene_df %>%
  group_by(Gene.stable.ID) %>%
  summarise(
    Human.paralogue.gene.stable.ID = list(Human.paralogue.gene.stable.ID)
  )
# for clarity, assign names as the gene stable IDs
names(
  collapsed_paralog_df$Human.paralogue.gene.stable.ID
) <- collapsed_paralog_df$Gene.stable.ID
# match gene ids to Gene.stable.IDs and collect matched paralogous genes
matched_paralogs <- collapsed_paralog_df[match(
  association_data$gene_id, collapsed_paralog_df$Gene.stable.ID
), "Human.paralogue.gene.stable.ID"]
is_paralog <- sapply(
  seq_along(association_data$phenotype_id),
  function(x) {
    association_data$phenotype_id[x] %in%
      matched_paralogs$Human.paralogue.gene.stable.ID[x][[1]]
  }
)
# assign whether a variant has a paralogous gene as an eQTL
association_data$is_paralog <- is_paralog

# label variants that are an eQTL to themselves
association_data <- association_data %>% mutate(
  is_paralog = replace(is_paralog, gene_id == phenotype_id, "Self-expression")
)
# make paralogs into distinct, readable categories
association_data$is_paralog <- factor(gsub(
  "FALSE", "Non-paralogous",
  gsub("TRUE", "Paralogous", association_data$is_paralog)
))

# filter down to significant genes only
# significance here is defined as 5x10-8 (for genome-wide significance)
genome_wide_sig <- association_data %>% filter(pval_nominal <= 5e-8)
exome_wide_sig <- association_data %>% filter(pval_nominal <= 1e-5)

print(nrow(genome_wide_sig))

# # Which genes are these?
# lof_candidates <- exome_wide_sig %>%
#   filter(variant_category == "lof" & is_paralog == "Paralogous")
# write.table(
#   lof_candidates,
#   file = paste0(output_folder, "lof_candidates_all.txt"), sep = "\t",
#   row.names = FALSE
# )

# # Plot data ---------------------------

# plot_data <- function(df, title, plot_categegory, xlab, keep_facet) {
#   # reassign paralogue names for better interpretability in plot

#   p <- ggplot(df, aes(
#     x = factor({{ plot_categegory }}), fill = {{ plot_categegory }}
#   )) +
#     geom_bar(stat = "count", width = 0.7, alpha = 0.8) +
#     geom_text(stat = "count", aes(label = ..count..), size = 5, vjust = -1) +
#     scale_fill_viridis_d() +
#     theme_bw() +
#     ggtitle(title) +
#     scale_x_discrete(guide = guide_axis(angle = 45)) +
#     xlab(xlab) +
#     ylab("n Variants") +
#     theme(
#       axis.text.x = element_blank(),
#       axis.ticks.x = element_blank(),
#       legend.title = element_blank(),
#       strip.text.x = element_text(size = 17),
#       text = element_text(size = 17)
#     )
#   if (tolower(keep_facet) == "yes") {
#     p <- p + facet_grid(. ~ is_paralog, )
#   }
#   # save plot
#   pdf(
#     paste0(
#       output_folder,
#       deparse(substitute(df)), "_", deparse(substitute(plot_categegory)), ".pdf"
#     ),
#     width = 14, height = 8
#   )
#   print(p)
#   dev.off()
#   return(p)
# }

# # plot data
# plot_data(
#   df = genome_wide_sig, title = "Genome-wide Significance",
#   plot_categegory = most_severe_consequence,
#   xlab = "Most Severe Consequence", keep_facet = "Yes"
# )
# plot_data(
#   df = exome_wide_sig, title = "Exome-wide Significance",
#   plot_categegory = most_severe_consequence,
#   xlab = "Most Severe Consequence", keep_facet = "Yes"
# )
# # now let's plot looking at the variant category
# plot_data(
#   df = genome_wide_sig, title = "Genome-wide Significance",
#   plot_categegory = variant_category,
#   xlab = "Variant category", keep_facet = "Yes"
# )
# plot_data(
#   df = exome_wide_sig, title = "Exome-wide Significance",
#   plot_categegory = variant_category,
#   xlab = "Variant category", keep_facet = "Yes"
# )

# # -----------------------------

# # Filter down to instances where variant leads to decreased expression
# # of the gene it’s in, as well as increased expression of the paralog
# negative_slope_ids <- association_data %>%
#   filter(is_paralog == "Self-expression" & slope < 0) %>%
#   pull(variant_id)
# # get all rows where variant_id has a negative slope against the gene it's in
# # then extract rows with a positive slope

# candidate_variants <- association_data %>%
#   filter(variant_id %in% negative_slope_ids) %>%
#   filter(slope > 0)

# # do any of these survive multiple testing correction?
# # We'll start with the higher threshold, genome wide sig
# genome_wide_sig_candidates <- candidate_variants %>%
#   filter(pval_nominal <= 5e-8)
# # save file
# write.table(
#   genome_wide_sig_candidates,
#   file = paste0(output_folder, "genome_wide_sig_candidates.txt"), sep = "\t",
#   row.names = FALSE
# )
# # Save just paralogous candidates
# paralogous_candidates <- genome_wide_sig_candidates %>%
#   filter(is_paralog == "Paralogous")
# write.table(
#   paralogous_candidates,
#   file = paste0(
#     output_folder, "genome_wide_sig_candidates_paralogous.txt"
#   ), sep = "\t",
#   row.names = FALSE
# )

# # plot
# plot_data(
#   df = genome_wide_sig_candidates, title = "Genome-wide Significance",
#   plot_categegory = most_severe_consequence, xlab = "Most Severe Consequence",
#   keep_facet = "Yes"
# )

# # Finally, for self-expression, look at just up-regulated instances
# # start with exome-wide significance
# positive_slope_self_expression <- association_data %>%
#   filter(is_paralog == "Self-expression" & slope > 0) %>%
#   filter(pval_nominal <= 1e-5)
# nrow(positive_slope_self_expression)
# # 69
