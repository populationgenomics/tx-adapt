#!/usr/bin/env Rscript

# Investigate evidence of TA in humans using GTEx data

# Library setup and loading data ----------------------------------------------

install.packages("googleCloudStorageR", repos = "http://cran.csiro.au")
install.packages("viridis", repos = "http://cran.csiro.au/")
install.packages("argparser", repos = "http://cran.csiro.au/")
library(googleCloudStorageR)
library(viridis)
library(argparser)
library(gargle)
library(tidyverse)
library(glue)

# Google cloud setup/token authorisation (to get files from GCP)
scope <- c("https://www.googleapis.com/auth/cloud-platform")
token <- gargle::token_fetch(scopes = scope)
googleCloudStorageR::gcs_auth(token = token)

# set bucket
googleCloudStorageR::gcs_global_bucket("gs://cpg-tx-adapt-test")

# create parser object and define flags
p <- arg_parser("gtex")
# Add a positional argument
p <- add_argument(p, "--gtex_file", help = "Name of gtex file")
argv <- parse_args(p)

# Copy in association analysis and paralogous gene files
system(glue(
  "gsutil cp {argv$gtex_file} gtex_annotation_file.tsv.bgz"
))
system(
  "gsutil cp gs://cpg-tx-adapt-test/mohamed_data/paralogs.txt paralogs.txt"
)
# read in files once copied
association_data <- read.table(
  "gtex_annotation_file.tsv.bgz",
  sep = "\t", header = TRUE
)
print(head(association_data))
# paralogous_gene_df <- read.csv("paralogs.txt",
#   na.strings = ""
# )

# # Data cleaning and qc --------------------------------------------------------

# # remove version numbers from association_data$phenotype_id and gene_id
# # https://www.biostars.org/p/302441/
# association_data$phenotype_id <- gsub(
#   "\\..*", "", association_data$phenotype_id
# )
# association_data$gene_id <- gsub(
#   "\\..*", "", association_data$gene_id
# )

# # make sure all gene IDs in the paralog file and eqtl association file match
# matching_genes <- which(
#   association_data$phenotype_id %in% paralogous_gene_df$Gene.stable.ID
# )
# association_data <- association_data[matching_genes, ]

# # label variants as being LoF, missense, or nonsynonymous
# lof <- c(
#   "transcript_ablation", "splice_acceptor_variant",
#   "splice_donor_variant", "stop_gained", "frameshift_variant",
#   "start_lost", "transcript_amplification"
# )
# association_data <- association_data %>% mutate(variant_category = case_when(
#   most_severe_consequence == "missense_variant" ~ "missense",
#   most_severe_consequence == "synonymous_variant" ~ "synonymous",
#   most_severe_consequence %in% lof ~ "lof",
#   TRUE ~ as.character(most_severe_consequence)
# ))

# # Only keep variant categories of interest
# association_data <- association_data %>% filter(
#   variant_category %in% c(
#     "missense", "synonymous", "lof"
#   )
# )

# # Define missense variants based on their deleteriousness
# # First, clean cadd column so that it is numeric
# association_data$cadd <- gsub(
#   "\\{PHRED:|\\}", "", association_data$cadd
# ) %>% as.numeric(.)
# # Assign deleteriousness. Make cutoff 15, as "this also happens to be the
# # median value for all possible canonical splice site changes and
# # non-synonymous variants in CADD v1.0": see https://cadd.gs.washington.edu/info
# association_data <- association_data %>% mutate(variant_category = case_when(
#   cadd <= 15 & variant_category == "missense" ~ "missense_benign",
#   cadd > 15 & variant_category == "missense" ~ "missense_deleterious",
#   TRUE ~ as.character(variant_category)
# ))
# # do this for the most severe consequence column as well
# association_data <- association_data %>%
#   mutate(most_severe_consequence = case_when(
#     cadd < 15 &
#       most_severe_consequence == "missense_variant" ~ "missense_benign",
#     cadd > 15 &
#       most_severe_consequence == "missense_variant" ~ "missense_deleterious",
#     TRUE ~ as.character(most_severe_consequence)
#   ))

# # Assign gene paralogs (and self-expression) ----------------------------------

# # Here, we want to match the gene_id in the association_data df to the
# # Gene.stable.ID in the paralogous gene df. Once these are linked, check whether
# # the Human.paralogue.gene.stable.ID of the paralogous gene df is in the
# # phenotype ID column the association_data df

# # Collapse duplicated Gene.stable.IDs and aggregate
# # Human.paralogue.gene.stable.IDs into a vector
# collapsed_paralog_df <- paralogous_gene_df %>%
#   group_by(Gene.stable.ID) %>%
#   summarise(
#     Human.paralogue.gene.stable.ID = list(Human.paralogue.gene.stable.ID)
#   )
# # for clarity, assign names as the gene stable IDs
# names(
#   collapsed_paralog_df$Human.paralogue.gene.stable.ID
# ) <- collapsed_paralog_df$Gene.stable.ID
# # match gene ids to Gene.stable.IDs and collect matched paralogous genes
# matched_paralogs <- collapsed_paralog_df[match(
#   association_data$gene_id, collapsed_paralog_df$Gene.stable.ID
# ), "Human.paralogue.gene.stable.ID"]
# is_paralog <- sapply(
#   seq_along(association_data$phenotype_id),
#   function(x) {
#     association_data$phenotype_id[x] %in%
#       matched_paralogs$Human.paralogue.gene.stable.ID[x][[1]]
#   }
# )
# # assign whether a variant has a paralogous gene as an eQTL
# association_data$is_paralog <- is_paralog

# # label variants that are an eQTL to themselves
# association_data <- association_data %>% mutate(
#   is_paralog = replace(is_paralog, gene_id == phenotype_id, "Self-expression")
# )
# # make paralogs into distinct, readable categories
# association_data$is_paralog <- factor(gsub(
#   "FALSE", "Non-paralogous",
#   gsub("TRUE", "Paralogous", association_data$is_paralog)
# ))

# # filter down to significant genes only
# # significance here is defined as 5x10-8 (for genome-wide significance)
# genome_wide_sig <- association_data %>% filter(pval_nominal <= 5e-8)
# exome_wide_sig <- association_data %>% filter(pval_nominal <= 1e-5)

# # Write out table for which genes these are
# # both at genome and exome-level significance
# genome_wide_tsv <- "genome_wide_sig_candidates_either_direction.tsv"
# exome_wide_tsv <- "genome_wide_sig_candidates_either_direction.tsv"
# write.table(
#   genome_wide_sig,
#   file = genome_wide_tsv, sep = "\t",
#   row.names = FALSE
# )
# write.table(
#   exome_wide_sig,
#   file = exome_wide_tsv, sep = "\t",
#   row.names = FALSE
# )
# gcs_outdir <- "gs://cpg-tx-adapt-test/output/"
# system(glue("gsutil cp {genome_wide_tsv} {exome_wide_tsv} {gcs_outdir}"))

# # Plot data -------------------------------------------------------------------

# # set output directory for plotting
# gcs_image_outdir <- "gs://cpg-tx-adapt-test-web/most_severe_consequences/"

# # make plotting function, as this is heavily repeated
# plot_data <- function(df, title, plot_categegory, xlab, keep_facet) {
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
#   ta_plot <- paste0(
#     deparse(substitute(df)), "_", deparse(substitute(plot_categegory)), ".pdf"
#   )
#   pdf(ta_plot, width = 14, height = 8)
#   print(p)
#   dev.off()
#   # copy pdf to system
#   system(glue("gsutil cp {ta_plot} {gcs_image_outdir}"))
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
# # plot looking at the variant category
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

# # Get TA candidates -----------------------------------------------------------

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

# # Filter for multiple testing correction
# exome_wide_sig_candidates <- candidate_variants %>%
#   filter(pval_nominal <= 1e-5)
# genome_wide_sig_candidates <- candidate_variants %>%
#   filter(pval_nominal <= 5e-8)
# # save files
# exome_wide_ta_tsv <- "exome_wide_sig_ta_candidates.tsv"
# genome_wide_ta_tsv <- "genome_wide_sig_ta_candidates.tsv"
# write.table(
#   exome_wide_sig_candidates,
#   file = exome_wide_ta_tsv, sep = "\t",
#   row.names = FALSE
# )
# write.table(
#   genome_wide_sig_candidates,
#   file = genome_wide_ta_tsv, sep = "\t",
#   row.names = FALSE
# )
# system(glue("gsutil cp {exome_wide_ta_tsv} {genome_wide_ta_tsv} {gcs_outdir}"))

# # plot data
# plot_data(
#   df = genome_wide_sig_candidates, title = "Genome-wide Significance",
#   plot_categegory = most_severe_consequence, xlab = "Most Severe Consequence",
#   keep_facet = "Yes"
# )
