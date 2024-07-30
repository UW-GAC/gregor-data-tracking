library(AnVIL)
library(dplyr)
library(readr)
source("compare_tables.R")

cycle1 <- "U06"
cycle2 <- "U07"
centers <- list(
  GRU=c("BCM", "UCI", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces1 <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle1, consent, sep="_")
) %>% unlist() %>% sort()
workspaces2 <- sub(cycle1, cycle2, workspaces1)
namespace <- "anvil-datastorage"

combined_bucket <- avbucket(namespace="gregor-dcc", name=paste0("GREGOR_COMBINED_CONSORTIUM_", cycle2))

for (i in seq_along(workspaces1)) {
  message(workspaces1[i])
  tables <- avtables(namespace=namespace, name=workspaces1[i])
  tables_to_check <- setdiff(tables$table[!(grepl("_set$", tables$table))], "genetic_findings")
  table1_list <- lapply(tables_to_check, avtable, namespace=namespace, name=workspaces1[i])
  names(table1_list) <- tables_to_check
  table2_list <- lapply(tables_to_check, avtable, namespace=namespace, name=workspaces2[i])
  names(table2_list) <- tables_to_check
  
  table_comparison <- compare_tables(table1_list, table2_list, 
                                     table1_prefix=cycle1, table2_prefix=cycle2)
  
  for (t in names(table_comparison$table_diff_list)) {
    # print differences for each table
    outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_", t, ".txt")
    writeLines(knitr::kable(table_comparison$table_diff_list[[t]]), outfile)
    gsutil_cp(outfile, paste0(combined_bucket, "/", cycle2, "_QC/"))
    gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
  }
  
  # print summary
  outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_summary.txt")
  writeLines(knitr::kable(table_comparison$summary_list), outfile)
  gsutil_cp(outfile, paste0(combined_bucket, "/", cycle2, "_QC/"))
  gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
}
