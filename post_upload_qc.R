library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
source("release_qc.R")

cycle <- "U04"
centers <- list(
  GRU=c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
) %>% unlist() %>% sort()
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"

for (w in workspaces) {
  message(w)
  tables <- avtables(namespace=namespace, name=w)
  table_names <- tables$table
  
  table_list <- list()
  for (t in table_names) {
    dat <- avtable(t, namespace=namespace, name=w)

    if (nrow(dat) > 0) {
      if (grepl("_set$", t)) {
        dat <- unnest_set_table(dat)
      }
    
      table_list[[t]] <- dat
    }
  }
  
  message("generating validation report")
  table_files <- sapply(table_list, function(x) {
    outfile <- tempfile()
    write_tsv(x, file=outfile)
    return(outfile)
  })
  params <- list(tables=table_files, model=model_url)
  report_file <- paste0(w, "_validation")
  custom_render_markdown("data_model_report", report_file, parameters=params)
  unlink(table_files)
  
  message("copying validation file")
  out_dir <- paste0(avbucket(namespace=namespace, name=w), "/post_upload_qc/")
  gsutil_cp(paste0(report_file, ".html"), out_dir)
  
  # QC on tables
  message("generating QC report")
  qc_table_list <- release_qc(table_list, cascading=FALSE)
  log <- release_qc_log(table_list, qc_table_list)
  report_file <- paste0(w, "_post_upload_qc.txt")
  message("copying QC file")
  writeLines(knitr::kable(log), report_file)
  gsutil_cp(report_file, out_dir)
}
