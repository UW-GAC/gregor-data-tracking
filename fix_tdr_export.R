library(AnVIL)
library(dplyr)
library(tidyr)
library(readr)
source("workflow_inputs_json.R")

release <- "RELEASE_01"
consent <- c("GRU", "HMB")
workspaces <- paste("AnVIL_GREGoR", release, consent, sep="_")
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"

for (i in seq_along(workspaces)) {
  bucket <- avbucket(namespace=namespace, name=workspaces[i])
  tables <- avtables(namespace=namespace, name=workspaces[i])
  table_list <- list()
  for (t in tables$table) {
    dat <- avtable(t, namespace=namespace, name=workspaces[i])
    
    # custom version of unnest_set_table since it looks different after TDR
    if (grepl("_set$", t)) {
      tmp <- unnest(dat, cols=ends_with(".items"))
      names(tmp)[grepl(".items$", names(tmp))] <- sub("_set$", "_id", t)
      dat <- select(tmp, -ends_with(".itemsType"))
    }
    
    # save original version of table
    tmpfile <- tempfile()
    write_tsv(dat, tmpfile)
    outfile <- paste0(bucket, "/TDR_exported_tables/", t, ".tsv")
    gsutil_cp(tmpfile, outfile)
    unlink(tmpfile)
    
    # fix primary keys after TDR export
    primary_key <- paste0(t, "_id")
    old_key <- paste0("tdr:", primary_key)
    if (old_key %in% names(dat)) {
      stopifnot(all(dat[[primary_key]] == dat[["datarepo_row_id"]]))
      dat2 <- dat %>%
        select(-!!primary_key) %>%
        rename(all_of(setNames(old_key, primary_key)))
    }
    
    # write new table to workspace
    tmpfile <- tempfile()
    write_tsv(dat2, tmpfile)
    bucket <- avbucket(namespace=namespace, name=workspaces[i])
    outfile <- paste0(bucket, "/GREGoR_data_model_tables/", t, ".tsv")
    gsutil_cp(tmpfile, outfile)
    unlink(tmpfile)
    table_list[[t]] <- outfile
  }
  
  # JSON file to use for validation workflow
  workflow_inputs_json(table_list = table_list, 
                       model_url = model_url, 
                       workspace = workspaces[i], 
                       namespace = namespace)
}
