library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
library(jsonlite)
source("combine_tables.R")
source("release_qc.R")
source("workflow_inputs_json.R")

cycle <- "U03"
release <- "R01"
centers <- list(
  GRU=c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
)
names(workspaces) <- names(centers)
namespace <- "anvil-datastorage"
combined_namespace <- namespace

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

table_names <- names(model)

for (consent in names(workspaces)) {
  table_list <- list()
  for (t in table_names) {
    dat <- combine_tables(t, model, workspaces=workspaces[[consent]], namespace=namespace)
    # only proceed if we have any data for this table
    if (nrow(dat) > 0) {
      if (grepl("_set$", t)) {
        dat <- unnest_set_table(dat)
      }
      
      # make sure primary key is still unique
      stopifnot(sum(duplicated(dat[[t]])) == 0)
    
      table_list[[t]] <- dat
    }
  }
  
  # QC on tables for release
  table_list <- release_qc(table_list)
  
  # create experiment table
  table_list[["experiment"]] <- experiment_table(table_list)
  
  # write tsv files to google bucket
  combined_workspace <- paste("AnVIL_GREGoR", release, consent, sep="_")
  bucket <- avbucket(namespace=combined_namespace, name=combined_workspace)
  file_list <- write_to_bucket(table_list, bucket)
  
  # write json for validation
  workflow_inputs_json(file_list = file_list, 
                       model_url = model_url, 
                       workspace = combined_workspace, 
                       namespace = combined_namespace)
}

