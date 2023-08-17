library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
library(jsonlite)
source("combine_tables.R")
source("workflow_inputs_json.R")

centers <- c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR")
center <- rep(centers, times=c(rep(1,3),rep(2,2)))
consent <- c(rep("GRU", 3), rep(c("GRU", "HMB"), 2))
cycle <- "U03"
workspaces <- paste("AnVIL_GREGoR", center, cycle, consent, sep="_")
namespace <- "anvil-datastorage"
combined_workspace <- paste0("GREGOR_COMBINED_CONSORTIUM_", cycle)
combined_namespace <- "gregor-dcc"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

table_names <- names(model)
  
table_list <- list()
experiment_tables <- list()
bucket <- avbucket(namespace=combined_namespace, name=combined_workspace)
for (t in table_names) {
    dat <- combine_tables(t, model, workspaces=workspaces, namespace=namespace)
    if (grepl("_set$", t)) {
        dat <- unnest_set_table(dat)
    }
    # only proceed if we have any data for this table
    if (nrow(dat) > 0) {
      
      # make sure primary key is still unique
      stopifnot(sum(duplicated(dat[[t]])) == 0)
    
      tmpfile <- tempfile()
      write_tsv(dat, tmpfile)
      outfile <- paste0(bucket, "/data_tables/", t, ".tsv")
      gsutil_cp(tmpfile, outfile)
      unlink(tmpfile)
      table_list[[t]] <- outfile
      
      # save tables to combine
      if (t == "analyte") {
        analyte <- dat
      }
      if (grepl("^experiment", t)) {
        experiment_tables[[t]] <- dat
      }
    }
}

experiment <- experiment_table(experiment_tables)
tmpfile <- tempfile()
write_tsv(experiment, tmpfile)
outfile <- paste0(bucket, "/data_tables/experiment.tsv")
gsutil_cp(tmpfile, outfile)
unlink(tmpfile)
table_list[["experiment"]] <- outfile

workflow_inputs_json(table_list = table_list, 
                     model_url = model_url, 
                     workspace = combined_workspace, 
                     namespace = combined_namespace)
