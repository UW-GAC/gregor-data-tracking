library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
source("workflow_inputs_json.R")

cycle1 <- "P02"
cycle2 <- "P03"
center <- "IHOPE"
consent <- "HMB"
workspaces1 <- paste("AnVIL_GREGoR", center, cycle1, consent, sep="_")
workspaces2 <- sub(cycle1, cycle2, workspaces1)
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/refs/heads/v1.9.2/GREGoR_data_model.json"
#model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"

i <- 1
message(workspaces1[i])
tables <- avtables(namespace=namespace, name=workspaces1[i])
table_list <- list()
for (t in tables$table) {
  message(t)
  dat <- avtable(t, namespace=namespace, name=workspaces1[i])
  if (grepl("_set$", t)) {
    dat <- unnest_set_table(dat)
  }
  tmpfile <- tempfile()
  write_tsv(dat, tmpfile)
  bucket <- avstorage(namespace=namespace, name=workspaces2[i])
  outfile <- paste0(bucket, "/", cycle1, "_data_tables/", workspaces1[i], "_", t, ".tsv")
  avcopy(tmpfile, outfile)
  unlink(tmpfile)
  table_list[[t]] <- outfile
}
workflow_inputs_json(file_list = table_list, 
                     model_url = model_url, 
                     workspace = workspaces2[i], 
                     namespace = namespace)
