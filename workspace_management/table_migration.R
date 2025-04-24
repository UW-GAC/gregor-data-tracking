library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
source("workflow_inputs_json.R")

cycle1 <- "U10"
cycle2 <- "U11"
centers <- list(
  GRU=c("BCM", "UCI", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces1 <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle1, consent, sep="_")
) %>% unlist() %>% sort()
workspaces2 <- sub(cycle1, cycle2, workspaces1)
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"

for (i in seq_along(workspaces1)) {
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
        bucket <- avbucket(namespace=namespace, name=workspaces2[i])
        outfile <- paste0(bucket, "/", cycle1, "_data_tables/", workspaces1[i], "_", t, ".tsv")
        gsutil_cp(tmpfile, outfile)
        unlink(tmpfile)
        table_list[[t]] <- outfile
    }
    workflow_inputs_json(file_list = table_list, 
                         model_url = model_url, 
                         workspace = workspaces2[i], 
                         namespace = namespace)
}
