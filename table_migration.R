library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
source("workflow_inputs_json.R")

centers <- c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR")
center <- rep(centers, times=c(rep(1,3),rep(2,2)))
consent <- c(rep("GRU", 3), rep(c("GRU", "HMB"), 2))
release1 <- "U03"
release2 <- "U04"
workspaces1 <- paste("AnVIL_GREGoR", center, release1, consent, sep="_")
workspaces2 <- paste("AnVIL_GREGoR", center, release2, consent, sep="_")
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"

for (i in seq_along(workspaces1)) {
    tables <- avtables(namespace=namespace, name=workspaces1[i])
    table_list <- list()
    for (t in tables$table) {
        dat <- avtable(t, namespace=namespace, name=workspaces1[i])
        if (grepl("_set$", t)) {
            dat <- unnest_set_table(dat)
        }
        tmpfile <- tempfile()
        write_tsv(dat, tmpfile)
        bucket <- avbucket(namespace=namespace, name=workspaces2[i])
        outfile <- paste0(bucket, "/", release1, "_data_tables/", workspaces1[i], "_", t, ".tsv")
        gsutil_cp(tmpfile, outfile)
        unlink(tmpfile)
        table_list[[t]] <- outfile
    }
    workflow_inputs_json(table_list = table_list, 
                         model_url = model_url, 
                         workspace = workspaces2[i], 
                         namespace = namespace)
}
