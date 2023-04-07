library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(tidyr)
library(readr)
library(jsonlite)

options("GCLOUD_SDK_PATH"="~/Applications/google-cloud-sdk") # don't run in AnVIL workspace
centers <- c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR")
center=rep(centers, times=c(rep(1,4),2))
consent=c(rep("GRU", 5), "HMB")
release1 <- "U2"
release2 <- "U3"
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
    json <- list("validate_data_model.table_files" = table_list,
                 "validate_data_model.model_url" = model_url,
                 "validate_data_model.workspace_name" = workspaces2[i],
                 "validate_data_model.workspace_namespace" = namespace,
                 "validate_data_model.import_tables" = "true"
    ) %>% toJSON(pretty=TRUE, auto_unbox=TRUE, unbox=TRUE)
    write(json, paste0(workspaces2[i], "_validate_data_model.json"))
}
