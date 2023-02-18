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
release1 <- "U1"
release2 <- "U2"
workspaces1 <- paste("AnVIL_GREGoR", center, release1, consent, sep="_")
workspaces2 <- paste("AnVIL_GREGoR", center, release2, consent, sep="_")
namespace <- "anvil-datastorage"

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
    json <- list("data_table_import.table_files" = table_list,
                 "data_table_import.model_url" = "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json",
                 "data_table_import.workspace_name" = workspaces2[i],
                 "data_table_import.workspace_namespace" = namespace,
                 "data_table_import.overwrite" = "true"
    ) %>% toJSON(pretty=TRUE, auto_unbox=TRUE, unbox=TRUE)
    write(json, paste0(workspaces2[i], "_data_table_import.json"))
}
