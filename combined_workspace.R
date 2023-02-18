library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
library(lubridate)

options("GCLOUD_SDK_PATH"="~/Applications/google-cloud-sdk") # don't run in AnVIL workspace
centers <- c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR")
center=rep(centers, times=c(rep(1,4),2))
consent=c(rep("GRU", 5), "HMB")
release <- "U1"
workspaces <- paste("AnVIL_GREGoR", center, release, consent, sep="_")
namespace <- "anvil-datastorage"
combined_workspace <- "GREGOR_COMBINED_CONSORTIUM_U1"
combined_namespace <- "gregor-dcc"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

conversion_function <- function(cm) {
    if (is.character(cm)) {
        return(as.character)
    } else if (is.logical(cm)) {
        return(as.logical)
    } else if (is.integer(cm)) {
        return(as.integer)
    } else if (is.numeric(cm)) {
        return(as.numeric)
    } else if (is.Date(cm)) {
        return(ymd)
    } else if (is.timepoint(cm)) {
        return(ymd_hms)
    } else {
        return(as.character)
    }
}

combine_tables <- function(table_name) {
    lapply(workspaces, function(x) {
        tables <- avtables(namespace=namespace, name=x)
        if (table_name %in% tables$table) {
            table <- avtable(table_name, namespace=namespace, name=x)
            # map columns to expected data types so they can be combined
            for (c in names(table)) {
                FUN <- conversion_function(model[[table_name]][[c]])
                table[[c]] <- FUN(table[[c]])
            }
            return(table)
        } else {
            warning("table ", table_name, " not present in workspace ", x)
            return(NULL)
        }
    }) %>% bind_rows()
}

table_names <- names(model)
table_list <- list()
for (t in table_names) {
    dat <- combine_tables(t)
    # make sure primary key is still unique
    stopifnot(sum(duplicated(dat[[t]])) == 0)
    
    tmpfile <- tempfile()
    write_tsv(dat, tmpfile)
    bucket <- avbucket(namespace=combined_namespace, name=combined_workspace)
    outfile <- paste0(bucket, "/data_tables/", t, ".tsv")
    gsutil_cp(tmpfile, outfile)
    unlink(tmpfile)
    table_list[[t]] <- outfile
}

json <- list("data_table_import.table_files" = table_list,
             "data_table_import.model_url" = "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json",
             "data_table_import.workspace_name" = combined_workspaces,
             "data_table_import.workspace_namespace" = combined_namespace,
             "data_table_import.overwrite" = "true"
) %>% toJSON(pretty=TRUE, auto_unbox=TRUE, unbox=TRUE)
write(json, paste0(combined_workspace, "_data_table_import.json"))
