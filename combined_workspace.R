library(AnVIL)
library(AnvilDataModels)
library(dplyr)
library(readr)
library(lubridate)
library(jsonlite)

options("GCLOUD_SDK_PATH"="~/Applications/google-cloud-sdk") # don't run in AnVIL workspace
centers <- c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR")
center=rep(centers, times=c(rep(1,4),2))
consent=c(rep("GRU", 5), "HMB")
release <- "U2"
workspaces <- paste("AnVIL_GREGoR", center, release, consent, sep="_")
namespace <- "anvil-datastorage"
combined_workspace <- paste0("GREGOR_COMBINED_CONSORTIUM_", release)
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
    #print(paste("table", table_name))
    lapply(workspaces, function(x) {
        #print(paste("workspace", x))
        tables <- avtables(namespace=namespace, name=x)
        if (table_name %in% tables$table) {
            table <- avtable(table_name, namespace=namespace, name=x)
            # map columns to expected data types so they can be combined
            if (!grepl("_set$", t)) {
                for (c in names(table)) {
                    #print(paste("column", c))
                    FUN <- conversion_function(model[[table_name]][[c]])
                    table[[c]] <- FUN(table[[c]])
                }
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
    if (grepl("_set$", t)) {
        dat <- unnest_set_table(dat)
    }
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

json <- list("validate_data_model.table_files" = table_list,
             "validate_data_model.model_url" = model_url,
             "validate_data_model.workspace_name" = combined_workspace,
             "validate_data_model.workspace_namespace" = combined_namespace,
             "validate_data_model.import_tables" = "true"
) %>% toJSON(pretty=TRUE, auto_unbox=TRUE, unbox=TRUE)
write(json, paste0(combined_workspace, "_validate_data_model.json"))
