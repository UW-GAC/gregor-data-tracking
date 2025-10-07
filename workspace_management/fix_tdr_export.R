library(AnVIL)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
source("workflow_inputs_json.R")

release <- "R04"
consent <- c("GRU", "HMB")
workspaces <- paste("AnVIL_GREGoR", release, consent, sep="_")
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/0418b30badca6dcfeb74b655df6e1831f0d22bd2/GREGoR_data_model.json"
#model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- AnvilDataModels::json_to_dm(model_url)

write_original_table <- function(dat, table_name, bucket) {
  tmpfile <- tempfile()
  write_tsv(dat, tmpfile)
  outfile <- paste0(bucket, "/TDR_exported_tables/", table_name, ".tsv")
  avcopy(tmpfile, outfile)
  unlink(tmpfile)
}

for (i in seq_along(workspaces)) {
  bucket <- avstorage(namespace=namespace, name=workspaces[i])
  tables <- avtables(namespace=namespace, name=workspaces[i]) %>%
    filter(table %in% names(model))
  table_list <- list()
  for (t in tables$table) {
    print(paste(workspaces[i], t))
    dat <- avtable(t, namespace=namespace, name=workspaces[i])
    
    # custom version of unnest_set_table since it looks different after TDR
    if (grepl("_set$", t)) {
      tmp <- unnest(dat, cols=ends_with(".items"))
      names(tmp)[grepl(".items$", names(tmp))] <- sub("_set$", "_id", t)
      dat <- select(tmp, -ends_with(".itemsType"))
    }
    
    # save original version of table
    write_original_table(dat, table_name=t, bucket=bucket)
    
    # fix primary keys after TDR export
    primary_key <- paste0(t, "_id")
    old_key <- paste0("tdr:", primary_key)
    stopifnot(all(dat[[primary_key]] == dat[["datarepo_row_id"]]))
    dat2 <- dat %>%
      select(-!!primary_key) %>%
      rename(all_of(setNames(old_key, primary_key)))
    
    # drop extraneous columns
    dat2 <- dat2 %>%
      select(-starts_with("import:"), -any_of("ingest_provenance"))
    
    ## implement fix for column names coerced to lower case and 5prime -> t_5prime
    model_names <- names(model[[t]])
    dat_names <- names(dat2)
    fix_case <- model_names[tolower(model_names) %in% dat_names & !(model_names %in% dat_names)]
    for (n in fix_case) {
      names(dat2)[names(dat2) == tolower(n)] <- n
    }
    fix_prefix <- model_names[paste0("t_", model_names) %in% dat_names & !(model_names %in% dat_names)]
    for (n in fix_prefix) {
      names(dat2)[names(dat2) == paste0("t_", n)] <- n
    }
    
    # write new table to workspace
    tmpfile <- tempfile()
    write_tsv(dat2, tmpfile)
    outfile <- paste0(bucket, "/GREGoR_data_model_tables/", t, ".tsv")
    avcopy(tmpfile, outfile)
    unlink(tmpfile)
    table_list[[t]] <- outfile
  }
  
  # JSON file to use for validation workflow
  workflow_inputs_json(file_list = table_list, 
                       model_url = model_url, 
                       workspace = workspaces[i], 
                       namespace = namespace)
}


## check
prep_workspaces <- paste("AnVIL_GREGoR", release, "prep", consent, sep="_")
for (i in seq_along(workspaces)) {
  tables <- avtables(namespace=namespace, name=workspaces[i]) %>%
    filter(table %in% names(model))
  
  for (t in tables$table) {
    print(paste(workspaces[i], t))
    dat <- avtable(t, namespace=namespace, name=workspaces[i])
    prep <- avtable(t, namespace=namespace, name=prep_workspaces[i])
    id <- paste0(t, "_id")
    stopifnot(setequal(dat[[id]], prep[[id]]))
    stopifnot(nrow(dat) == nrow(prep))
  }
}

