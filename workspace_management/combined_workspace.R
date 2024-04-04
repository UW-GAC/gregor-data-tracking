library(AnVIL)
library(AnvilDataModels)
library(dplyr)
source("combine_tables.R")
source("workflow_inputs_json.R")

cycle <- "U06"
centers <- list(
  GRU=c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
) %>% unlist() %>% sort()

joint_call_tables <- c("aligned_dna_short_read", "aligned_dna_short_read_set", "called_variants_dna_short_read")
joint_call_workspaces <- paste("AnVIL_GREGoR_DCC", cycle, names(centers), sep="_")

namespace <- "anvil-datastorage"
combined_workspace <- paste0("GREGOR_COMBINED_CONSORTIUM_", cycle)
combined_namespace <- "gregor-dcc"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

table_names <- setdiff(names(model), c("experiment", "aligned"))
  
table_list <- list()
for (t in table_names) {
  workspaces_t <- workspaces
  if (t %in% joint_call_tables) {
    workspaces_t <- c(workspaces, joint_call_workspaces)
  }
  
  dat <- combine_tables(t, model, workspaces=workspaces_t, namespace=namespace)
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

# create experiment and aligned tables
table_list[["experiment"]] <- experiment_table(table_list)
table_list[["aligned"]] <- aligned_table(table_list)

# write tsv files to google bucket
bucket <- avbucket(namespace=combined_namespace, name=combined_workspace)
file_list <- write_to_bucket(table_list, bucket)

# write json for validation
workflow_inputs_json(file_list = file_list, 
                     model_url = model_url, 
                     workspace = combined_workspace, 
                     namespace = combined_namespace)
