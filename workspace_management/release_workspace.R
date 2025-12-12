library(AnVIL)
#BiocManager::install("AnVILGCP")
#remotes::install_github("UW-GAC/AnvilDataModels")
library(AnvilDataModels)
library(dplyr)
library(readr)
library(jsonlite)
source("combine_tables.R")
source("release_qc.R")
source("workflow_inputs_json.R")

cycle <- "U11"
release <- "R04"
prev_release <- "R03" # to check for dropped participants
centers <- list(
  GRU=c("BCM", "UCI", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
)
names(workspaces) <- names(centers)
workspaces[["HMB"]] <- c(workspaces[["HMB"]], "AnVIL_GREGoR_IHOPE_P01_HMB")
namespace <- "anvil-datastorage"
combined_namespace <- namespace

joint_call_tables <- c("aligned_dna_short_read", "aligned_dna_short_read_set", "called_variants_dna_short_read")
joint_call_workspaces <- lapply(names(centers), function(x)
  paste("AnVIL_GREGoR_DCC", cycle, x, sep="_")
)
names(joint_call_workspaces) <- names(centers)

reprocessed_mapping_file <- "gs://fc-secure-046bd4ec-b98e-44a6-8fb4-5c5a86642263/data_tables/reprocessedData_notin_mtCallset_R04.tsv"
#avcopy(reprocessed_mapping_file, ".")
reprocessed_map <- read_tsv(basename(reprocessed_mapping_file))

sample_remove_file <- "gs://fc-secure-c0f33243-22f5-4fb9-826a-2a4eaffdf5a9/U11_QC/U11_samples_to_remove.tsv"
#avcopy(sample_remove_file, ".")
samples_to_remove <- read_tsv(basename(sample_remove_file)) %>%
  select(participant_id)

# use data model version 1.9
model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/0418b30badca6dcfeb74b655df6e1831f0d22bd2/GREGoR_data_model.json"
#model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/refs/heads/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

table_names <- setdiff(names(model), c("experiment", "aligned"))
# testing
#table_names <- c("participant", "analyte", "family", "phenotype", table_names[grepl("dna_short_read", table_names)])

log_dir <- file.path(avstorage(), paste0(release, "_QC"))

for (consent in names(workspaces)) {
  table_list <- list()
  for (t in table_names) {
    workspaces_t <- workspaces[[consent]]
    if (t %in% joint_call_tables) {
      workspaces_t <- c(workspaces_t, joint_call_workspaces[[consent]])
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
  
  # drop participants
  remove <- intersect(samples_to_remove$participant_id, table_list$participant$participant_id)
  if (length(remove) > 0) {
    original_table_list <- table_list
    table_list <- remove_participants(remove, table_list)
    drop_participants_log <- release_qc_log(original_table_list, table_list)
    report_file <- paste0(release, "_dropped_participants_", consent, ".log")
    writeLines(knitr::kable(drop_participants_log), report_file)
    avcopy(report_file, file.path(log_dir, report_file))
  }
  
  # QC on tables for release
  original_table_list <- table_list
  table_list <- release_qc(table_list)
  qc_log <- release_qc_log(original_table_list, table_list)
  report_file <- paste0(release, "_release_qc_", consent, ".log")
  writeLines(knitr::kable(qc_log), report_file)
  avcopy(report_file, file.path(log_dir, report_file))
  
  # check for participants dropped from previous release
  prev_release_workspace <- paste("AnVIL_GREGoR", prev_release, consent, sep="_")
  prev_participant <- avtable("participant", namespace=combined_namespace, name=prev_release_workspace)
  withdrawn_participants <- setdiff(prev_participant$participant_id, table_list$participant$participant_id)
  if (length(withdrawn_participants) > 0) {
    report_file <- paste0(release, "_withdrawn_participants_", consent, ".txt")
    writeLines(withdrawn_participants, report_file)
    avcopy(report_file, file.path(log_dir, report_file))
  }
  
  # for reprocessed files, remove source file
  # for now, just one reprocessed table, but could loop in future
  original_table_list <- table_list
  reprocessed_table_name <- sub("^original_", "", sub("_id$", "", names(reprocessed_map)[1]))
  id <- paste0(reprocessed_table_name, "_id")
  tmp <- reprocessed_map %>%
    filter(!!sym(paste0("reprocessed_", id)) %in% table_list[[reprocessed_table_name]][[id]])
  table_list[[reprocessed_table_name]] <- table_list[[reprocessed_table_name]] %>%
    filter(!(!!sym(id) %in% tmp[[paste0("original_", id)]]))
  # remove sets and called variants files that are linked to source file
  reprocessed_set_name <- paste0(reprocessed_table_name, "_set")
  table_list[[reprocessed_set_name]] <- table_list[[reprocessed_set_name]] %>%
    filter(!(!!sym(id) %in% tmp[[paste0("original_", id)]]))
  table_list <- AnvilDataModels:::.filter_set_tables(table_list, original_table_list)
  # remove called variants files that are linked to dropped sets
  variant_table_name <- sub("^aligned", "called_variants", reprocessed_table_name)
  set_id <- paste0(reprocessed_set_name, "_id")
  table_list[[variant_table_name]] <- table_list[[variant_table_name]] %>%
    filter(!!sym(set_id) %in% table_list[[reprocessed_set_name]][[set_id]])
  
  # create experiment and aligned tables
  table_list[["experiment"]] <- experiment_table(table_list)
  table_list[["aligned"]] <- aligned_table(table_list)
  
  # write tsv files to google bucket
  combined_workspace <- paste("AnVIL_GREGoR", release, "PREP", consent, sep="_")
  bucket <- avstorage(namespace=combined_namespace, name=combined_workspace)
  file_list <- write_to_bucket(table_list, bucket)
  
  # write json for validation
  workflow_inputs_json(file_list = file_list, 
                       model_url = model_url, 
                       workspace = combined_workspace, 
                       namespace = combined_namespace,
                       check_vcf = "true")
  avcopy(paste0(combined_workspace, "_validate_gregor_model.json"), bucket)
}

