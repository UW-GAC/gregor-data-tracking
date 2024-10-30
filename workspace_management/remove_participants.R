library(AnVIL)
#remotes::install_github("UW-GAC/AnvilDataModels", ref="edit_tables")
library(AnvilDataModels)
library(dplyr)
library(readr)

remove_participants <- function(participant_ids, workspace, namespace, model_url, dry_run=TRUE) {
  model <- json_to_dm(model_url)
  
  tables <- avtables(namespace=namespace, name=workspace)
  table_list <- list()
  for (t in tables$table) {
    message(t)
    dat <- avtable(t, namespace=namespace, name=workspace)
    if (grepl("_set$", t)) {
      dat <- unnest_set_table(dat)
    }
    table_list[[t]] <- dat
  }
  
  original_table_list <- table_list
  table_list[["genetic_findings"]] <- NULL # dm can't handle cyclical relationships
  for (p in participant_ids) {
    table_list <- delete_rows(p, "participant", tables=table_list, model=model)
  }
  new_findings <- original_table_list[["genetic_findings"]]
  for (p in participant_ids) {
    new_findings <- new_findings %>%
      filter(!(participant_id %in% p)) %>%
      filter(!grepl(p, additional_family_members_with_variant))
  }
  table_list[["genetic_findings"]] <- new_findings
  
  drop_participants_log <- list()
  for (t in names(table_list)) {
    message(t)
    id_col <- paste0(t, "_id")
    ids_to_delete <- setdiff(original_table_list[[t]][[id_col]], table_list[[t]][[id_col]])
    if (!dry_run) avtable_delete_values(t, ids_to_delete, namespace=namespace, name=workspace)
    drop_participants_log[[t]] <- ids_to_delete
  }
  
  pretty_log <- lapply(names(drop_participants_log), function(t) {
    tibble(!!paste0(t, "_id") := drop_participants_log[[t]])
  })
  report_file <- paste0(workspace, "_dropped_participants.txt")
  writeLines(knitr::kable(pretty_log), report_file)
  log_dir <- file.path(avbucket(namespace=namespace, name=workspace)) 
  gsutil_cp(report_file, file.path(log_dir, report_file))
}
