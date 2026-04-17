library(AnVIL)
#remotes::install_github("UW-GAC/AnvilDataModels")
library(AnvilDataModels)
library(dplyr)
library(readr)


remove_participants <- function(participant_ids, table_list, model) {
  original_table_list <- table_list
  table_list[["genetic_findings"]] <- NULL # dm can't handle cyclical relationships
  opt_map_tables <- names(table_list)[grepl("optical_mapping", names(table_list))]
  for (t in opt_map_tables) table_list[[t]] <- NULL # more cyclical relationships
  for (p in participant_ids) {
    table_list <- delete_rows(p, "participant", tables=table_list, model=model)
  }
  if ("genetic_findings" %in% names(original_table_list)) {
    new_findings <- original_table_list[["genetic_findings"]]
    for (p in participant_ids) {
      new_findings <- new_findings %>%
        filter(!(participant_id %in% p)) %>%
        filter(!grepl(p, additional_family_members_with_variant))
    }
    table_list[["genetic_findings"]] <- new_findings
  }
  if (length(opt_map_tables) > 0) {
    opt_map_participants <- original_table_list[["experiment_optical_mapping"]] %>%
      inner_join(original_table_list[["analyte"]], by="analyte_id") %>%
      select(participant_id) %>%
      unlist()
    # logic to drop participants from optical mapping table is complex, for now just check if we need to do it
    stopifnot(all(opt_map_participants %in% table_list[["participant"]][["participant_id"]]))
    for (t in opt_map_tables) table_list[[t]] <- original_table_list[[t]]
  }
  
  # restore samples with no participant ids
  if ("experiment_rna_short_read" %in% names(original_table_list)) {
    orig_exprna <- original_table_list[["experiment_rna_short_read"]]
    if ("rna_sample_type" %in% names(orig_exprna)) {
      isogenic <- orig_exprna %>%
        filter(rna_sample_type == "isogenic_cell_line")
      new_exprna <- table_list[["experiment_rna_short_read"]] %>%
        bind_rows(isogenic)
      table_list[["experiment_rna_short_read"]] <- new_exprna
    }
  }
  
  return(table_list)
}


remove_participants_workspace <- function(participant_ids, workspace, namespace, model_url, dry_run=TRUE) {
  model <- json_to_dm(model_url)
  
  tables <- avtables(namespace=namespace, name=workspace)
  table_list <- list()
  for (t in tables$table) {
    message("reading table ", t)
    dat <- avtable(t, namespace=namespace, name=workspace)
    if (grepl("_set$", t)) {
      dat <- unnest_set_table(dat)
    }
    table_list[[t]] <- dat
  }
  
  original_table_list <- table_list
  table_list <- remove_participants(participant_ids, table_list, model)
  
  drop_participants_log <- list()
  # need to drop sets first
  set_tables <- names(table_list)[grepl("_set$", names(table_list))]
  table_names <- c(set_tables, setdiff(names(table_list), set_tables))
  for (t in table_names) {
    id_col <- paste0(t, "_id")
    ids_to_delete <- setdiff(original_table_list[[t]][[id_col]], table_list[[t]][[id_col]])
    if (!dry_run & length(ids_to_delete) > 0) {
      message("deleting values from table ", t)
      avtable_delete_values(t, ids_to_delete, namespace=namespace, name=workspace)
    }
    drop_participants_log[[t]] <- ids_to_delete
  }
  
  pretty_log <- lapply(names(drop_participants_log), function(t) {
    tibble(!!paste0(t, "_id") := drop_participants_log[[t]])
  })
  report_file <- paste0(workspace, "_dropped_participants.txt")
  writeLines(knitr::kable(pretty_log), report_file)
  if (!dry_run) {
    log_dir <- file.path(avstorage(namespace=namespace, name=workspace)) 
    avcopy(report_file, file.path(log_dir, report_file))
  }
}
