library(dplyr)
library(readr)

# remove rows from a set of tables for release
release_qc <- function(table_list) {
  types <- sub("experiment_", "", 
               names(table_list)[grepl("^experiment_", names(table_list))])
  
  # only experiments that have an aligned_read
  for (t in types) {
    experiment_table <- paste0("experiment_", t)
    experiment <- table_list[[experiment_table]]
    aligned_read <- table_list[[paste0("aligned_", t)]]
    experiment_id_col <- paste0("experiment_", t, "_id")
    table_list[[experiment_table]] <- experiment %>%
      filter(.data[[experiment_id_col]] %in% aligned_read[[experiment_id_col]])
  }
  
  # only analytes that have an experiment (either DNA or RNA)
  analytes_ref <- lapply(types, function(t) {
    table_list[[paste0("experiment_", t)]][["analyte_id"]]
  }) %>%
    unlist() %>%
    unique()
  table_list[["analyte"]] <- table_list[["analyte"]] %>%
    filter(analyte_id %in% analytes_ref)
  
  # only participants who either have an analyte or are related to someone with an analyte
  participants_ref <- table_list[["analyte"]][["participant_id"]]
  families_ref <- table_list[["participant"]] %>%
    filter(participant_id %in% participants_ref) %>%
    select(family_id) %>%
    unlist()
  table_list[["participant"]] <- table_list[["participant"]] %>%
    filter(participant_id %in% participants_ref | family_id %in% families_ref)
  
  # only families with participants
  table_list[["family"]] <- table_list[["family"]] %>%
    filter(family_id %in% table_list[["participant"]][["family_id"]])
  
  # only phenotypes with participants
  table_list[["phenotype"]] <- table_list[["phenotype"]] %>%
    filter(participant_id %in% table_list[["participant"]][["participant_id"]])
  
  return(table_list)
}


# compare a set of release tables with original tables and return tables
# with rows that were removed
release_qc_log <- function(table_list, release_table_list) {
  log <- list()
  
  types <- sub("experiment_", "", 
               names(table_list)[grepl("^experiment_", names(table_list))])
  for (t in types) {
    experiment_table <- paste0("experiment_", t)
    experiment_id_col <- paste0("experiment_", t, "_id")
     rem <- table_list[[experiment_table]] %>%
      filter(!(.data[[experiment_id_col]] %in% release_table_list[[experiment_table]][[experiment_id_col]])) %>%
      select(!!sym(experiment_id_col), analyte_id) %>%
      left_join(select(table_list[["analyte"]], analyte_id, participant_id)) %>%
      left_join(select(table_list[["participant"]], participant_id, family_id)) %>%
      mutate(note="experiment with missing aligned_read")
    if (nrow(rem) > 0) log[[experiment_table]] <- rem
  }
  
  rem <- table_list[["analyte"]] %>%
    filter(!(.data[["analyte_id"]] %in% release_table_list[["analyte"]][["analyte_id"]])) %>%
    select(analyte_id, participant_id) %>%
    left_join(select(table_list[["participant"]], participant_id, family_id)) %>%
    mutate(note="analyte with missing experiment")
  if (nrow(rem) > 0) log[["analyte"]] <- rem
  
  rem <- table_list[["participant"]] %>%
    filter(!(.data[["participant_id"]] %in% release_table_list[["participant"]][["participant_id"]])) %>%
    select(participant_id, family_id, consent_code, gregor_center) %>%
    mutate(note="participant with missing analyte")
  if (nrow(rem) > 0) log[["participant"]] <- rem
  
  rem <- table_list[["family"]] %>%
    filter(!(.data[["family_id"]] %in% release_table_list[["family"]][["family_id"]])) %>%
    select(family_id) %>%
    mutate(note="family with missing participant")
  if (nrow(rem) > 0) log[["family"]] <- rem
  
  rem <- table_list[["phenotype"]] %>%
    filter(!(.data[["phenotype_id"]] %in% release_table_list[["phenotype"]][["phenotype_id"]])) %>%
    select(phenotype_id, participant_id, term_id, presence) %>%
    mutate(note="phenotype with missing participant")
  if (nrow(rem) > 0) log[["phenotype"]] <- rem
  
  return(log)
}
