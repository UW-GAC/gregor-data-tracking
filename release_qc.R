library(dplyr)

release_qc <- function(table_list) {
  types <- sub("experiment_", "", 
               names(table_list)[grepl("^experiment_", names(table_list))])
  
  # only experiments that have an aligned_read
  for (t in types) {
    experiment <- table_list[[paste0("experiment_", t)]]
    aligned_read <- table_list[[paste0("aligned_", t)]]
    experiment_id_col <- paste0("experiment_", t, "_id")
    table_list[[paste0("experiment_", t)]] <- experiment %>%
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
