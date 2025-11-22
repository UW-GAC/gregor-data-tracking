library(dplyr)

affected_qc <- function(participant_table, phenotype_table) {
  log <- list()
  chk <- participant_table %>%
    filter(solve_status == "Unaffected", !(affected_status %in% c("Unaffected", "Unknown"))) %>%
    select(participant_id, family_id, proband_relationship, affected_status, solve_status) %>%
    arrange(family_id)
  if (nrow(chk) > 0) log[["unaffected discrepancies"]] <- chk
  
  chk <- participant_table %>%
    filter(proband_relationship == "Self", solve_status == "Unaffected" | affected_status == "Unaffected") %>%
    select(participant_id, family_id, proband_relationship, affected_status, solve_status) %>%
    arrange(family_id)
  if (nrow(chk) > 0) log[["proband discrepancies"]] <- chk
  
  chk <- participant_table %>%
    filter(affected_status == "Affected") %>%
    filter(!(participant_id %in% phenotype_table$participant_id)) %>%
    select(participant_id,family_id, proband_relationship, affected_status) %>%
    mutate(phenotype = "missing")
  if (nrow(chk) > 0) log[["missing phenotype"]] <- chk
  
  return(log)
}
