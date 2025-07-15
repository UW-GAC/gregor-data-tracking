library(dplyr)

affected_qc <- function(participant_table) {
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
  
  return(log)
}
