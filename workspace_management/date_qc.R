library(dplyr)
library(stringr)

check_field <- function(tbl, field, id_col) {
  date_regex1 <- "[[:digit:]]+[[/]][[:digit:]]{2,4}"
  date_regex2 <- "[[:digit:]]{1,2}[[-]][[:digit:]]{2,4}"
  date_regex3 <- "[[:digit:]]{2,4}[[-]][[:digit:]]{1,2}"
  chk <- filter(tbl, str_detect(!!sym(field), date_regex1) | 
                  str_detect(!!sym(field), date_regex2) | 
                  str_detect(!!sym(field), date_regex3)) %>%
    select(all_of(c(id_col, field)))
  if (nrow(chk) > 0) {
    return(chk)
  } else {
    return(NULL)
  }
}

date_qc <- function(table_list) {
  field_list <- list()
  field_list[["participant"]] <- c(
    "prior_testing", 
    "proband_relationship_detail", 
    "sex_detail",
    "ancestry_detail",
    "phenotype_description"
  )
  field_list[["family"]] <- c(
    "consanguinity_detail",
    "pedigree_file_detail",
    "family_history_detail"
  )
  field_list[["phenotype"]] <- c(
    "additional_details"
  )
  field_list[["genetic_findings"]] <- c(
    "notes"
  )
  
  log <- list()
  tables <- names(field_list)
  for (t in tables) {
    fields <- field_list[[t]]
    for (f in fields) {
      if (f %in% colnames(table_list[[t]])) {
        log[[paste(t, f, sep="_")]] <- check_field(table_list[[t]], f, paste0(t, "_id"))
      }
    }
  }
  return(log)
}
