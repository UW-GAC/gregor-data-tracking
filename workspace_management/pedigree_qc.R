library(dplyr)

pedigree_qc <- function(participant_table) {
  # run pedigreeCheck function
  ped <- participant_table %>%
    mutate(sex = c("Male"="M", "Female"="F", "Unknown"="Unknown")[sex]) %>%
    select(
      family = family_id,
      individ = participant_id,
      father = paternal_id,
      mother = maternal_id,
      sex
    ) %>%
    as.data.frame()
  chk <- GWASTools::pedigreeCheck(ped)
  chk$subfamilies.ident <- NULL # don't report on subfamilies
  if ("sexcode.error.rows" %in% names(chk)) {
    chk$sexcode.error.rows <- ped[chk$sexcode.error.rows, c("individ", "sex")]
  }
  if ("mismatch.sex" %in% names(chk)) {
    ids <- chk$mismatch.sex$individ
    chk$mismatch.sex <- ped %>%
      filter(individ %in% ids | mother %in% ids | father %in% ids)
  }
  if ("unknown.parent.rows" %in% names(chk)) {
    chk$unknown.parent.rows <- ped[chk$unknown.parent.rows$row.num, c("family", "individ", "mother", "father")]
  }
  
  # check 1:1 mapping of proband:family
  families <- unique(participant_table$family_id)
  proband_errors <- list()
  for (f in families) {
    fam <- participant_table %>%
      filter(family_id == f) %>%
      select(family_id, participant_id, maternal_id, paternal_id, proband_relationship)
    n_proband <- sum(fam$proband_relationship == "Self")
    if (n_proband != 1) {
      proband_errors[[f]] <- fam
      break
    }
    pb <- filter(fam, proband_relationship == "Self")
    mother <- fam$participant_id[fam$proband_relationship == "Mother"]
    if (length(mother) > 0) {
      if (mother != pb$maternal_id) {
        proband_errors[[f]] <- fam
        break
      }
    }
    father <- fam$participant_id[fam$proband_relationship == "Father"]
    if (length(father) > 0) {
      if (father != pb$paternal_id) {
        proband_errors[[f]] <- fam
        break
      }
    }
  }
  chk <- c("proband_errors"=proband_errors, chk)
  return(chk)
}
