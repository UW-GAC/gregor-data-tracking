library(AnVIL)
library(dplyr)
library(readr)

release <- "R03"
consent_groups <- c("HMB", "GRU")
namespace <- "anvil-datastorage"

subj_list <- list()
for (consent in consent_groups) {
  workspace <- paste("AnVIL_GREGoR", release, consent, sep="_")
  participant <- avtable("participant", namespace=namespace, name=workspace)
  
  subj_list[[consent]] <- participant %>%
    select(participant_id, consent_code, gregor_center)
}
subj <- bind_rows(subj_list)

for (RC in unique(subj$gregor_center)) {
  subj %>%
    filter(gregor_center %in% RC) %>%
    select(participant_id, consent_code) %>%
    write_tsv(paste(RC, release, "participant_consent.txt", sep="_"))
}
