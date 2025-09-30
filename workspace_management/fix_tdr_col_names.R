library(AnVIL)
library(dplyr)
library(tidyr)
library(readr)

release <- "R03"
consent <- c("GRU", "HMB")
workspaces <- paste("AnVIL_GREGoR", release, consent, sep="_")
namespace <- "anvil-datastorage"

tables_to_fix <- c("genetic_findings", "experiment_rna_short_read", "aligned_rna_short_read", "experiment_pac_bio")
model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- AnvilDataModels::json_to_dm(model_url)

for (i in seq_along(workspaces)) {
  bucket <- file.path(avstorage(namespace=namespace, name=workspaces[i]), "GREGoR_data_model_tables/")
  for (t in tables_to_fix) {
    avcopy(paste0(bucket, t, ".tsv"), paste0(bucket, t, "_pre_col_fix.tsv"))
    avcopy(paste0(bucket, t, ".tsv"), ".")
    dat <- read_tsv(paste0(t, ".tsv"))
    
    model_names <- names(model[[t]])
    dat_names <- names(dat)
    fix_case <- model_names[tolower(model_names) %in% dat_names & !(model_names %in% dat_names)]
    for (n in fix_case) {
      names(dat)[names(dat) == tolower(n)] <- n
    }
    fix_prefix <- model_names[paste0("t_", model_names) %in% dat_names & !(model_names %in% dat_names)]
    for (n in fix_prefix) {
      names(dat)[names(dat) == paste0("t_", n)] <- n
    }
    
    write_tsv(dat, paste0(t, ".tsv"))
    avcopy(paste0(t, ".tsv"), paste0(bucket, t, ".tsv"))
  }
}
