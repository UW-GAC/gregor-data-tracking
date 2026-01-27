args <- commandArgs(trailingOnly = TRUE)
#args <- c("13", "UCI")
this_cycle <- args[1]
this_center <- args[2]

library(tools)
library(AnVIL)
library(tidyverse)

ALL_CYCLES <- paste0("U", str_pad(2:this_cycle, 2, "left", "0"))
UPLOAD_CYCLE <- ALL_CYCLES[length(ALL_CYCLES)]

centers <- list(
  GRU=c("BCM", "CNH_I", "UCI", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
WORKSPACES_TO_KEEP <- lapply(ALL_CYCLES, function(cycle) {
  lapply(names(centers), function(consent) 
  paste("anvil-datastorage/AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
)}) %>% unlist() %>% sort()

if (this_center == "UCI") {
  keep <- str_detect(WORKSPACES_TO_KEEP, "UCI") | str_detect(WORKSPACES_TO_KEEP, "CNH_I")
} else {
  keep <- str_detect(WORKSPACES_TO_KEEP, this_center)
}
WORKSPACES_TO_KEEP <- WORKSPACES_TO_KEEP[keep]

# Look up bucket for each workspace.

WORKSPACES <- avworkspaces() %>%
  mutate(workspace=file.path(namespace, name)) %>%
  filter(workspace %in% WORKSPACES_TO_KEEP) %>%
  # Extract upload_cycle
  mutate(
    upload_cycle = str_extract(workspace, "_(U\\d+)", group=1)
  ) %>%
  # Add bucket path using avstorage
  rowwise() %>%
  mutate(
    path = avstorage(namespace=namespace, name=name)
  ) %>%
  select(workspace, namespace, name, upload_cycle, path)
WORKSPACES %>% select(workspace, upload_cycle, path) %>% print()


# Pull out data tables from current workspaces

CURRENT_WORKSPACES <- WORKSPACES %>%
  filter(upload_cycle == UPLOAD_CYCLE)
table_list <- list()
for (i in seq_along(CURRENT_WORKSPACES$workspace)) {
  workspace <- CURRENT_WORKSPACES$workspace[i]
  namespace <- CURRENT_WORKSPACES$namespace[i]
  name <- CURRENT_WORKSPACES$name[i]
  # Create a list for storing the tables.
  table_list[[workspace]] <- list()
  tables <- avtables(namespace=namespace, name=name)
  tmp_list <- list()
  for (table in tables$table) {
    tmp <- avtable(table, namespace=namespace, name=name) %>%
      # Only keep character type columns.
      select(where(is.character)) %>%
      # Add row numbers to have a key for the column.
      mutate(rownum=row_number()) %>%
      # Convert to long format.
      pivot_longer(!rownum, names_to="column", values_to="file") %>%
      # Only keep records where the value is gs.
      filter(str_detect(file, "^gs://")) %>%
      # no need to keep multiple rows with the same file
      select(-rownum) %>%
      distinct()
    tmp_list[[table]] <- tmp
    tmp <- bind_rows(tmp_list, .id="table")
    table_list[[i]] <- tmp
  }
}
table_data <- bind_rows(table_list, .id="referencing_workspace")
table_data %>% group_by(referencing_workspace) %>% count() %>% print()


# Check files in all buckets

file_list <- list()
for (i in seq_along(WORKSPACES$workspace)) {
  bucket <- WORKSPACES$path[i]
  workspace <- WORKSPACES$workspace[i]
  print(workspace)
  
  top_level <- avlist(bucket, recursive=FALSE)
  subdirs <- top_level[str_detect(top_level, "/$")]
  # Don't check the notebooks and submissions directories.
  subdirs <- subdirs[!str_detect(subdirs, "notebooks|submissions")]
  # Using the ** makes the gsutil_ls command a lot faster.
  sub_levels <- lapply(subdirs, function(f) avlist(paste0(f, "**"), recursive=TRUE))
  tmp <- tibble(file=c(top_level, unlist(sub_levels)))
  
  file_list[[workspace]] <- tmp
}

file_data <- bind_rows(file_list, .id="workspace")
file_data %>% group_by(workspace) %>% count() %>% print()


## Check if each file is referenced in a data table in a current workspace


tmp <- file_data %>%
  # Which tables are the files referenced in?
  left_join(table_data, by = "file") %>%
  select(workspace, file, referencing_workspace) %>%
  arrange(workspace, file, referencing_workspace)



x <- tmp %>% 
  group_by(workspace, file) %>%
  summarise(
    n_workspaces=length(unique(na.omit(referencing_workspace)))
  ) %>%
  ungroup() %>%
  mutate(filetype = file_ext(file)) %>%
  filter(filetype != "") %>% # remove directories
  mutate(center=str_match(workspace, "anvil-datastorage/AnVIL_GREGoR_([A-Z_]+?)_U")[,2])

x0 <- x %>%
  filter(n_workspaces == 0) %>%
  select(-n_workspaces) %>%
  mutate(center = ifelse(center == "CNH_I", "UCI", center))

x1 <- x0 %>%
    filter(filetype %in% c("bam", "bai", "cram", "crai"))





## Save a list of bam/cram/index files per RC


combined_bucket <- avstorage(namespace="gregor-dcc", name=paste0("GREGOR_COMBINED_CONSORTIUM_", UPLOAD_CYCLE))
uri <- paste0(combined_bucket, "/", UPLOAD_CYCLE, "_QC")
for (category in unique(x0$center)) {
  outfile <- paste(category, UPLOAD_CYCLE, "files_not_referenced_in_data_tables.tsv", sep="_")
  x1_out <- x1 %>%
    filter(center == !! category) %>%
    arrange(workspace, file, filetype)
  if (nrow(x1_out) > 0) {
    write_tsv(x1_out, outfile)
    avcopy(outfile, file.path(uri, outfile))
    # copy to workspaces so RCs can access
    rc_uri <- avstorage(namespace="anvil-datastorage", 
                       name=paste("AnVIL_GREGoR", category, UPLOAD_CYCLE, "GRU", sep="_"))
    avcopy(outfile, file.path(uri, outfile))
    avcopy(outfile, file.path(rc_uri, "post_upload_qc", outfile))
  }
}




