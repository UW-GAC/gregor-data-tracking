library(AnVIL)
library(dplyr)
library(readr)

cycle1 <- "U04"
cycle2 <- "U05"
centers <- list(
  GRU=c("BCM", "CNH_I", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces1 <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle1, consent, sep="_")
) %>% unlist() %>% sort()
workspaces2 <- sub(cycle1, cycle2, workspaces1)
namespace <- "anvil-datastorage"

for (i in seq_along(workspaces1)) {
  message(workspaces1[i])
  tables <- avtables(namespace=namespace, name=workspaces1[i])
  tables_to_check <- setdiff(tables$table[!(grepl("_set$", tables$table))], "genetic_findings")
  summary_list <- list()
  for (t in tables_to_check) {
    table1 <- avtable(t, namespace=namespace, name=workspaces1[i])
    table2 <- avtable(t, namespace=namespace, name=workspaces2[i])
    fixme <- "chrom"
    if (fixme %in% names(table1)) table1[[fixme]] <- as.character(table1[[fixme]])
    
    entity_id <- paste0(t, "_id")
    dat <- semi_join(table2, table1, by=entity_id) # rows in table2 also in table1
    if (nrow(dat) == 0) next
    dat <- dat %>%
      anti_join(table1) %>% # rows that are different between table 2 and table 1
      left_join(table1, by=entity_id, suffix=paste0(".", c(cycle2, cycle1)))
    if (nrow(dat) == 0) next
    message(t)
    diff_list <- list()
    for (c in setdiff(names(table1), entity_id)) {
      col1 <- paste(c, cycle1, sep=".")
      col2 <- paste(c, cycle2, sep=".")
      # problem if data was previously supplied and now missing, not a problem if new data was added
      prob1 <- dat[!is.na(dat[[col1]]) & is.na(dat[[col2]]), c(entity_id, col1, col2)]
      prob2 <- dat[!is.na(dat[[col1]]) & !is.na(dat[[col2]]) & dat[[col1]] != dat[[col2]], c(entity_id, col1, col2)]
      dat2 <- rbind(prob1, prob2)
      if (nrow(dat2) > 0) diff_list[[c]] <- dat2
    }
    if (length(diff_list) == 0) next
    
    # summarize number of differences for each column
    diff_sum <- sapply(diff_list, nrow)
    summary_list[[t]] <- tibble(column=names(diff_sum), n_differences=diff_sum) %>%
      arrange(desc(n_differences))
    names(summary_list[[t]])[1] <- paste0(t, "_column")
    
    # concatenate diff sets with exactly the same ids
    id_list <- lapply(diff_list, function(x) x[[entity_id]])
    combined_diff_list <- list()
    index <- 1
    for (ids in unique(id_list)) {
        id_tbl <- tibble(id=ids)
        names(id_tbl) <- entity_id
        for (x in diff_list) {
          if (setequal(x[[entity_id]], ids)) {
            id_tbl <- left_join(id_tbl, x, by=entity_id)
          }
        }
        combined_diff_list[[index]] <- id_tbl
        index <- index + 1
    }
    
    # print differences for each table
    outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_", t, ".txt")
    writeLines(knitr::kable(combined_diff_list), outfile)
    gsutil_cp(outfile, paste0(avbucket(), "/", cycle2, "_QC/"))
    gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
  }
  
  # print summary
  outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_summary.txt")
  writeLines(knitr::kable(summary_list), outfile)
  gsutil_cp(outfile, paste0(avbucket(), "/", cycle2, "_QC/"))
  gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
}
