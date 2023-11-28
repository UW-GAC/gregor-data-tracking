library(AnVIL)
library(dplyr)
library(readr)

cycle1 <- "U03"
cycle2 <- "U04"
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
  tables_to_check <- tables$table[!(grepl("_set$", tables$table))]
  table_list <- list()
  for (t in tables_to_check) {
    table1 <- avtable(t, namespace=namespace, name=workspaces1[i])
    table2 <- avtable(t, namespace=namespace, name=workspaces2[i])
    
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
      prob1 <- dat[is.na(dat[[col1]]) != is.na(dat[[col2]]), c(entity_id, col1, col2)]
      prob2 <- dat[!is.na(dat[[col1]]) & !is.na(dat[[col2]]) & dat[[col1]] != dat[[col2]], c(entity_id, col1, col2)]
      dat2 <- rbind(prob1, prob2)
      if (nrow(dat2) > 0) diff_list[[c]] <- dat2
    }
    ids <- unique(unlist(lapply(diff_list, function(x) x[[entity_id]])))
    id_list <- list()
    for (id in ids) {
      id_tbl <- tibble(id)
      names(id_tbl) <- entity_id
      for (x in diff_list) {
        tmp <- x[x[[entity_id]] == id,]
        if (nrow(tmp) > 0) {
          id_tbl <- left_join(id_tbl, tmp, by=entity_id)
        }
      }
      id_list[[id]] <- id_tbl
    }
    
    col_sets <- lapply(id_list, names)
    col_list <- list()
    index <- 1
    for (cols in unique(col_sets)) {
      tmp_list <- list()
      j <- 1
      for (x in id_list) {
        if (setequal(unlist(cols), names(x))) {
          tmp_list[[j]] <- x
          j <- j+1
        }
      }
      col_list[[index]] <- bind_rows(tmp_list)
      index <- index+1
    }
    
    outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_", t, ".txt")
    writeLines(knitr::kable(col_list), outfile)
    gsutil_cp(outfile, paste0(avbucket(), "/", cycle2, "_QC/"))
    #gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
  }
}
