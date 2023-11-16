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

allequal <- function(x, y) {
  if (!all(is.na(x) == is.na(y))) return(FALSE)
  return(all(x[!is.na(x)] == y[!is.na(y)]))
}

for (i in seq_along(workspaces1)) {
  message(workspaces1[i])
  tables <- avtables(namespace=namespace, name=workspaces1[i])
  tables_to_check <- tables$table[!(grepl("_set$", tables$table))]
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
    for (c in setdiff(names(table1), entity_id)) {
      col1 <- paste(c, cycle1, sep=".")
      col2 <- paste(c, cycle2, sep=".")
      if (allequal(dat[[col1]], dat[[col2]])) {
        dat <- dat %>%
          select(-any_of(c(col1, col2)))
      }
    }
    dat <- dat[,c(entity_id, sort(setdiff(names(dat), entity_id)))]
    
    outfile <- paste0(workspaces1[i], "_diff_", cycle2, "_", t, ".tsv")
    write_tsv(dat, file=outfile)
    gsutil_cp(outfile, paste0(avbucket(), "/", cycle2, "_QC/"))
    #gsutil_cp(outfile, paste0(avbucket(namespace=namespace, name=workspaces2[i]), "/post_upload_qc/"))
  }
}
