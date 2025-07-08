library(dplyr)

compare_tables <- function(table1_list, table2_list, table1_prefix, table2_prefix) {
  tables_to_check <- intersect(names(table1_list), names(table2_list))
  summary_list <- list()
  table_diff_list <- list()
  for (t in tables_to_check) {
    table1 <- table1_list[[t]]
    table2 <- table2_list[[t]]
    fixme <- c("chrom", "instrument_ics_version")
    for (f in fixme) {
      if (f %in% names(table1)) table1[[f]] <- as.character(table1[[f]])
    }
    
    entity_id <- paste0(t, "_id")
    dat <- semi_join(table2, table1, by=entity_id) # rows in table2 also in table1
    if (nrow(dat) == 0) next
    dat <- dat %>%
      anti_join(table1) %>% # rows that are different between table 2 and table 1
      left_join(table1, by=entity_id, suffix=paste0(".", c(table2_prefix, table1_prefix)))
    if (nrow(dat) == 0) next
    message(t)
    
    diff_list <- list()
    for (c in setdiff(names(table1), entity_id)) {
      col1 <- paste(c, table1_prefix, sep=".")
      col2 <- paste(c, table2_prefix, sep=".")
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
    table_diff_list[[t]] <- combined_diff_list
  }
  
  return(list(
    summary_list=summary_list,  
    table_diff_list=table_diff_list
    ))
}
