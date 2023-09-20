library(AnVIL)
library(dplyr)
library(readr)
library(lubridate)

conversion_function <- function(cm) {
  if (is.character(cm)) {
    return(as.character)
  } else if (is.logical(cm)) {
    return(as.logical)
  } else if (is.integer(cm)) {
    return(as.integer)
  } else if (is.numeric(cm)) {
    return(as.numeric)
  } else if (is.Date(cm)) {
    return(ymd)
  } else if (is.timepoint(cm)) {
    return(ymd_hms)
  } else {
    return(as.character)
  }
}

combine_tables <- function(table_name, model, workspaces, namespace) {
  #print(paste("table", table_name))
  lapply(workspaces, function(x) {
    #print(paste("workspace", x))
    tables <- avtables(namespace=namespace, name=x)
    if (table_name %in% tables$table) {
      table <- avtable(table_name, namespace=namespace, name=x)
      # map columns to expected data types so they can be combined
      if (!grepl("_set$", t)) {
        for (c in names(table)) {
          #print(paste("column", c))
          FUN <- conversion_function(model[[table_name]][[c]])
          table[[c]] <- FUN(table[[c]])
        }
      }
      return(table)
    } else {
      #warning("table ", table_name, " not present in workspace ", x)
      return(NULL)
    }
  }) %>% bind_rows()
}


# create experiment table
experiment_table <- function(table_list) {
  experiment_tables <- names(table_list)[grepl("^experiment", names(table_list))]
  lapply(experiment_tables, function(t) {
    table_list[[t]] %>%
      select(id_in_table = paste0(t, "_id"), analyte_id) %>%
      left_join(table_list[["analyte"]]) %>%
      select(id_in_table, participant_id) %>%
      mutate(experiment_id = paste(t, id_in_table, sep="."),
             table_name = t)
  }) %>% bind_rows()
}


# create aligned table
aligned_table <- function(table_list) {
  aligned_tables <- setdiff(
    names(table_list)[grepl("^aligned", names(table_list))],
    names(table_list)[grepl("_set$", names(table_list))]
  )
  lapply(aligned_tables, function(t) {
    experiment_table <- sub("^aligned", "experiment", t)
    experiment_id <- paste0(experiment_table, "_id")
    table_list[[t]] %>%
      select(id_in_table = paste0(t, "_id"), !!experiment_id) %>%
      left_join(table_list[[experiment_table]]) %>%
      select(id_in_table, !!experiment_id, analyte_id) %>%
      left_join(table_list[["analyte"]]) %>%
      select(id_in_table, participant_id) %>%
      mutate(aligned_id = paste(t, id_in_table, sep="."),
             table_name = t)
  }) %>% bind_rows()
}


# write tables to tsv
write_to_bucket <- function(table_list, bucket) {
  file_list <- list()
  for (t in names(table_list)) {
    tmpfile <- tempfile()
    write_tsv(table_list[[t]], tmpfile)
    outfile <- paste0(bucket, "/data_tables/", t, ".tsv")
    gsutil_cp(tmpfile, outfile)
    unlink(tmpfile)
    file_list[[t]] <- outfile
  }
  return(file_list)
}
