library(AnVIL)
library(dplyr)
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
      warning("table ", table_name, " not present in workspace ", x)
      return(NULL)
    }
  }) %>% bind_rows()
}


# create experiment table
experiment_table <- function(experiment_tables) {
  lapply(names(experiment_tables), function(t) {
    experiment_tables[[t]] %>%
      select(id_in_table = paste0(t, "_id"), analyte_id) %>%
      left_join(analyte) %>%
      select(id_in_table, participant_id) %>%
      mutate(experiment_id = paste(t, id_in_table, sep="."),
             table_name = t)
  }) %>% bind_rows()
}

