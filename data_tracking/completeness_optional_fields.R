library(AnVIL)
library(AnvilDataModels)
library(tidyverse)
library(jsonlite)

#workspace <- "GREGOR_COMBINED_CONSORTIUM_U10"
#namespace <- "gregor-dcc"
#tables <- avtables(name=workspace, namespace=namespace)
#table_names <- tables$table
#table_list <- lapply(table_names, avtable, name=workspace, namespace=namespace)

cycle <- "U10"
centers <- list(
  GRU=c("BCM", "UCI", "GSS", "BROAD", "UW_CRDR"),
  HMB=c("BROAD", "UW_CRDR")
)
workspaces <- lapply(names(centers), function(consent) 
  paste("AnVIL_GREGoR", centers[[consent]], cycle, consent, sep="_")
) %>% unlist() %>% sort()
namespace <- "anvil-datastorage"

model_url <- "https://raw.githubusercontent.com/UW-GAC/gregor_data_models/main/GREGoR_data_model.json"
model <- json_to_dm(model_url)

table_names <- setdiff(names(model), c("experiment", "aligned"))
table_names <- table_names[!str_detect(table_names, "_set$")]


source("../workspace_management/combine_tables.R")
combine_tables <- function(table_name, model, workspaces, namespace) {
  print(paste("table", table_name))
  lapply(workspaces, function(x) {
    print(paste("workspace", x))
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
      center <- x %>%
        str_replace("^AnVIL_GREGoR_", "") %>%
        str_replace("_U[[:digit:]]{2}_[[:alpha:]]{3}$", "")
      table <- table %>%
        mutate(gregor_center=center)
      return(table)
    } else {
      return(NULL)
    }
  }) %>% bind_rows()
}


table_list <- list()
for (t in table_names) {
  dat <- combine_tables(t, model, workspaces=workspaces, namespace=namespace)
  if (nrow(dat) > 0) {
    table_list[[t]] <- dat
  }
}
saveRDS(table_list, "combined_U10_tables.rds")



table_list <- readRDS("combined_U10_tables.rds")


model_json <- fromJSON(model_url)
tbls <- model_json$tables
field_descriptions <- lapply(1:nrow(tbls), function(i) {
  tibble (
    table = tbls$table[i],
    field = tbls$columns[[i]]$column,
    description = tbls$columns[[i]]$description
  )
}) %>% bind_rows()


completeness <- list()
for (t in names(table_list)) {
  req <- attr(model[[t]], "required")
  opt <- setdiff(names(model[[t]]), req)
  if (length(opt) == 0) {
    completeness[[t]] <- NULL
  } else {
    n <- nrow(table_list[[t]])
    completeness[[t]] <- table_list[[t]] %>%
      summarise(across(any_of(opt), ~sum(!is.na(.x))/n)) %>%
      pivot_longer(everything(), names_to="field", values_to="completeness") %>%
      mutate(table=t) %>%
      relocate(table) %>%
      left_join(field_descriptions) %>%
      relocate(description, .after=field)
  }
}
completeness <- bind_rows(completeness)
#outfile <- paste("completeness_optional_fields", cycle, "total.tsv", sep="_")
#write_tsv(completeness, outfile)
#gsutil_cp(outfile, file.path(avbucket(), paste0(cycle, "_QC"), outfile))
completeness_total <- completeness %>%
  mutate(gregor_center = "ALL")


completeness_centers <- list()
for (center in unique(unlist(centers, use.names=FALSE))) {
  completeness <- list()
  for (t in names(table_list)) {
    req <- attr(model[[t]], "required")
    opt <- setdiff(names(model[[t]]), req)
    if (length(opt) == 0) {
      completeness[[t]] <- NULL
    } else {
      tmp <- table_list[[t]] %>%
        filter(gregor_center == center)
      n <- nrow(tmp)
      if (nrow(tmp) == 0) {
        completeness[[t]] <- NULL
      } else {
        completeness[[t]] <- tmp %>%
          summarise(across(any_of(opt), ~sum(!is.na(.x))/n)) %>%
          pivot_longer(everything(), names_to="field", values_to="completeness") %>%
          mutate(table=t) %>%
          relocate(table) %>%
          left_join(field_descriptions) %>%
          relocate(description, .after=field) %>%
          mutate(gregor_center = center)
      }
    }
  }
  completeness <- bind_rows(completeness)
  #outfile <- paste("completeness_optional_fields", cycle, center, "center.tsv", sep="_")
  #write_tsv(completeness, outfile)
  #gsutil_cp(outfile, file.path(avbucket(), paste0(cycle, "_QC"), outfile))
  completeness_centers[[center]] <- completeness
}


comp_final <- bind_rows(completeness_centers) %>%
  bind_rows(completeness_total) %>%
  pivot_wider(names_from = gregor_center, values_from = completeness)
outfile <- paste("completeness_optional_fields", cycle, "all.tsv", sep="_")
write_tsv(comp_final, outfile)
gsutil_cp(outfile, file.path(avbucket(), paste0(cycle, "_QC"), outfile))
