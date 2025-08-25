library(dplyr)
library(jsonlite)

workflow_inputs_json <- function(file_list, model_url, 
                          workspace, namespace,
                          import_tables = "true",
                          overwrite = "false",
                          check_md5 = "false",
                          check_vcf = "false",
                          check_bam = "false",
                          check_bucket_paths = "false",
                          check_phenotype_terms = "false"
                          ) {
  
  json <- list("validate_gregor_model.table_files" = file_list,
               "validate_gregor_model.model_url" = model_url,
               "validate_gregor_model.workspace_name" = workspace,
               "validate_gregor_model.workspace_namespace" = namespace,
               "validate_gregor_model.import_tables" = import_tables,
               "validate_gregor_model.overwrite" = overwrite,
               "validate_gregor_model.check_md5" = check_md5,
               "validate_gregor_model.check_vcf" = check_vcf,
               "validate_gregor_model.check_bam" = check_bam,
               "validate_gregor_model.check_bucket_paths" = check_bucket_paths,
               "validate_gregor_model.check_phenotype_terms" = check_phenotype_terms
  ) %>% toJSON(pretty=TRUE, auto_unbox=TRUE, unbox=TRUE)
  write(json, paste0(workspace, "_validate_gregor_model.json"))
}
