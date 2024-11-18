library(AnVIL)
library(dplyr)
library(readr)

release <- "R02"
# determined order of consent groups from dbGaP page: 
# https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=phs003047.v1.p1
consent_groups <- c("HMB", "GRU")
namespace <- "anvil-datastorage"

tissue_map <- c(
  "UBERON:0000479" = "Unknown", #(tissue) 
  "UBERON:0003714" = "Brain", #(neural tissue) 
  "UBERON:0001836" = "Saliva", #(saliva) 
  "UBERON:0001003" = "Skin", #(skin epidermis) 
  "UBERON:0002385" = "Muscle", #(muscle tissue) 
  "UBERON:0000178" = "Blood", #(whole blood) 
  "UBERON:0002371" = "Bone marrow", #(bone marrow) 
  "UBERON:0006956" = "Buccal", #(buccal mucosa)
  "UBERON:0001359" = "Cerebrospinal fluid", #(cerebrospinal fluid)
  "UBERON:0001088" = "Urine", #(urine)
  "UBERON:0019306" = "Nose epithelium", #(nose epithelium)
  "CL: 0000034" = "Stem cells", #(iPSC)
  "CL: 0000576" = "Blood", #(monocytes - PBMCs)
  "CL: 0000542" = "Blood", #(lymphocytes - LCLs)
  "CL: 0000057" = "Connective tissue", #(fibroblasts)
  "UBERON:0005291" = "Embyro", #(embryonic tissue)
  "CL: 0011020" = "Stem cells", #(iPSC NPC)
  "UBERON:0002037" = "Brain", #(cerebellum tissue)
  "UBERON:0001133" = "Heart" #(cardiac tissue)
)

subj_list <- list()
ssm_list <- list()
attr_list <- list()
for (consent in consent_groups) {
  workspace <- paste("AnVIL_GREGoR", release, consent, sep="_")
  participant <- avtable("participant", namespace=namespace, name=workspace)
  analyte <- avtable("analyte", namespace=namespace, name=workspace)
  
  subj <- participant %>%
    mutate(CONSENT = which(consent_groups == consent)) %>%
    select(SUBJECT_ID = participant_id,
           CONSENT,
           SEX = sex)
  
  ssm <- analyte %>%
    select(SUBJECT_ID = participant_id,
           SAMPLE_ID = analyte_id)
  
  attr <- analyte %>%
    mutate(BODY_SITE = tissue_map[primary_biosample],
           IS_TUMOR = "No") %>%
    select(SAMPLE_ID = analyte_id,
           BODY_SITE,
           ANALYTE_TYPE = analyte_type,
           IS_TUMOR,
           HISTOLOGICAL_TYPE = primary_biosample)
  
  subj_list[[consent]] <- subj
  ssm_list[[consent]] <- ssm
  attr_list[[consent]] <- attr
}

subj <- bind_rows(subj_list)
ssm <- bind_rows(ssm_list)
attr <- bind_rows(attr_list)

subj_dd <- tibble(
  VARNAME = names(subj),
  VARDESC = c("Subject ID", "Consent group", "Biological sex assigned at birth"),
  TYPE = c("string", "encoded value", "string"),
  VALUES = c("", "1=Health/Medical/Biomedical (HMB)", ""),
  ` ` = c("", "2=General Research Use (GRU)", "")
)

ssm_dd <- tibble(
  VARNAME = names(ssm),
  VARDESC = c("Subject ID", "Sample ID"),
  TYPE = rep("string", 2)
)

attr_dd <- tibble(
  VARNAME = names(attr),
  VARDESC = c("Sample ID", "Body site where sample was collected", 
              "Analyte type", "Tumor status", 
              "Cell or tissue type or subtype of sample"),
  TYPE = rep("string", 5)
)

write_tsv(subj, "GREGoR_SubjectConsent_DS.txt")
write_tsv(subj_dd, "GREGoR_SubjectConsent_DD.txt")
write_tsv(ssm, "GREGoR_SubjectSampleMapping_DS.txt")
write_tsv(ssm_dd, "GREGoR_SubjectSampleMapping_DD.txt")
write_tsv(attr, "GREGoR_SampleAttributes_DS.txt")
write_tsv(attr_dd, "GREGoR_SampleAttributes_DD.txt")
