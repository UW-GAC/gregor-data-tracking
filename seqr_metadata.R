# define data source
NAMESPACE <- "gregor-dcc"
WORKSPACE <- "GREGOR_COMBINED_CONSORTIUM_U03"
VCF_MANIFEST <- "gs://fc-secure-0392c60d-b346-4e60-b5f1-602a9bbfb0e1/gregor_joint_callset_022824/sample_manifest_v2.tsv"
OUTFILE <- "gregor_consortium_dataset_u03_seqr_metadata.tsv"
BUCKET_DEST <- "gs://fc-secure-0392c60d-b346-4e60-b5f1-602a9bbfb0e1/gregor_joint_callset_022824/"

metadata_columns <- c(
  "Family ID", 
  "Individual ID",
  "HPO Terms (present)", # comma-separated list
  "HPO Terms (absent)", # comma-separated list
  "Birth Year",
  "Death Year",
  "Age of Onset", # allowed values: Congenital/Embryonal/Fetal/Neonatal/Infantile/Childhood/Juvenile/Adult/Young adult/Middle age/Late onset
  "Individual Notes",
  "Consanguinity", # true, false, or blank
  "Other Affected Relatives", # true, false, or blank
  "Expected Mode of Inheritance",
  "Fertility medications",
  "Intrauterine insemination",
  "In vitro fertilization",
  "Intra-cytoplasmic sperm injection",
  "Gestational surrogacy",
  "Donor egg",
  "Donor sperm",
  "Maternal Ancestry",
  "Paternal Ancestry",
  "Pre-discovery OMIM disorders",
  "Previously Tested Genes",
  "Candidate Genes"
)



library(AnVIL)
library(dplyr)
library(tidyr)
library(readr)

participant <- avtable("participant", namespace=NAMESPACE, name=WORKSPACE)
family <- avtable("family", namespace=NAMESPACE, name=WORKSPACE)
phenotype <- avtable("phenotype", namespace=NAMESPACE, name=WORKSPACE)

HPO_present <- phenotype %>%
  filter(ontology %in% "HPO", presence %in% "Present") %>%
  select(participant_id, term_id) %>%
  group_by(participant_id) %>%
  summarise(HPO_terms_present = paste(term_id, collapse=","))

HPO_absent <- phenotype %>%
  filter(ontology %in% "HPO", presence %in% "Absent") %>%
  select(participant_id, term_id) %>%
  group_by(participant_id) %>%
  summarise(HPO_terms_absent = paste(term_id, collapse=","))

OMIM <- phenotype %>%
  filter(ontology %in% "OMIM") %>%
  select(participant_id, term_id) %>%
  group_by(participant_id) %>%
  summarise(omim_disorders = paste(term_id, collapse=","))

consanguinity_map <- c(
  "None suspected" = "false",
  "Suspected" = "true",
  "Present" = "true",
  "Unknown" = ""
)

consang <- participant %>%
  select(participant_id, family_id) %>%
  left_join(select(family, family_id, consanguinity)) %>%
  mutate(notes = ifelse(consanguinity %in% "Suspected", "Consanguinity suspected", NA),
         consanguinity = consanguinity_map[consanguinity])

affected_relatives <- participant %>%
  filter(affected_status %in% c("Affected", "Possibly affected")) %>%
  group_by(family_id) %>%
  summarise(affected = paste(participant_id, collapse=","))

has_affected_rel <- participant %>%
  select(participant_id, family_id) %>%
  inner_join(affected_relatives, by="family_id") %>%
  filter(!(participant_id == affected)) %>%
  mutate(affected_relatives = "true") %>%
  select(participant_id, affected_relatives)


seqr <- participant %>%
  select(participant_id, family_id) %>%
  left_join(HPO_present) %>%
  left_join(HPO_absent) %>%
  left_join(consang) %>%
  left_join(has_affected_rel) %>%
  left_join(OMIM) %>%
  rename(`Family ID` = family_id,
         `Individual ID` = participant_id,
         `HPO Terms (present)` = HPO_terms_present,
         `HPO Terms (absent)` = HPO_terms_absent,
         `Consanguinity` = consanguinity,
         `Individual Notes` = notes,
         `Other Affected Relatives` = affected_relatives,
         `Pre-discovery OMIM disorders` = omim_disorders)

stopifnot(all(names(seqr) %in% metadata_columns))
stopifnot(nrow(seqr) == nrow(participant))

# subset to only participants present in joint callset

gsutil_cp(VCF_MANIFEST, ".")
vcf_participants <- read_tsv(basename(VCF_MANIFEST))

seqr <- seqr %>%
  filter(`Individual ID` %in% vcf_participants$participant_id)

write_tsv(seqr, OUTFILE)
gsutil_cp(OUTFILE, BUCKET_DEST)


### don't use this; too many duplicate values

onset_map <- c(
  "HP:0003581" = "Adult onset",
  "HP:0030674" = "Antenatal onset",
  "HP:0011463" = "Childhood onset",
  "HP:0003577" = "Congenital onset",
  "HP:0025708" = "Early young adult onset",
  "HP:0011460" = "Embryonal onset",
  "HP:0011461" = "Fetal onset",
  "HP:0003593" = "Infantile onset",
  "HP:0025709" = "Intermediate young adult onset",
  "HP:0003621" = "Juvenile onset",
  "HP:0034199" = "Late first trimester onset",
  "HP:0003584" = "Late onset",
  "HP:0025710" = "Late young adult onset",
  "HP:0003596" = "Middle age onset",
  "HP:0003623" = "Neonatal onset",
  "HP:0410280" = "Pediatric onset",
  "HP:4000040" = "Puerpural onset",
  "HP:0034198" = "Second trimester onset",
  "HP:0034197" = "Third trimester onset",
  "HP:0011462" = "Young adult onset"
)

onset_HPO_to_seqr <- c(
  "Antenatal onset" = "Fetal onset",
  "Late first trimester onset" = "Embryonal onset",
  "Second trimester onset" = "Fetal onset",
  "Third trimester onset" = "Fetal onset",
  "Puerpural onset" = "Fetal onset",
  "Pediatric onset" = "Childhood onset",
  "Early young adult onset" = "Young adult onset",
  "Intermediate young adult onset" = "Young adult onset",
  "Late young adult onset" = "Young adult onset"
)

age <- phenotype %>%
  filter(!is.na(onset_age_range)) %>%
  mutate(age_of_onset=onset_map[onset_age_range]) %>%
  mutate(age_of_onset=ifelse(age_of_onset %in% names(onset_HPO_to_seqr), 
                             onset_HPO_to_seqr[age_of_onset],
                             age_of_onset)) %>%
  distinct(participant_id, age_of_onset)
           
nrow(age)
nrow(distinct(age, participant_id))
length(unique(age$participant_id))
length(unique(age$participant_id[duplicated(age$participant_id)]))
