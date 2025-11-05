# Check the monthly data of onshore sampling and generate the files to dump it
# in SIRENO database.
#
# The script import the files obtained from the subcontracted company, check
# the data finding errors and fix them when is possible. Then, it generates the
# files to dump in SIRENO database.
#
# INSTRUCTIONS -----------------------------------------------------------------
# The default folder organization is:
#   - data
#     - YYYY  (year with four digits)
#       - YYYY_MM  (MM is the month with two digits: 01, 02, ..., 12)
#          - originals (folder with the files obtained from the subcontracted
#                       company)
#          - backup (folder with the backup of the scripts, files used in the
#                     process and final files)
#          - errors (folder with the errors found in the data)
#          - finals (folder with the final files to dump in SIRENO database)
#
# The script will generate a excel file ready to dump in SIRENO database. It
# will be stored in the folder ./data/YYYY/YYYY_MM/finals/.
#
# Before to run the script, change variables in "YOU HAVE ONLY TO CHANGE THIS
# VARIABLES" section.


# PACKAGES ---------------------------------------------------------------------

library(dplyr)
library(blastula) # to send emails
library(tools) # file_ext() and file_path_sans_ext
# library(devtools) # Need this package to use install and install_github
source("barbarize.R")

# ---- install sapmuebase from local
# remove.packages("sapmuebase")
# .rs.restartR()
# install("C:/Users/ieoma/Desktop/sap/sapmuebase")
# ---- install sapmuebase from github
# remove.packages("sapmuebase")
# .rs.restartR()
# install_github("Eucrow/sapmuebase")

library(sapmuebase) # and load the library


# ---- install openxlsx
# install.packages("openxlsx")
library(openxlsx)

# ---- install archive package
# install.packages("archive")
library(archive)


# FUNCTIONS --------------------------------------------------------------------
# All the functions required in this script are located in
# revision_volcado_functions.R file.
source("rim_pre_dump_functions.R")
source("R/rim_pre_dump_functions_final.R")

# YOU HAVE ONLY TO CHANGE THIS VARIABLES: --------------------------------------

MONTH <- 12

MONTH_AS_CHARACTER <- sprintf("%02d", MONTH)

YEAR <- "2025"

# Suffix to add to path. Use only in case MONTH is a vector of months. This
# suffix will be added to the end of the path with a "_" as separation.
suffix_multiple_months <- ""

# Suffix to add at the end of the export file name. This suffix will be added to
# the end of the file name with a "_" as separation.
suffix <- "TEST"

# Path where the ".rar" file stored and you need to move to original folder
# STORED_FILE_PATH <- "D:/cost_santander_global/b_trabajo/c_muestreos/d_predump/rim_pre_dump-master_2"

# Define work file name

FILENAME <- "muestreos_8_ICES_A CORUÑA Y LLANES.rar"

# ------------------------------------------------------------------------------

# Identifier for the directory where the working files are in
IDENTIFIER <- createIdentifier(MONTH, 
                               YEAR, 
                               MONTH_AS_CHARACTER, 
                               suffix_multiple_months, 
                               suffix)

BASE_PATH <- file.path(getwd(), 
                       "data", 
                       YEAR)

# Create the base work directory (YYYY_MM - YEAR_MONTH)

YEAR_BASE_PATH <- file.path(BASE_PATH, 
                            IDENTIFIER)

ifelse(dir.exists(YEAR_BASE_PATH), 
       message(paste("Directory", 
                     IDENTIFIER, 
                     "already exists")), 
       dir.create(YEAR_BASE_PATH))


# Name of the folder that is stored the items to send error mail
PRIVATE_FOLDER_NAME <- "private"

# USER SETTINGS ----------------------------------------------------------------
# This file contains the user settings:
# - Share folder's path
source(file.path(PRIVATE_FOLDER_NAME, 
                 ("user_settings.R")))


# VARIABLES --------------------------------------------------------------------
ERRORS <- list() # list with all errors found in data frames
# MESSAGE_ERRORS<- list() #list with the errors

PATH_FILE <- getwd()
LOG_FILE <- paste0("LOG_", 
                  YEAR, 
                  "_", 
                  MONTH_AS_CHARACTER, 
                  ".csv")
PATH_LOG_FILE <- file.path(paste(BASE_PATH, 
                                 LOG_FILE, 
                                 sep = "/"))

# Path to store the private files (which are not shared in this repository)
PATH_PRIVATE <- file.path(getwd(), 
                          PRIVATE_FOLDER_NAME)


# List with the main internal work directories

directories_name <- list(originals = c("originals"), # Path of the files to import 
                         finals = c("finals"), # Path where the final files are created
                         errors = c("errors"), # Path where the error files are generated
                         backup = c("backup") # Path where the backup files are stored
                         )

# Create/check the existence of the mandatory folders and import its path
directories_path <- lapply(directories_name, 
                           manage_work_folder, 
                           YEAR_BASE_PATH)

# Path to shared folder
PATH_SHARE_ERRORS <- file.path(PATH_SHARE_FOLDER, 
                               YEAR, 
                               IDENTIFIER)

# List with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", 
                 "FECHA", 
                 "COD_BARCO", 
                 "ESTRATO_RIM", 
                 "COD_TIPO_MUE")

# Files to backup
FILES_TO_BACKUP <- c("rim_pre_dump.R", 
                     "rim_pre_dump_functions.R")

# Mail template to send different weight error
EMAIL_TEMPLATE <- "errors_email.Rmd"

# Read the list of contact to send errors
CONTACTS <- read.csv(file.path(PATH_PRIVATE, 
                               "contacts.csv"))

# REUBICATION OF THE FILE ------------------------------------------------------
# Move the ubication of ICES compressed file in the case it is not in 
# originals' folder

move_file(STORED_FILE_PATH, 
          directories_path[["originals"]],
          FILENAME)

# UNCOMPRESS SAMPLES FILE ------------------------------------------------------
# Extrat the files inside de compressed file

COMPRESSED_FILE_PATH <- file.path(directories_path[["originals"]], 
                                  FILENAME)

archive_extract(COMPRESSED_FILE_PATH,
                directories_path[["originals"]])

# USE THE ".txt" FILE TO WORK WITH IT ------------------------------------------
#' When we extract the files inside the ".rar", we have
#' to take the ".txt" archive to make the errors analysis

original_files <- list.files(directories_path[["originals"]])

FILENAME <- grep(".txt", 
                 original_files, 
                 value = TRUE)

# IMPORT FILES -----------------------------------------------------------------
records <- importIPDFile(FILENAME, 
                         by_month = MONTH, 
                         path = directories_path[["originals"]])

# Import sireno fleet
# Firstly download the fleet file from Informes --> Listados --> Por proyecto
# in SIRENO, and then:
fleet_sireno <- read.csv(paste0(getwd(), 
                                "/private/", 
                                "IEOPROBARACANDELARIO.TXT"), 
                         sep = ";", 
                         encoding = "latin1")

fleet_sireno <- fleet_sireno[, c("COD.BARCO", 
                                 "NOMBRE", 
                                 "ESTADO")]

fleet_sireno$COD.BARCO <- gsub("'", 
                               "", 
                               fleet_sireno$COD.BARCO)

# EXPORT FILE TO CSV -----------------------------------------------------------
# file_name <- unlist(strsplit(FILENAME, '.', fixed = T))
# file_name <- paste0(file_name[1], '_raw_imported.csv')
#
# exportCsvSAPMUEBASE(records, file_name, path = PATH_IMPORT)


# START CHECK ------------------------------------------------------------------
check_mes <- check_month(records)

# No longer use prescRiptions
# check_estrato_rim <- checkVariableWithPrescriptions(records, "ESTRATO_RIM")
# check_puerto <- checkVariableWithPrescriptions(records, "COD_PUERTO")
# check_arte <- checkVariableWithPrescriptions(records, "COD_ARTE")
# check_origen <- checkVariableWithPrescriptions(records, "COD_ORIGEN")
# coherence_prescription_rim_mt2 <- coherencePrescriptionsRimMt2(records)

check_estrato_rim <- checkVariableWithMetierCoherence(records, 
                                                      "ESTRATO_RIM")
check_arte <- checkVariableWithMetierCoherence(records, 
                                               "COD_ARTE")
# check_arte <- humanize(check_arte)
# This error is usually detected in checkVariableWithMetierCoherence(records, "COD_ARTE"). To fix it:
# records[records$ESTRATO_RIM=="PALANGRE_CN" & records$COD_PUERTO=="0913", "COD_ARTE"] <- "302"
# records[records$ESTRATO_RIM=="PALANGRE_CN" & records$COD_PUERTO=="1423", "COD_ARTE"] <- "302"

check_origen <- checkVariableWithMetierCoherence(records, 
                                                 "COD_ORIGEN")

check_procedencia <- checkVariableWithMaster("PROCEDENCIA", 
                                             records)

check_metier_coherence <- checkMetierCoherence(records)
check_metier_coherence <- humanize(check_metier_coherence)
# Fix the error detected in the upper check before dumping the final dataframe
# This error is usually detected in checkMetierCoherence. To fix it:
records[records$ESTRATO_RIM == "CERCO_GC" & records$COD_ORIGEN == "010", "COD_ORIGEN"] <- "011"

check_estrategia <- check_strategy(records)

# The MT1 samples are not longer sampled.
# check_duplicados_tipo_muestreo <- check_duplicates_type_sample(records)
# check_falsos_mt2 <- check_false_mt2(records)
# check_falsos_mt1 <- check_false_mt1(records)

# TODO: Change the name of this function:
check_ship_date <- checkShipDate()

check_barcos_extranjeros <- check_foreing_ship(records)
# The function remove_MT1_trips_foreing_vessels(df) remove all the MT1 trips
# with foreign vessels so use it just in case.
# humanize(check_barcos_extranjeros)

# this error is only for informational pourposes
check_especies_mezcla_categoria <- errorsMixedSpeciesInCategory(records)
# exportCsvSAPMUEBASE(check_especies_mezcla_categoria, "errors_mixed_sp_2023_07.csv")

check_mixed_species_as_not_mixed <- errorsMixedSpeciesAsNotMixed(records)
check_mixed_species_as_not_mixed <- humanize(check_mixed_species_as_not_mixed)
# records[records$COD_ESP_MUE=="10725", "COD_ESP_MUE"] <- "10726"
# exportCsvSAPMUEBASE(check_not_mixed_species_in_sample, "check_not_mixed_species_in_sample.csv")

check_categorias <- check_categories(records)
# check_categorias <- humanize(check_categorias)
# check_categorias <- unique(check_categorias)
# unique(check_categorias[, c("PUERTO", "COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA")])
# all the categories are correct in Sireno

check_ejemplares_medidos_na <- check_measured_individuals_na(records)
# if any EJEM_MEDIDOS is NA, must be change to 0.
# TODO: make a function to fix it automatically

check_dni <- checkDni(records)

# Sometimes, one category with various species of the category has various landing weights sampled.
# This is not possible to save it in SIRENO, so with one_category_with_different_landing_weights(df)
# function this mistakes are detected. This errors are separated by influence area and
# must be sent to the sups to fix it after save it in SIRENO
check_one_category_with_different_landing_weights <- one_category_with_different_landing_weights(records)

# Create files to send to sups:
check_one_category_with_different_landing_weights <- humanize(check_one_category_with_different_landing_weights)
errors_category <- separateDataframeByInfluenceArea(check_one_category_with_different_landing_weights, 
                                                    "COD_PUERTO")
# remove empty data frames from list:
errors_category <- Filter(function(x) {
  nrow(x) > 0
}, errors_category)

suf <- paste0(
  "_",
  YEAR,
  "_",
  MONTH_AS_CHARACTER,
  "_",
  "errors_categorias_con_varios_pesos_desembarcados"
)


# exportListToXlsx(errors_category, suffix = suf, path = PATH_ERRORS)

exportListToXlsx(errors_category, 
                 suffix = suf, 
                 path = directories_path[["errors"]])

# SAVE FILES TO SHARED FOLDER --------------------------------------------------
copyFilesToFolder(directories_path[["errors"]], 
                  PATH_SHARE_ERRORS)

# To send the errors category for mail

# The internal_links data frame must have two variables:
# - AREA_INF: influence area with the values GC, GS, GN and AC.
# - INTERNAL_LINK: with the link to the error file in its AREA_INF. If there
# aren't any error file of a certain AREA_INF, must be set to "".
# - NOTES: any notes to add to the email. If there aren't, must be set to "".
accesory_email_info <- data.frame(
  AREA_INF = c(
    "AC",
    "GC",
    "GN",
    "GS"
  ),
  LINK = c(
    "",
    "",
    "",
    ""
  ),
  NOTES = c(
    "",
    "",
    "",
    ""
  )
)


sendErrorsByEmail(
  accesory_email_info = accesory_email_info,
  contacts = CONTACTS,
  credentials_file = "credentials",
  identification_sampling = IDENTIFIER
)


# All the data saved by IPD are lengths samples so the MEDIDA variable can't be
# "P" ("Pesos", weights) or empty. The function fix_medida_variable(df) fix it:
records <- fix_medida_variable(records)

# Check if there are vessels not registered in fleet census
# TODO: check if this is mandatory to check here, in rim_pre_dump.
not_registered_vessels <- unique(records[, "COD_BARCO", 
                                         drop = FALSE])
not_registered_vessels <- merge(not_registered_vessels,
  fleet_sireno,
  by.x = "COD_BARCO",
  by.y = "COD.BARCO",
  all.x = TRUE
)
registered <- c("ALTA DEFINITIVA", 
                "G - A.P. POR NUEVA CONSTRUCCION", 
                "H - A.P. POR REACTIVACION")
not_registered_vessels <- not_registered_vessels[!not_registered_vessels$ESTADO %in% registered, ]
not_registered_vessels <- not_registered_vessels[!is.na(not_registered_vessels$ESTADO), ]


# Check if there are vessels not filtered in ICES project. In this case a
# a warning should be sent to Ricardo with the data upload in Sireno.
not_filtered_vessels <- unique(records[, "COD_BARCO", 
                                       drop = FALSE])
not_filtered_vessels <- merge(not_filtered_vessels,
  fleet_sireno,
  by.x = "COD_BARCO",
  by.y = "COD.BARCO",
  all.x = TRUE
)
not_filtered_vessels <- not_filtered_vessels[is.na(not_filtered_vessels$ESTADO), ]


# By default, the IPD file hasn't the country variable filled. Create and fill
# it with "724" Spain
records$COD_PAIS <- 724

# Check if there are any vessel which is SIRENO code doesn't start with 2 or 0
# and five digits more. In case there are any, check if it is a foreign ship.
which(!grepl("^[2,0]\\d{5}", 
             records$COD_BARCO))


# source: https://github.com/awalker89/openxlsx/issues/111
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
export_to_excel(records, 
                directories_path[["finals"]])


# BACKUP SCRIPTS AND RELATED FILES ----
# first save all files opened
rstudioapi::documentSaveAll()
# and the backup the scripts and files:
sapmuebase::backupScripts(FILES_TO_BACKUP, 
                          path_backup = directories_path[["backup"]])
# backup_files()
