# IPDtoSIRENO
# Script to check the monthly data dump from IPD database previous to SIRENO
# upload.
# Return csv files with errors detected
# author: Marco A. Ámez Fernandez
# email: ieo.marco.a.amez@gmail.com
# files required: file from IPD with data to save in SIRENO and IPD_to_SIRENO.R
# with all the functions used in this script

# PACKAGES ---------------------------------------------------------------------

library(dplyr)
library(blastula) # to send emails
library(tools) #file_ext() and file_path_sans_ext
# library(devtools) # Need this package to use install and install_github
source("barbarize.R")

# ---- install sapmuebase from local
# remove.packages("sapmuebase")
# .rs.restartR()
# install("C:/Users/ieoma/Desktop/sap/sapmuebase")
# ---- install sapmuebase from github
#remove.packages("sapmuebase")
#.rs.restartR()
#install_github("Eucrow/sapmuebase")

library(sapmuebase) # and load the library


# ---- install openxlsx
#install.packages("openxlsx")
library(openxlsx)

# FUNCTIONS --------------------------------------------------------------------
# All the functions required in this script are located in
# revision_volcado_functions.R file.
source('rim_pre_dump_functions.R')

# YOU HAVE ONLY TO CHANGE THIS VARIABLES: ----

PATH_FILES <- file.path(getwd(), "data/2024/2024_04")

# Path to store in nextCloud the errors of "one category with different landing weights"

PATH_SHARE_FOLDER <- "C:/Users/alberto.candelario/Desktop/nextCloud/SAP_RIM/RIM_data_review"

# Name of the folder that is stored the items to send error mail

PRIVATE_FOLDER_NAME <- "private"

FILENAME <- "muestreos_4_ICES.txt"

MONTH <- 4

YEAR <- "2024"

# Suffix to add to path. Use only in case MONTH is a vector of months. This
# suffix will be added to the end of the path with a "_" as separation.
suffix_multiple_months <- "annual_post_oab"

# Suffix to add at the end of the export file name. This suffix will be added to
# the end of the file name with a "_" as separation.
suffix <- ""

# VARIABLES --------------------------------------------------------------------
ERRORS <- list() #list with all errors found in data frames
#MESSAGE_ERRORS<- list() #list with the errors

PATH_FILE <- getwd()
MONTH_AS_CHARACTER <- sprintf("%02d", MONTH)
LOG_FILE <- paste("LOG_", YEAR, "_", MONTH_AS_CHARACTER, ".csv", sep="")
PATH_LOG_FILE <- file.path(paste(PATH_FILES, LOG_FILE, sep = "/"))
PATH_BACKUP_FILE <- file.path(paste(PATH_FILES, "backup", sep = "/"))
PATH_ERRORS <- file.path(PATH_FILES, "errors")
PATH_PRIVATE_FILES <- file.path(getwd(), PRIVATE_FOLDER_NAME)

#In order to create/check the existence of the backup and error directories

DIR_BACKUP_ERRORS <- list(PATH_BACKUP_FILE, PATH_ERRORS)
lapply(DIR_BACKUP_ERRORS, createDirectory)

# path to store files as backup
PATH_BACKUP <- file.path(PATH_FILES, "backup")

IDENTIFIER <- createIdentifier(MONTH, YEAR, MONTH_AS_CHARACTER, suffix_multiple_months, suffix)
# path to shared folder
PATH_SHARE_LANDING_ERRORS <- file.path(PATH_SHARE_FOLDER, YEAR, IDENTIFIER)

# list with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_TIPO_MUE")

# files to backup
FILES_TO_BACKUP <- c("rim_pre_dump.R",
                     "rim_pre_dump_functions.R")

# Mail template to send different weight error
EMAIL_TEMPLATE <- "errors_email.Rmd"

# Read the list of contact to send errors
CONTACTS <- read.csv(file.path(PATH_PRIVATE_FILES, "contacts.csv"))


# IMPORT FILES -----------------------------------------------------------------
records <- importIPDFile(FILENAME, by_month = MONTH, path = PATH_FILES)
# Import sireno fleet
# Firstly download the fleet file from Informes --> Listados --> Por proyecto
# in SIRENO, and then:
fleet_sireno <- read.csv(paste0(getwd(), "/private/", "IEOPROBARSIRENO.TXT"),
                         sep = ";", encoding = "latin1")
fleet_sireno <- fleet_sireno[, c("COD.BARCO", "NOMBRE", "ESTADO")]
fleet_sireno$COD.BARCO <- gsub("'", "", fleet_sireno$COD.BARCO)


# EXPORT FILE TO CSV -----------------------------------------------------------
file_name <- unlist(strsplit(FILENAME, '.', fixed = T))
file_name <- paste0(file_name[1], '_raw_imported.csv')

exportCsvSAPMUEBASE(records, file_name, path = PATH_FILES)


#' Check code:
#' Check variable with prescriptions data set. Use the
#' metier_coherence data set from sapmuebase.
#' @param df Dataframe where the variable to check is.
#' @param variable Variable to check as character. Allowed variables:
#' ESTRATO_RIM, COD_ORIGEN, COD_ARTE, METIER_DCF and CALADERO_DCF.
#' @return dataframe with errors
checkVariableWithMetierCoherence <- function(df, variable){

    valid_variables = c("ESTRATO_RIM", "COD_ORIGEN", "COD_ARTE", "METIER_DCF",
                        "CALADERO_DCF")

    if (!(variable %in% valid_variables)) {
      stop(paste("This function is not available for variable ", variable))
    }

    allowed <- sapmuebase::metier_coherence[,variable]

    df <- df[!(df[[variable]] %in% allowed), ]


    fields <- BASE_FIELDS

    if (!(variable %in% BASE_FIELDS)) {
      fields <- c(BASE_FIELDS, variable)
    }

    df <- df[, fields]

    df <- unique(df)

    return(df)

}



# START CHECK ------------------------------------------------------------------
# if any error is detected use function:
# correct_levels_in_variable(df, variable, erroneus_data, correct_data, conditional_variables, conditions)
# to fix it. This function return the 'df' already corrected, so you have to assign
# the data returned to the records dataframe: records <- correct_level_in_variable.

check_mes <- check_month(records)


# Not longer use presctiptions
# check_estrato_rim <- checkVariableWithPrescriptions(records, "ESTRATO_RIM")
# check_puerto <- checkVariableWithPrescriptions(records, "COD_PUERTO")
# check_arte <- checkVariableWithPrescriptions(records, "COD_ARTE")
# check_origen <- checkVariableWithPrescriptions(records, "COD_ORIGEN")
# coherence_prescription_rim_mt2 <- coherencePrescriptionsRimMt2(records)

check_estrato_rim <- checkVariableWithMetierCoherence(records, "ESTRATO_RIM")
check_arte <- checkVariableWithMetierCoherence(records, "COD_ARTE")
check_arte <- humanize(check_arte)
check_origen <- checkVariableWithMetierCoherence(records, "COD_ORIGEN")


# TODO: ¡¡¡¡¡!!!!! create checkMetierCoherence function!!
records[records$ESTRATO_RIM=="CERCO_GC" & records$COD_ORIGEN=="010", "COD_ORIGEN"] <- "011"

check_procedencia <- checkVariableWithMaster("PROCEDENCIA", records)

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

# this error is only for  ******* pourposes
check_especies_mezcla_categoria <- errorsMixedSpeciesInCategory(records)
# exportCsvSAPMUEBASE(check_especies_mezcla_categoria, "errors_mixed_sp_2023_07.csv")

check_mixed_species_as_not_mixed <- check_mixed_as_not_mixed(records)
# exportCsvSAPMUEBASE(check_not_mixed_species_in_sample, "check_not_mixed_species_in_sample.csv")

check_categorias <- check_categories(records)
check_categorias <- humanize(check_categorias)
check_categorias <- unique(check_categorias)
unique(check_categorias[, c("PUERTO", "COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA")])
# all the categories are correct in Sireno

check_ejemplares_medidos_na <- check_measured_individuals_na(records)
# if any EJEM_MEDIDOS is NA, must be change to 0.
# TODO: make a function to fix it automatically

check_dni <- checkDni(records)

# Sometimes, one category with various species of the category has various landing weights sampled.
# This is not possible to save it in SIRENO, so with one_category_with_different_landing_weights(df)
# function this mistakes are detected. This errors are separated by influence area and
# must be send to the sups to fix it after save it in SIRENO
check_one_category_with_different_landing_weights <- one_category_with_different_landing_weights(records)

# Create files to send to sups:
check_one_category_with_different_landing_weights <- humanize(check_one_category_with_different_landing_weights)
errors_category <- separateDataframeByInfluenceArea(
  check_one_category_with_different_landing_weights,
  "COD_PUERTO")
#remove empty data frames from list:
errors_category <- Filter(function(x){
                            nrow(x) > 0
                          }, errors_category)

suf <- paste0("_",
              YEAR,
              "_",
              MONTH_AS_CHARACTER,
              "_",
              "errors_categorias_con_varios_pesos_desembarcados")

exportListToXlsx(errors_category, suffix = suf, path = PATH_ERRORS)

# SAVE FILES TO SHARED FOLDER --------------------------------------------------
copyFilesToFolder(PATH_ERRORS, PATH_SHARE_LANDING_ERRORS)

#To send the errors category for mail

# The internal_links data frame must have two variables:
# - AREA_INF: influence area with the values GC, GS, GN and AC.
# - INTERNAL_LINK: with the link to the error file in its AREA_INF. If there
# aren't any error file of a certain AREA_INF, must be set to "".
# - NOTES: any notes to add to the email. If there aren't, must be set to "".
accesory_email_info <- data.frame(
  AREA_INF = c("AC",
               "GC",
               "GN",
               "GS"),
  LINK = c("",
           "https://saco.csic.es/index.php/f/213076532",
           "https://saco.csic.es/index.php/f/213076535",
           ""),
  NOTES = c("",
            "",
            "",
            "")
)


sendErrorsByEmail(accesory_email_info = accesory_email_info,
                  contacts = CONTACTS,
                  credentials_file = "credentials",
                  identification_sampling = IDENTIFIER)


# All the data saved by IPD are lengths samples so the MEDIDA variable can't be
# "P" ("Pesos", weights) or empty. The function fix_medida_variable(df) fix it:
records <- fix_medida_variable(records)

# Check if there are vessels not registered in fleet census
not_registered_vessels <- unique(records[, "COD_BARCO", drop = FALSE])
not_registered_vessels <- merge(not_registered_vessels,
                                fleet_sireno,
                                by.x = "COD_BARCO",
                                by.y = "COD.BARCO",
                                all.x = TRUE)
registered <- c("ALTA DEFINITIVA", "G - A.P. POR NUEVA CONSTRUCCION", "H - A.P. POR REACTIVACION")
not_registered_vessels <- not_registered_vessels[!not_registered_vessels$ESTADO %in% registered,]
not_registered_vessels <- not_registered_vessels[!is.na(not_registered_vessels$ESTADO),]
#Change the vessel "PARA LOS TRES" to "PARA LOS TRES I"
levels(records$COD_BARCO) <- c(levels(records$COD_BARCO), "209343")
records[records$COD_BARCO == "022302", "COD_BARCO"] <- "209343"


# Check if there are vessels not filtered in ICES project. In this case a
# a warning should be sent to Ricardo with the data upload in Sireno.
not_filtered_vessels <- unique(records[, "COD_BARCO", drop = FALSE])
not_filtered_vessels <- merge(not_filtered_vessels,
                                fleet_sireno,
                                by.x = "COD_BARCO",
                                by.y = "COD.BARCO",
                                all.x = TRUE)
not_filtered_vessels <- not_filtered_vessels[is.na(not_filtered_vessels$ESTADO),]


# By default, the IPD file hasn't the country variable filled. Create and fill
# it with "724" Spain
records$COD_PAIS <- 724

# Check if there are any vessel which is SIRENO code doesn't start with 2 or 0
# and five digits more. In case there are any, check if it is a foreign ship.
which(!grepl("^[2,0]\\d{5}", records$COD_BARCO ))


# source: https://github.com/awalker89/openxlsx/issues/111
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
export_to_excel(records)


# BACKUP SCRIPTS AND RELATED FILES ----
# first save all files opened
rstudioapi::documentSaveAll()
# and the backup the scripts and files:
sapmuebase::backupScripts(FILES_TO_BACKUP, path_backup = PATH_BACKUP)
# backup_files()
