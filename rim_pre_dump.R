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

# YOU HAVE ONLY TO CHANGE THIS VARIABLES: ----

PATH_FILES <- file.path(getwd(), "data/2023/2023_11")

FILENAME <- "muestreos_11_ICES.txt"

MONTH <- 11

YEAR <- "2023"

# VARIABLES --------------------------------------------------------------------
ERRORS <- list() #list with all errors found in data frames
#MESSAGE_ERRORS<- list() #list with the errors

PATH_FILE <- getwd()
MONTH_AS_CHARACTER <- sprintf("%02d", MONTH)
LOG_FILE <- paste("LOG_", YEAR, "_", MONTH_AS_CHARACTER, ".csv", sep="")
PATH_LOG_FILE <- file.path(paste(PATH_FILES, LOG_FILE, sep = "/"))
PATH_BACKUP_FILE <- file.path(paste(PATH_FILES, "backup", sep = "/"))
PATH_ERRORS <- paste(PATH_FILES,"/errors",sep="")
# path to store files as backup
PATH_BACKUP <- file.path(PATH_FILES, "backup")

# list with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_TIPO_MUE")

# files to backup
FILES_TO_BACKUP <- c("rim_pre_dump.R",
                     "rim_pre_dump_functions.R")


# FUNCTIONS --------------------------------------------------------------------
# All the functions required in this script are located in
# revision_volcado_functions.R file.
source('rim_pre_dump_functions.R')


# IMPORT FILES -----------------------------------------------------------------
records <- importIPDFile(FILENAME, by_month = MONTH, path = PATH_FILES)

# Import sireno fleet
# Firstly download the fleet file from Informes --> Listados --> Por proyecto
# in SIRENO, and then:
fleet_sireno <- read.csv(paste0(getwd(), "/private/", "IEOPROBARMARCO.TXT"),
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

check_especies_mezcla_no_mezcla <- check_mixed_as_no_mixed(records)
# humanize(check_especies_mezcla_no_mezcla)

check_especies_mezcla_categoria <- errorsMixedSpeciesInCategory(records)
# exportCsvSAPMUEBASE(check_especies_mezcla_categoria, "errors_mixed_sp_2023_07.csv")

check_not_mixed_species_in_sample<- errorsNoMixedSpeciesInSample(records)
# exportCsvSAPMUEBASE(check_not_mixed_species_in_sample, "check_not_mixed_species_in_sample.csv")

check_categorias <- check_categories(records)
check_categorias <- humanize(check_categorias)
check_categorias <- unique(check_categorias)
# unique(check_categorias[, c("PUERTO", "COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA")])
# all the categories are correct in Sireno

check_ejemplares_medidos_na <- check_measured_individuals_na(records)
# if any EJEM_MEDIDOS is NA, must be change to 0.
# TODO: make a function to fix it automatically
# records <- records[which(!is.na(records$EJEM_MEDIDOS)),]


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

exportListToXlsx(errors_category, suffix = suf, path = PATH_FILES)

# All the data saved by IPD are lengths samples so the MEDIDA variable can't be
# "P" ("Pesos", weights) or empty. The function fix_medida_variable(df) fix it:
records <- fix_medida_variable(records)


# By default, the IPD file hasn't the country variable filled. The
# create_variable_code_country(df) function fix it:
records <- create_variable_code_country(records)


# Check if there are vessels not registered in fleet census
not_registered_vessels <- unique(records[,"COD_BARCO", drop=FALSE])
not_registered_vessels <- merge(not_registered_vessels,
                                fleet_sireno,
                                by.x = "COD_BARCO",
                                by.y = "COD.BARCO",
                                all.x = TRUE)
registered <- c("ALTA DEFINITIVA", "G - A.P. POR NUEVA CONSTRUCCION")
not_registered_vessels <- not_registered_vessels[!not_registered_vessels$ESTADO %in% registered,]
not_registered_vessels <- not_registered_vessels[!is.na(not_registered_vessels$ESTADO),]


# Check if there are vessels not filtered in ICES project. In this case a
# a warning should be sent to Ricardo with the data upload in Sireno.
not_filtered_vessels <- unique(records[,"COD_BARCO", drop=FALSE])
not_filtered_vessels <- merge(not_filtered_vessels,
                                fleet_sireno,
                                by.x = "COD_BARCO",
                                by.y = "COD.BARCO",
                                all.x = TRUE)
not_filtered_vessels <- not_filtered_vessels[is.na(not_filtered_vessels$ESTADO),]


#check if there are any vessel register without COD_PAIS
vessel_without_country_code <- unique(records[is.na(records$COD_PAIS),c("COD_BARCO")])
vessel_without_country_code <- data.frame("COD_BARCO"=vessel_without_country_code)
# First, I check in SIRENO if this vessels are in the fleet master.
vessel_without_country_code <- merge(vessel_without_country_code,
                                     fleet_sireno,
                                     all.x = TRUE,
                                     by.x = "COD_BARCO",
                                     by.y = "COD.BARCO")

# In this case all of them are.
# So this vessels must be saved in SIRENO with 724 code country, so change it:
records[is.na(records$COD_PAIS),c("COD_PAIS")] <- 724
# export_log_file("Change", "COD_PAIS", "NA", "724")
# TODO: this field is bumped in SIRENO? Ask Ricardo, if it doesn't is not
# necessary fix it.

# source: https://github.com/awalker89/openxlsx/issues/111
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
export_to_excel(records)


# BACKUP SCRIPTS AND RELATED FILES ----
# first save all files opened
rstudioapi::documentSaveAll()
# and the backup the scripts and files:
sapmuebase::backupScripts(FILES_TO_BACKUP, path_backup = PATH_BACKUP)
# backup_files()
