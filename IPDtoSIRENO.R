#### IPDtoSIRENO
#### Script to check the monthly data dump from IPD database previous to SIRENO upload
#### 
#### Return csv files with errors detected
####
#### author: Marco A. Amez Fernandez
#### email: ieo.marco.a.amez@gmail.com
#### date of last modification: 19/8//2016
#### version: 0.9
####
#### files required: file from IPD with data for de the dump in SIRENO


# #### CONFIG ##################################################################

# ---- PACKAGES ----------------------------------------------------------------

library(plyr) #mapvalues(): replace items IT'S BETTER TO LOAD plyr BEFORE dplyr
library(dplyr) #arrange_()
library(tools) #file_ext()
library(stringr) #str_split()
library(devtools) # Need this package to use install and install_github

  # ---- install sapmuebase from local
  install("F:/misdoc/sap/sapmuebase")
  # ---- install sapmuebase from github
  #install_github("Eucrow/sapmuebase") # Make sure this is the last version

library(sapmuebase) # and load the library
  
  # ---- install openxlsx
  #install.packages("openxlsx")
library(openxlsx)

#initial_wd <- getwd()
#setwd("F:/misdoc/sap")
#install("sapmuebase")

# ---- SET WORKING DIRECTORY ---------------------------------------------------

setwd("F:/misdoc/sap/IPDtoSIRENO/")


# ---- CONSTANTS ---------------------------------------------------------------

PATH <- getwd()

# ---- GLOBAL VARIABLES --------------------------------------------------------
ERRORS <- list() #list with all errors found in dataframes
MESSAGE_ERRORS<- list() #list with the errors

################################################################################
# YOU HAVE ONLY TO CHANGE THIS VARIABLES:
PATH_FILE <- "F:/misdoc/sap/IPDtoSIRENO"
PATH_DATA<- "/data"
#FILENAME <- "muestreos_especie_4_2016_todos_ANADIDOS_ERRORES.txt"
FILENAME <- "muestreos_especie_5_2016_todos_.txt"
MONTH <- 5
YEAR <- "2016"
################################################################################

MONTH <- sprintf("%02d", MONTH)
LOG_FILE <- paste("LOG_", YEAR, "_", MONTH, ".csv", sep="")
PATH_LOG_FILE <- file.path(paste(PATH_FILE, PATH_DATA, LOG_FILE, sep = "/"))

PATH_BACKUP_FILE <- file.path(paste(PATH_FILE, PATH_DATA, "backup", sep = "/"))

PATH_ERRORS <- paste(PATH_FILE,"/errors",sep="")

# #### RECOVERY DATA SETS ######################################################

data(estrato_rim) #load the data set
#estrato_rim <- estrato_rim
data(puerto)
# puerto <- puerto
data(maestro_flota_sireno)
data(cfpo2015)
data(especies_mezcla)
data(especies_no_mezcla)
data(maestro_categorias)

# #### CONSTANS ################################################################

# list with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_TIPO_MUE")

# #### FUNCTIONS ###############################################################

#function to read operation code if exists. If not, operation code = 0
read_operation_code <- function(){
  operation_code <- 0
  if (file.exists(PATH_LOG_FILE)){
    log_file <- read.csv(PATH_LOG_FILE)
    index_operation_code <- as.numeric(length(log_file$OPERATION_CODE))
    if (index_operation_code != 0){
      operation_code <- log_file$OPERATION_CODE[index_operation_code]
    }
    rm(log_file)
  } else {
    operation_code <- 0
  }
  return(operation_code)
}

# function to export file
# create the file with the operation code
export_file_to_sireno <- function(){
  operation_code <- read_operation_code()
  filename <- file_path_sans_ext(FILENAME)
  filename <- paste(filename, operation_code, sep="_")
  filename <- paste(PATH_FILE, PATH_DATA, filename, sep="/")
  filename <- paste(filename,  ".csv", sep="")
  write.csv(records, filename, quote = FALSE, row.names = FALSE)
}

# function to create and/or update log file
export_log_file <- function(action, variable, erroneus_data="", correct_data="", conditional_variable ="", condition =""){
  
  #append data to file:
  date <- format(as.POSIXlt(Sys.time()), "%d-%m-%Y %H:%M:%S")
    #convert action to uppercase
    action <- toupper(action)
    #obtain the operation code
    operation_code <- read_operation_code() + 1
    
  to_append <- paste(action, variable, erroneus_data, correct_data, conditional_variable, condition, operation_code, date, sep = ",")
  
  #check if the file exists. If not, create it.
  if (!file.exists(PATH_LOG_FILE)){
    header <- "ACTION,variable,ERRONEUS_DATA,CORRECT_DATA,CONDITIONAL_VARIABLE,CONDITION,OPERATION_CODE,DATE"
    write(header, PATH_LOG_FILE)    
  }
  #and write:
  write(to_append, PATH_LOG_FILE, append = TRUE)
}


# ---- function to ckeck variables ---------------------------------------------
#' Check variables
# 
#' Check if the value of variables are consistents to the value in its SIRENO master.
#' It's only available for variables with a data source (master): ESTRATO_RIM, COD_PUERTO,
#' COD_ORIGEN, COD_ARTE, COD_PROCEDENCIA and TIPO_MUESTREO
#' @param variable: one of this values: ESTRATO_RIM, COD_PUERTO, COD_ORIGEN,
#' COD_ARTE, COD_PROCEDENCIA or TIPO_MUESTREO
#' @return Return a dataframe with samples containing erroneus variables
check_variable_with_master <- function (variable){

  if(variable != "ESTRATO_RIM" &&
     variable != "COD_PUERTO" &&
     variable != "COD_ORIGEN" &&
     variable != "COD_ARTE" &&
     variable != "PROCEDENCIA" &&
     variable != "COD_TIPO_MUE"){
    stop(paste("This function is not available for ", variable))
  }
  
  # look if the variable begin with "COD_". In this case, the name of the data source
  # is the name of the variable without "COD_"
  if (grepl("^COD_", variable)){
    variable <- strsplit(variable, "COD_")
    variable <- variable[[1]][2]
  }
  name_data_set <- tolower(variable)
  #search the errors in variable
  errors <- merge(x = records, y = get(name_data_set), all.x = TRUE)
  variable_to_filter <- names(errors[length(errors)])
  errors <- subset(errors, is.na(get(variable_to_filter)))
  #prepare to return
  fields_to_filter <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_ARTE", "COD_ORIGEN", "COD_TIPO_MUE", "PROCEDENCIA")
  
  errors <- errors[,fields_to_filter]
  errors$FECHA <- as.POSIXct(errors$FECHA)
  errors <- arrange_(errors, fields_to_filter)
  errors <- unique(errors)
  #return
  return(errors)
}


# ---- Change levels in a variable of a dataframe ---------------------------------------------
#' Change levels
#' 
#' function to change levels in a variable of a dataframe. Add record to Log file
#' and export file.
#' @param df: dataframe
#' @param variable: variable name (column)
#' @param erroneus_data: the erroneus value to change
#' @param correct_data: the correct value
#' @param conditional_variables: a vector of characters with the name of the conditional variables
#' @param conditions: a vector of characters with the conditional values, whith the same lenght that conditional_variables
#' @return dataframe corrected
correct_levels_in_variable <- function(df, variable, erroneus_data, correct_data, conditional_variables, conditions) {

  if (missing(conditional_variables) && missing(conditions)) {
      df[[variable]] <- mapvalues(df[[variable]], from = erroneus_data, to = correct_data)
      # add to log file
      export_log_file("change", variable, erroneus_data, correct_data)
      #export file
      export_file_to_sireno()
      #return
      return(df)
  } else if (!missing(df) && !missing(variable) && !missing(erroneus_data) && !missing(correct_data)){
    # TODO: check if conditional_variables and conditiosn are lists??
      data <- df
      
      for (i in 1:length(conditional_variables)) {
        data <- data[data[conditional_variables[i]]==conditions[i],]
      }
      
      index_row_to_change <- which(data[[variable]]==erroneus_data)
      row_names<-row.names(data[index_row_to_change,])
      row_to_change <- df[row_names,]
      levels_in_df <- levels(df[[variable]])
      # check if the levels exists in df ...
      if (!(correct_data %in% levels_in_df)){ #if not ...
        levels(df[[variable]]) <- c(levels(df[[variable]]), correct_data) # ... add it
      }
      
      row_to_change[variable] <- correct_data
      df[row.names(df) %in% row_names,] <- row_to_change

      return(df)  
  } else {
    stop("Some argument is missing.")
  }
} 


# function to remove trip. Add record to Log file and export file.
# It's imperative the next data to identify a trip:
# date, cod_type_sample, cod_ship, cod_port, cod_gear, cod_origin and rim_stratum
# df: dataframe
# return: dataframe without the deleted trips
remove_trip <- function(df, date, cod_type_sample, cod_ship, cod_port, cod_gear, cod_origin, rim_stratum){
  df <- df[!(df["FECHA"]==date & df["COD_TIPO_MUE"] == cod_type_sample & df["COD_BARCO"] == cod_ship & df["COD_PUERTO"] == cod_port & df["COD_ARTE"] == cod_gear & df["COD_ORIGEN"] == cod_origin & df["ESTRATO_RIM"] == rim_stratum),]
  
  # add to log file
  error_text <- paste(date, cod_type_sample, cod_ship, cod_port, cod_gear, cod_origin, rim_stratum, sep=" ")
  export_log_file("remove trip", "trip", error_text)
  #export file
  export_file_to_sireno()
  # return
  return(df)
}

# function to check the month: Check if all the data in the dataframe belongs to
# the same month, allocated in MONTH variable
# df: dataframe to check
# return a dataframe with the samples of incorrect month
check_month <- function(df){
  df$months <- sapply (df["FECHA"], function(x){format(x, "%m")})
  erroneus_months <- as.data.frame(unique(df$months))
  erroneus_months <- erroneus_months %>%
    filter(FECHA != MONTH)
  erroneus_samples <- merge(x = df, y = erroneus_months, by.x = "months", by.y = "FECHA", all.y = TRUE)
  return(erroneus_samples)
}


# function to check the type of sample.
# df: dataframe to check
# return samples with TIPO_MUE != to "CONCURRENTE EN LONJA"
check_type_sample <- function(df){
  errors <- df[df[["TIPO_MUE"]]!="CONCURRENTE EN LONJA",]
  return(errors)
}


# function to search duplicate samples by type of sample (between MT1 and MT2)
# df: dataframe where find duplicate samples
# returns a dataframe with duplicate samples
check_duplicates_type_sample <- function(df){
  mt1 <- df[df["COD_TIPO_MUE"]=="MT1A",c("COD_PUERTO","FECHA","COD_BARCO","ESTRATO_RIM")]
  mt1 <- unique(mt1)
  mt2 <- df[df["COD_TIPO_MUE"]=="MT2A",c("COD_PUERTO","FECHA","COD_BARCO","ESTRATO_RIM")]
  mt2 <- unique(mt2)
  
  duplicated <- merge(x = mt1, y = mt2) 
  
  return(duplicated)
}

# function to search false mt2 samples: samples with COD_TIPO_MUE as MT2A and
# without any lenght
# df: dataframe
# return: dataframe with erroneus samples
check_false_mt2 <- function(df){
  dataframe <- df
  dataframe$FECHA <- as.POSIXct(dataframe$FECHA)
  mt2_errors <- dataframe %>%
    filter(COD_TIPO_MUE=="MT2A") %>%
    group_by(COD_PUERTO, FECHA, COD_BARCO, ESTRATO_RIM) %>%
    summarise(summatory = sum(EJEM_MEDIDOS)) %>%
    filter(summatory == 0)
  
  return(mt2_errors)
}

# function to search false mt1 samples: samples with COD_TIPO_MUE as MT1A and
# lenghts
# df: dataframe
# return: dataframe with erroneus samples
check_false_mt1 <- function(df){
  dataframe <- df
  dataframe$FECHA <- as.POSIXct(dataframe$FECHA)
  mt1_errors <- dataframe %>%
    filter(COD_TIPO_MUE=="MT1A") %>%
    group_by(COD_PUERTO, FECHA, COD_BARCO, ESTRATO_RIM) %>%
    summarise(summatory = sum(EJEM_MEDIDOS)) %>%
    filter(summatory != 0)
  
  return(mt1_errors)
}

# function to search foreing ships
# the BAR_COD code in the foreing ships begins with an 8 and continue with 5 digits
# df: dataframe
# return: dataframe with foreing ships and COD_TIPO_MUE
check_foreing_ship <- function(df){
  dataframe <- df
  dataframe$FECHA <- as.POSIXct(dataframe$FECHA)
  dataframe$COD_BARCO <- as.character(dataframe$COD_BARCO)
  ships <- dataframe %>%
    filter(grepl("^8\\d{5}",COD_BARCO)) %>%
    group_by(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM) %>%
    count(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM)
  
  return(ships[, c("FECHA", "COD_TIPO_MUE", "COD_BARCO", "COD_PUERTO", "COD_ARTE", "COD_ORIGEN", "ESTRATO_RIM")])
}

# TODO: function to search ships not active
# ships <- as.data.frame(unique(records[,c("COD_BARCO" )]))
# colnames(ships) <- "COD_BARCO"
# ships_sireno <- merge(x=ships, y=maestro_flota_sireno, by.x = "COD_BARCO", by.y = "BARCOD", all.x = TRUE)

# function to check mixed species keyed as non mixed species: in COD_ESP_MUE
# there are codes from mixed species 
# df: dataframe
# return a dataframe with the samples with species keyed as non mixed species
check_mixed_as_no_mixed <- function(df){
  non_mixed <- merge(x=df, y=especies_mezcla["COD_ESP_CAT"], by.x = "COD_ESP_MUE", by.y = "COD_ESP_CAT")
  return(non_mixed)
}

# function to check no mixed species keyed as mixed species: in COD_ESP_MUE
# there are codes from mixed species 
# df: dataframe
# return a dataframe with the samples with species keyed as non mixed species
check_no_mixed_as_mixed <- function(df){
  non_mixed <- merge(x=records, y=especies_no_mezcla["COD_ESP"], by.x = "COD_ESP_MUE", by.y = "COD_ESP")
  return(non_mixed)
}


# function to check if the categories in the IPD file are in the categories master of the SIRENO
# df: dataframe
# return: dataframe of samples with erroneus categories
check_categories <- function(df){
  
  maestro_categorias[["TRACK"]] <- "OK"
  df[["FECHA"]]<-as.POSIXct(df[["FECHA"]])
  errors <- merge(x = df, y = maestro_categorias, by.x = c("COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA"), by.y = c("PUECOD", "ESPCOD", "ESPCAT"), all.x = TRUE)
  errors <- errors %>% 
    filter(is.na(TRACK)) %>%
    select(COD_PUERTO, FECHA, COD_BARCO, COD_ESP_MUE, COD_CATEGORIA) %>%
    arrange(COD_PUERTO, FECHA, COD_BARCO, COD_ESP_MUE, COD_CATEGORIA)
  errors <- unique(errors)
  
  
  return (errors)
}

# ---- function to check if one category has two or more different P_MUE_DES ----------------------------------------
#
#' function to check if one category has two or more different P_MUE_DES. 
#' Mostly, this cases correspond to mixed species or sexed species, but in other
#' cases this can be an error in the keyed process by IPD:
#' - in some mixed species, one category (0901) contains two 'species
#' of the category', for example Lophis piscatorius and L. budegassa, everyone
#' with its own 'landing weight'. In the dumped in SIRENO, only the first of the
#' 'landing weight' is used and the records with the second 'landing weight' are
#' discarded. The correct way to introduce this samples in SIRENO is with 
#' the specie of the second 'landing weight' keyed like another category (0902)
#' 
#' With this function we obtain all the categories with two or more different
#' 'landing weight'.
# 
#' @param df: dataframe to modify
#' @return Return a dataframe with all the categories with two or more different
#' 'landing weight'
#' 


check_one_category_with_different_landing_weight <- function(df){
  df <- df[,c(BASE_FIELDS, "COD_ESP_MUE", "COD_CATEGORIA", "P_MUE_DESEM")]
  df$FECHA <- as.POSIXct(df$FECHA)
  fields_to_count <- c(BASE_FIELDS, "COD_ESP_MUE", "COD_CATEGORIA")
  df_filtrado <- df %>%
    distinct() %>%
    count_(fields_to_count) %>%
    filter(n>1)
}

# function to export file to excel.
# if this error is returned:
#    Error: zipping up workbook failed. Please make sure Rtools is installed or a zip application is available to R.
#    Try installr::install.rtools() on Windows.
# run:Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
# source: https://github.com/awalker89/openxlsx/issues/111

export_to_excel <- function(df){
  month_in_spanish <- c("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")
  
  # TODO: add this to import_IPD_file in sapmuebase
  df[["FECHA"]] <- format(df[["FECHA"]], "%d/%m/%Y")
  
  filename = paste("MUESTREOS_IPD_", month_in_spanish[as.integer(MONTH)], "_2016.xlsx", sep="")
  filepath = paste(PATH_FILE, PATH_DATA, sep="")
  filepath = paste(filepath, filename, sep = "/")
  colnames(df) <- c("FECHA","PUERTO","BUQUE","ARTE","ORIGEN","METIER","PROYECTO","TIPO MUESTREO","NRECHAZOS","NBARCOS MUESTREADOS","CUADRICULA","LAT DECIMAL","LON DECIMAL","DIAS_MAR","PESO_TOTAL","COD_ESP_TAX","TIPO_MUESTREO","PROCEDENCIA","COD_CATEGORIA","PESO","COD_ESP_MUE","SEXO","PESO MUESTRA","MEDIDA","TALLA","NEJEMPLARES","COD_PUERTO_DESCARGA")
  df[["FECHA"]] <- as.character(df[["FECHA"]]) #it's mandatory convert date to character. I don't know why.
  write.xlsx(df, filepath, keepNA=TRUE, colnames=TRUE)
}


# ---- function to add variable country ----------------------------------------
#
#' Add variable country code
# 
#' Add variable country code (COD_PAIS) at the end of the dataframe
#' @param df: dataframe to modify
#' @return Return a dataframe with the variable country code
#' 
create_variable_code_country <- function(df){
  ships <- maestro_flota_sireno[,c("BARCOD", "PAICOD")]
  with_country <- merge(x = df, y = ships, by.x = "COD_BARCO", by.y = "BARCOD", all.x = TRUE)
  #the order of the variables has been changed in the merge (I think), so
  #we need to reorder:
  with_country <- with_country[,c(2,3,1,4:length(with_country))]
  colnames(with_country["PAICOD"])<-c("COD_PAIS")
  return (with_country)
}


# #### IMPORT FILE #############################################################
records <- importIPDFile(paste(PATH_DATA,FILENAME, sep="/"))

# #### START CHECK #############################################################

ver <- records[records$FECHA=="2016/06/01" & records$COD_BARCO=="202618" & records$COD_ESP_MUE=="10821",]

check_mes <- check_month(records)


check_estrato_rim <- check_variable_with_master("ESTRATO_RIM")


check_puerto <- check_variable_with_master("COD_PUERTO")


check_arte <- check_variable_with_master("COD_ARTE")


check_origen <- check_variable_with_master("COD_ORIGEN")


check_procedencia <- check_variable_with_master("PROCEDENCIA")


check_tipo_muestreo <- check_variable_with_master("COD_TIPO_MUE")

# TODO: add check_type_sample to check_variable_with_master
check_nombre_tipo_muestreo <- check_type_sample(records)


check_duplicados_tipo_muestreo <- check_duplicates_type_sample(records)


check_falsos_mt2 <- check_false_mt2(records)


check_falsos_mt1 <- check_false_mt1(records)


check_barcos_extranjeros <- check_foreing_ship(records)


check_especies_mezcla_no_mezcla <- check_mixed_as_no_mixed(records)


check_especies_no_mezcla_mezcla <- check_no_mixed_as_mixed(records)


check_categorias <- check_categories(records)

check_one_category_with_different_landing_weight <- check_one_category_with_different_landing_weight(records)

records <- create_variable_code_country(records)

# source: https://github.com/awalker89/openxlsx/issues/111
Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
export_to_excel(records)





