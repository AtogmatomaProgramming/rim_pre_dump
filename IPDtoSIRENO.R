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

# list with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", "LOCCODE", "PUERTO", "FECHA", "COD_BARCO", "BARCO", "ESTRATO_RIM", "COD_TIPO_MUE", "TIPO_MUE")

################################################################################
# YOU HAVE ONLY TO CHANGE THIS VARIABLES:
PATH_FILE <- "F:/misdoc/sap/IPDtoSIRENO"
PATH_DATA<- "/data"
#FILENAME <- "muestreos_especie_4_2016_todos_ANADIDOS_ERRORES.txt"
FILENAME <- "muestreos_especie_4_2016_todos_.txt"
MONTH <- 4
YEAR <- "2016"
################################################################################

LOG_FILE <- paste("LOG_", YEAR, "_", MONTH, ".csv", sep="")

PATH_ERRORS <- paste(PATH_FILE,"/errors",sep="")

# #### RECOVERY DATA SETS ######################################################

data(estrato_rim) #load the data set
# estrato_rim <- estrato_rim

data(puerto)
# puerto <- puerto
# #### CONSTANS ################################################################

###list with the common fields used in the tables
BASE_FIELDS <- c("PUERTO", "FECHA", "BARCO", "UNIPESCOD", "TIPO_MUESTREO")

# #### FUNCTIONS ###############################################################




# function to create and/or update log file
export_log_file <- function(action, variable, erroneus_data, correct_data){
  
  #check if the file exists. If not, create it.
  file_with_path <- file.path(paste(PATH_FILE, PATH_DATA, LOG_FILE, sep = "/"))
  
  if (!file.exists(file_with_path)){
    header <- "ACTION,variable,ERRONEUS_DATA,CORRECT_DATA,DATE"
    write(header, file_with_path)    
  }
  
  #append data to file:
  date <- format(as.POSIXlt(Sys.time()), "%d-%m-%Y %H:%M:%S")
    #convert action to uppercase
    action <- toupper(action)
    
  to_append <- paste(action, variable, erroneus_data, correct_data, date, sep = ",")
  write(to_append, file_with_path, append = TRUE)
  
}


# function to ckeck variables.
# It's only available for variables with a data source (master): ESTRATO_RIM, COD_PUERTO,
# COD_ORIGEN, COD_ARTE, COD_PROCEDENCIA and TIPO_MUESTREO
# return a dataframe with samples with the erroneus variables
check_variable_with_master <- function (variable){
  
  if(variable != "ESTRATO_RIM" && variable != "COD_PUERTO" &&
     variable != "COD_ORIGEN" && variable != "COD_ARTE" &&
     variable != "COD_PROCEDENCIA" && variable != "COD_TIPO_MUESTREO"){
    stop(paste("This function is not available for ", variable))
  }
  
  # look if the variable begin with "COD_". In this case, the name of the data source
  # is the name of the variable without "COD."
  if (grepl("^COD_", variable)){
    variable <- strsplit(variable, "COD_")
    variable <- variable[[1]][2]
  }
  name_data_set <- tolower(variable)
  errors <- merge(x = records, y = get(name_data_set), all.x = TRUE)
  variable_to_filter <- names(errors[length(errors)])
  errors <- subset(errors, is.na(get(variable_to_filter)))
  return(errors)
}



# function to change the level in a variable of a dataframe:
# df: dataframe
# variable: variable (column)
# erroneus_data: a vector of characters with the erroneus factor
# correct_data: a vector of characters with its correct factor
# conditional_variable: a vector of characters with the name of the conditional variable
# condition: a vector of characters with the conditional value
# correct_levels_in_variable <- function(df, variable, erroneus_data, correct_data) {
#   for (data in erroneus_data) {
#     index <- which(erroneus_data==data)
#     df[[variable]] <- recode(df[[variable]], data = correct_data[index])
#   }
#   return(df)
# }

correct_levels_in_variable <- function(df, variable, erroneus_data, correct_data, conditional_variable, condition) {
  # for (data in erroneus_data) {
  #   index <- which(erroneus_data==data)
  #   df[[variable]] <- recode(df[[variable]], data = correct_data[index])
  # }

  if (missing(conditional_variable) && missing(condition)) {
      df[[variable]] <- recode(df[[variable]], erroneus_data = correct_data)
      return(df)
  } else if (!missing(df) && !missing(variable) && !missing(erroneus_data) && !missing(correct_data)){
    filtered <- df[df[conditional_variable] == condition,]
    filtered[[variable]] <- mapvalues(filtered[[variable]], from = erroneus_data, to = correct_data)
    
    not_filtered <- df[df[conditional_variable] != condition,]
    
    df<-rbind(filtered, not_filtered)
    return(df)  
  } else {
    stop("Some argument is missing.")
  }
} 

# function to export file
exportFileLog <- function(df, log){
  filename <- file_path_sans_ext(basename(FILENAME))
  
  MONTH <- sprintf("%02d", MONTH)

  file = paste("samples_up", YEAR, MONTH, "LOG", log, sep="_")
  write.csv(df, file)
  exportLogFile (file)
}

# #### IMPORT FILE #############################################################
records <- import_IPD_file(paste(PATH_DATA,FILENAME, sep="/"))


# #### START CHECK #############################################################


check_estrato_rim <- check_variable_with_master("ESTRATO_RIM")
check_puerto <- check_variable_with_master("COD_PUERTO")
check_arte <- check_variable_with_master("COD_ARTE")
check_origen <- check_variable_with_master("COD_ORIGEN")
levels(check_origen$COD_ORIGEN)
check_procedencia <- check_variable_with_master("COD_PROCEDENCIA")
check_tipo_muestreo <- check_variable_with_master("COD_TIPO_MUESTREO")


erroneus <- "OTB_DEF"
correct <- "BACA_CN"
prueba <- correct_levels_in_variable(records, "ESTRATO_RIM", "OTB_DEF", "baca", "ESTRATO_RIM")
levels(prueba$ESTRATO_RIM)



