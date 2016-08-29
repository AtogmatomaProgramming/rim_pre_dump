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

################################################################################
# YOU HAVE ONLY TO CHANGE THIS VARIABLES:
PATH_FILE <- "F:/misdoc/sap/IPDtoSIRENO"
PATH_DATA<- "/data"
#FILENAME <- "muestreos_especie_4_2016_todos_ANADIDOS_ERRORES.txt"
FILENAME <- "muestreos_especie_4_2016_todos_.txt"
MONTH <- 4
YEAR <- "2016"
################################################################################

MONTH <- sprintf("%02d", MONTH)
LOG_FILE <- paste("LOG_", YEAR, "_", MONTH, ".csv", sep="")
PATH_LOG_FILE <- file.path(paste(PATH_FILE, PATH_DATA, LOG_FILE, sep = "/"))

PATH_BACKUP_FILE <- file.path(paste(PATH_FILE, PATH_DATA, "backup", sep = "/"))

PATH_ERRORS <- paste(PATH_FILE,"/errors",sep="")

# #### RECOVERY DATA SETS ######################################################

data(estrato_rim) #load the data set
# estrato_rim <- estrato_rim

data(puerto)
# puerto <- puerto
# #### CONSTANS ################################################################

# list with the common fields used in all tables
BASE_FIELDS <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_TIPO_MUE")

# #### FUNCTIONS ###############################################################

#function to read operation code
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
export_log_file <- function(action, variable, erroneus_data, correct_data, conditional_variable ="", condition =""){
  
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



# function to ckeck variables.
# It's only available for variables with a data source (master): ESTRATO_RIM, COD_PUERTO,
# COD_ORIGEN, COD_ARTE, COD_PROCEDENCIA and TIPO_MUESTREO
# return a dataframe with samples with the erroneus variables
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



# function to change the level in a variable of a dataframe:
# df: dataframe
# variable: variable (column)
# erroneus_data: a vector of characters with the erroneus factor
# correct_data: a vector of characters with its correct factor
# conditional_variable: a vector of characters with the name of the conditional variable
# condition: a vector of characters with the conditional value
correct_levels_in_variable <- function(df, variable, erroneus_data, correct_data, conditional_variable, condition) {

  if (missing(conditional_variable) && missing(condition)) {
      df[[variable]] <- mapvalues(df[[variable]], from = erroneus_data, to = correct_data)
      # add to log file
      export_log_file("change", variable, erroneus_data, correct_data)
      #export file
      export_file_to_sireno()
      #return
      return(df)
  } else if (!missing(df) && !missing(variable) && !missing(erroneus_data) && !missing(correct_data)){
      filtered <- df[df[conditional_variable] == condition,]
      filtered[[variable]] <- mapvalues(filtered[[variable]], from = erroneus_data, to = correct_data)
      
      not_filtered <- df[df[conditional_variable] != condition,]
      
      df<-rbind(filtered, not_filtered)
      
      # add to log file
      error_text <- paste(erroneus_data, "from", conditional_variable, condition, sep=" ")
      export_log_file("change", variable, erroneus_data, correct_data, conditional_variable, condition)
      #export file
      export_file_to_sireno()
      # return
      return(df)  
  } else {
    stop("Some argument is missing.")
  }
} 




# #### IMPORT FILE #############################################################
records <- import_IPD_file(paste(PATH_DATA,FILENAME, sep="/"))


# #### START CHECK #############################################################

check_estrato_rim <- check_variable_with_master("ESTRATO_RIM")
records <- correct_levels_in_variable(records, "ESTRATO_RIM", "OTB_DEF", "BACA_CN")


check_puerto <- check_variable_with_master("COD_PUERTO")

check_arte <- check_variable_with_master("COD_ARTE")
records <- correct_levels_in_variable(records, "COD_ARTE", 200, 201, "ESTRATO_RIM", "BETA_CN")
records <- correct_levels_in_variable(records, "COD_ARTE", 150, 102, "ESTRATO_RIM", "BACA_CN")
records <- correct_levels_in_variable(records, "COD_ARTE", 150, 102, "ESTRATO_RIM", "JURELERA_CN")
records <- correct_levels_in_variable(records, "COD_ARTE", 200, 202, "ESTRATO_RIM", "ENMALLE_AC")
records <- correct_levels_in_variable(records, "COD_ARTE", 390, 302, "ESTRATO_RIM", "PALANGRE_AC")

check_origen <- check_variable_with_master("COD_ORIGEN")
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "008", "COD_PUERTO", "0409") #Santoña VIIIc
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "009", "COD_PUERTO", "0904") #Cedeira VIIIc
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "009", "COD_PUERTO", "0907") #A Coruña VIIIc
records <- correct_levels_in_variable(records, "COD_ORIGEN", "031", "002", "COD_PUERTO", "0907") #A Coruña VIIc
records <- correct_levels_in_variable(records, "COD_ORIGEN", "005", "010", "COD_PUERTO", "0917") #Ribeira IXa
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "010", "COD_PUERTO", "0921") #Marín VIIIc
records <- correct_levels_in_variable(records, "COD_ORIGEN", "005", "010", "COD_PUERTO", "0921") #Marín IXa
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "009", "COD_PUERTO", "0925") #Burela IXa
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "008", "COD_PUERTO", "1417") #Gijón
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "008", "COD_PUERTO", "1418") #Avilés
records <- correct_levels_in_variable(records, "COD_ORIGEN", "065", "038", "COD_PUERTO", "1418") #Avilés Subáreas VI,VII, Divisiones VIIIabd
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "008", "COD_PUERTO", "1420") #Luarca
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "010", "COD_PUERTO", "0913") #Finisterre
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "010", "COD_PUERTO", "0914") #Muros
records <- correct_levels_in_variable(records, "COD_ORIGEN", "003", "010", "COD_PUERTO", "0922") #Vigo
records <- correct_levels_in_variable(records, "COD_ORIGEN", "065", "038", "COD_PUERTO", "0926") #Cillero

check_procedencia <- check_variable_with_master("PROCEDENCIA")

check_tipo_muestreo <- check_variable_with_master("COD_TIPO_MUE")





