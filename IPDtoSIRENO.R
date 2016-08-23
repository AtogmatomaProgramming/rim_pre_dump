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

library(dplyr) #arrange_()
library(plyr) #mapvalues(): replace items
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

LOG_FILE <- paste("LOG_", YEAR, "_", MONTH, ".csv", sep="")

PATH_ERRORS <- paste(PATH_FILE,"/errors",sep="")

# #### RECOVERY DATA SETS ######################################################

data(estrato_rim) #load the data set

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



# function to change the level in a variable of a dataframe:
# df: dataframe
# variable: variable (column)
# erroneus_data
# correct_data 
correct_level_in_variable <- function(df, variable, erroneus_data, correct_data) {
  df[[variable]] <- mapvalues(df[[variable]], c(erroneus_data), c(correct_data))
  export_log_file("change", variable, erroneus_data, correct_data)
  return(df)
}



# function to export file
exportFileLog <- function(df, log){
  filename <- file_path_sans_ext(basename(FILENAME))

  file = paste("samples_up", YEAR, MONTH, "LOG", log, sep="_")
  write.csv(df, file)
  exportLogFile (file)
}

# ---- import file ---- #
records <- importIPDFile(paste(PATH_DATA,FILENAME, sep="/"))


# ---- metiers ----#
levels(records$ESTRATO_RIM)
new_estrato_rim <- estrato_rim
new_estrato_rim$VALID <- "VALID"
estrato_rim_erroneus <- merge(x = records, y= new_estrato_rim, by.x = c("ESTRATO_RIM"), by.y = c("ESTRATO_RIM"), all.x = TRUE)
estrato_rim_erroneus <- subset(estrato_rim_erroneus, is.na(VALID))
estrato_rim_erroneus <- levels(droplevels(estrato_rim_erroneus$ESTRATO_RIM)) # estrato_rim erroneus

errors_estrato_rim <- records[records$ESTRATO_RIM==estrato_rim_erroneus,]

recordsppp <- correct_level_in_variable(records, "ESTRATO_RIM", "OTB_DEF", "BACA_CN")
levels(recordsppp$ESTRATO_RIM)







