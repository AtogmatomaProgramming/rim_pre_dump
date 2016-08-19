#### IPDtoSIRENO
#### Script to check the monthly data dump from IPD database previous to SIRENO upload
#### 
#### Return csv files with errors detected
####
#### author: Marco A. Amez Fernandez
#### email: ieo.marco.a.amez@gmail.com
#### date of last modification: 22/7//2016
#### version: 0.1
####
#### files required: metiers.csv


# #### CONFIG ##################################################################

# ---- PACKAGES ----------------------------------------------------------------

library(dplyr) #arrange_()
library(devtools) # Need this package to use install_github
install_github("Eucrow/sapmuebase") # Make sure this is the last version
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
PATH_FILENAME <- "F:/misdoc/sap/IPDtoSIRENO/data/"
FILENAME <- "muestreos_especie_1_2016_todos.txt"
MONTH <- 1
YEAR <- "2016"
################################################################################

PATH_ERRORS <- paste(PATH_FILENAME,"/errors",sep="")

# #### IMPORT FILES ############################################################
metiers <- read.csv("metiers_nuevo.csv")
puertos <- read.csv("puerto_locode_sireno.csv")
divisiones <- read.csv("divisiones.csv")

# #### CONSTANS ################################################################

###list with the common fields used in the tables
BASE_FIELDS <- c("PUERTO", "FECHA", "BARCO", "UNIPESCOD", "TIPO_MUESTREO")

# #### FUNCTIONS ###############################################################


# ---- import file ---- #
records <- importIPDFile("data/muestreos_especie_4_2016_todos_.txt")

# ---- metiers ----#
levels(records$metier)
levels(records$tipo_muestreo)
errors_1_metiers <- merge(x = records, y= metiers, by.x = c("tipo_muestreo", "metier", "cod_puerto", "cod_origen"), by.y = c("tipo_muestreo", "UNIPESCOD", "COD_SIRENO", "COD_DIVISION_SIRENO"), all.x = TRUE)
errors_1_metiers <- unique(errors_1_metiers$metier)
nas <- subset(errors_1_metiers, is.na(errors_1_metiers$ARTE))
