#### IPDtoSIRENO
#### Script to check the monthly data dump from IPD database previous to SIRENO upload
#### 
#### Return csv files with errors detected
####
#### author: Marco A. Amez Fernandez
#### email: ieo.marco.a.amez@gmail.com
#### date of last modification: 1/7//2016
#### version: 0.1
####
#### files required:


# #### CONFIG ##################################################################

# ---- PACKAGES ----------------------------------------------------------------


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

# #### FUNCTIONS ###############################################################

#function to save the errors in csv files:
export_errors_lapply<-function(x, errors){
  if(nrow(errors[[x]])!= 0){
    fullpath<-paste(PATH_ERRORS, "/", YEAR, "_", MONTH, "_errors_", x, ".csv", sep="")
    write.csv(errors[[x]], file=fullpath, row.names = FALSE, quote = FALSE)
    MESSAGE_ERRORS[[x]]<<-"has errors" #this not recomended in R but is the only way I know
    print(x)
  } else {
    fullpath<-paste(PATH_ERRORS, "/", YEAR, "_", MONTH, "_no_errors_", x, ".csv", sep="")
    write.csv(errors[[x]], file=fullpath, row.names = FALSE, quote = FALSE)
    MESSAGE_ERRORS[[x]]<<-"errors free" #this not recomended in R but is the only way I know
    print(paste('Great,', x, 'is error free!'))
  }
}


#function to import and clean the IPD file
##filename: name to import
##export = "TRUE" the file is export to csc
importIPDFile <- function(){
  
  fullpath<-paste(PATH_FILENAME, FILENAME, sep="/")
  
  #read the file
  records <- read.fwf(
    file="data/muestreos_especie_1_2016_todos.txt",
    widths=c(10, 4, 6, 3, 3, 21, 10, 20, 1, 10, 8,  7, 7, 4, 20, 5, 20, 10, 4, 20, 5, 1, 20, 20, 10, 10, 10)
  )
  
  colnames(records) <- c("fecha_muestreo", "cod_puerto", "cod_buque", "cod_arte",
                      "cod_origen", "metier", "cod_proyecto", "tipo_muestreo",
                      "numero_rechazados", "numero_barcos_muestreados", "cod_cuadricula",
                      "latitud_decimal", "longitud_decimal", "dias_mar", "peso_total",
                      "cod_especie_tax", "tipo_muestreo2", "procedencia", "cod_categoria",
                      "peso", "cod_especie_muestreada", "sexo", "peso_muestra", "medida",
                      "talla", "numero_ejemplares", "cod_puerto_descarga")
  
  #select only samples from ICES
  records <- records[records$"cod_proyecto"==1101001,]
  
  #format fecha_muestreo
  records$fecha_muestreo <- as.POSIXlt(records$fecha_muestreo, format="%d/%m/%Y")

  #return data
  if (MONTH != ""){
    return(subset(records, fecha_muestreo$mon == MONTH-1))
  } else {
    return(records)
  }
}

records <- importIPDFile()
