#function to read the operation code. If not exist, operation code = 0
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

# ---- function to export tracking file ----------------------------------------
# create the file with the operation code
exportTrackingFile<- function(){
  operation_code <- read_operation_code()
  filename <- file_path_sans_ext(FILENAME)
  filename <- paste(filename, operation_code, sep="_")
  filename <- paste(PATH_DATA, filename, sep="/")
  filename <- paste(filename,  ".csv", sep="")
  write.csv(records, filename, quote = FALSE, row.names = FALSE)
}

# ---- function to create and/or update log file -------------------------------
#' Create and/or update file
#'
#' This function create (or update, if it's already exists) a log file with the
#' arguments sended.
#' @param action: the realized action. For example "Remove" or "Change"
#' @param df: dataframe
#' @param variable: variable name (column)
#' @param erroneus_data: the erroneus value to change
#' @param correct_data: the correct value
#' @param conditional_variables: a vector of characters with the name of the
#' conditional variables
#' @param conditions: a vector of characters with the conditional values, whith
#' the same lenght that conditional_variables
#'
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
#' Check if the value of variables are consistent whit the SIRENO masters.
#' It's only available for variables with a data source (master): ESTRATO_RIM, COD_PUERTO,
#' COD_ORIGEN, COD_ARTE, COD_PROCEDENCIA and TIPO_MUESTREO
#' @param variable: one of this variables: ESTRATO_RIM, COD_PUERTO, COD_ORIGEN,
#' COD_ARTE or COD_PROCEDENCIA
#' @return Return a dataframe with samples containing erroneus variables
check_variable_with_master <- function (variable, df){
  
  valid_variables = c("ESTRATO_RIM","COD_PUERTO","COD_ORIGEN","COD_ARTE","PROCEDENCIA")
  if (!(variable %in% valid_variables)) {
    stop(paste("This function is not available for ", variable))
  }
  
  # look if the variable begin with "COD_". In this case, the name of the data source
  # is the name of the variable without "COD_"
  if (grepl("^COD_", variable)){
    variable <- strsplit(variable, "COD_")
    variable <- variable[[1]][2]
  }
  name_data_set <- tolower(variable)
  
  errors <- anti_join(x = df, y = get(name_data_set))
  
  #prepare to return
  fields_to_filter <- c("COD_PUERTO", "FECHA", "COD_BARCO", "ESTRATO_RIM", "COD_ARTE", "COD_ORIGEN", "COD_TIPO_MUE", "PROCEDENCIA")
  
  
  errors <- errors %>%
    select(one_of(fields_to_filter))%>%
    arrange_(fields_to_filter)%>%
    unique()
  
  #return
  return(errors)
}


# ---- Change levels in a variable of a dataframe ------------------------------
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
    exportTrackingFile()
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
    
    
    # add to log file
    
    string_conditional_variables <- toString(conditional_variables)
    string_conditional_variables <- gsub(",","",string_conditional_variables)
    
    string_conditions <- toString(conditions)
    string_conditions <- gsub(",","",string_conditions)
    
    export_log_file("change variable", variable, erroneus_data, correct_data, string_conditional_variables, string_conditions)
    #export file
    exportTrackingFile()
    # return
    
    return(df)
    
    
  } else {
    stop("Some argument is missing.")
  }
}


# function to remove trip. Add record to Log file and export file.
# It's imperative this data to identify a trip:
# FECHA, COD_TIPO_MUESTRA, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN and ESTRATO_RIM
# df: dataframe
# return: dataframe without the deleted trips
remove_trip <- function(df, date, cod_type_sample, cod_ship, cod_port, cod_gear, cod_origin, rim_stratum){
  df <- df[!(df["FECHA"]==date & df["COD_TIPO_MUE"] == cod_type_sample & df["COD_BARCO"] == cod_ship & df["COD_PUERTO"] == cod_port & df["COD_ARTE"] == cod_gear & df["COD_ORIGEN"] == cod_origin & df["ESTRATO_RIM"] == rim_stratum),]
  
  # add to log file
  error_text <- paste(date, cod_type_sample, cod_ship, cod_port, cod_gear, cod_origin, rim_stratum, sep=" ")
  export_log_file("remove trip", "trip", error_text)
  #export file
  exportTrackingFile()
  # return
  return(df)
}


# function to remove MT1 trips with foreing vessels. Add record to Log file and export file.
# df: dataframe
# return: dataframe without the deleted trips
remove_MT1_trips_foreing_vessels <- function(df){
  
  #obtain MT1 trips with foreing vessels
  mt1_foreing <- df %>%
    filter( as.integer(as.character(COD_BARCO)) >= 800000 & COD_TIPO_MUE == "MT1A")
  
  #remove trips
  df <- df %>%
    #ATENTION to the ! and ():
    filter( !(as.integer(as.character(COD_BARCO)) >= 800000 & COD_TIPO_MUE == "MT1A"))
  
  
  # add to log file
  # concat all the variables of the dataframe
  r <- apply(mt1_foreing, 1, function(x){
    c <- paste0(x, collapse = " ")
    return(c)
  }
  )
  # apply the export_log_file to every element of the list
  lapply (r, function(x){
    export_log_file("remove trip", "trip", x)
  }
  )
  
  #export file
  exportTrackingFile()
  # return
  return(df)
}


# TODO: remove this function, don't have any sense because in the import proccess
# the month is selected
# function to check the month: Check if all the data in the dataframe belongs to
# the same month, allocated in MONTH variable
# df: dataframe to check
# return a dataframe with the samples of incorrect month
check_month <- function(df){
  df$months <- sapply (as.Date(df[["FECHA"]], "%d/%m/%Y"), function(x){format(x, "%m")})
  erroneus_months <- as.data.frame(unique(df$months))
  colnames(erroneus_months) <- c("FECHA")
  erroneus_months <- erroneus_months %>%
    filter(FECHA != MONTH_STRING)
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
  mt1 <- df[df["COD_TIPO_MUE"]=="MT1A",c("COD_PUERTO","FECHA","COD_BARCO")]
  mt1 <- unique(mt1)
  mt2 <- df[df["COD_TIPO_MUE"]=="MT2A",c("COD_PUERTO","FECHA","COD_BARCO")]
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
  dataframe$COD_BARCO <- as.character(dataframe$COD_BARCO)
  ships <- dataframe %>%
    filter(grepl("^8\\d{5}",COD_BARCO)) %>%
    group_by(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM) %>%
    count(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM)
  
  if(nrow(ships) == 0) {
    print("There aren't foreing ships.")
  } else if(ships$COD_TIPO_MUE != 1) {
    warning("there are some MT2A with foreings ship!!!")
  }
  
  return(ships[, c("FECHA", "COD_TIPO_MUE", "COD_BARCO", "COD_PUERTO", "COD_ARTE", "COD_ORIGEN", "ESTRATO_RIM")])
  
}

# TODO: function to search ships not active
# ships <- as.data.frame(unique(records[,c("COD_BARCO" )]))
# colnames(ships) <- "COD_BARCO"
# ships_sireno <- merge(x=ships, y=maestro_flota_sireno, by.x = "COD_BARCO", by.y = "BARCOD", all.x = TRUE)

# function to check mixed species saved as non mixed species: in COD_ESP_MUE
# there are codes from mixed species
# df: dataframe
# return a dataframe with the samples with species saved as non mixed species
check_mixed_as_no_mixed <- function(df){
  non_mixed <- merge(x=df, y=especies_mezcla["COD_ESP_CAT"], by.x = "COD_ESP_MUE", by.y = "COD_ESP_CAT")
  return(non_mixed)
}

# function to check no mixed species saved as mixed species: in COD_ESP_MUE
# there are codes from mixed species
# df: dataframe
# return a dataframe with the samples with species saved as non mixed species
check_no_mixed_as_mixed <- function(df){
  non_mixed <- merge(x=records, y=especies_no_mezcla["COD_ESP"], by.x = "COD_ESP_MUE", by.y = "COD_ESP")
  return(non_mixed)
}


# function to check if the categories in the IPD file are in the categories master of the SIRENO
# df: dataframe
# return: dataframe of samples with erroneus categories
check_categories <- function(df){
  
  maestro_categorias[["CONTROL"]] <- "OK"
  #errors <- merge(x = df, y = maestro_categorias, by.x = c("COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA"), by.y = c("COD_PUERTO", "COD_ESP", "COD_CATEGORIA"), all.x = TRUE)
  errors <- merge(x = df, y = maestro_categorias, by.x = c("COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA"), by.y = c("PUECOD", "ESPCOD", "ESPCAT"), all.x = TRUE)
  errors <- errors %>%
    filter(is.na(CONTROL)) %>%
    select(COD_PUERTO, FECHA, COD_BARCO, COD_ESP_MUE, COD_CATEGORIA) %>%
    arrange(COD_PUERTO, FECHA, COD_BARCO, COD_ESP_MUE, COD_CATEGORIA)
  errors <- unique(errors)
  
  
  return (errors)
}

# ---- function to check if any length has the EJEM_MEDIDOS as NA
#' function to check if any length has the EJEM_MEDIDOS as NA.
#' @param df: dataframe to check
#' @return dataframe with errors

check_measured_individuals_na <- function(df){
  errors <- df %>%
    filter(is.na(EJEM_MEDIDOS))
  
  return (errors)
}

# ---- function to check if one category has two or more different P_MUE_DES ---
#
#' function to check if one category has two or more different P_MUE_DES.
#' Mostly, this cases correspond to mixed species or sexed species, but in other
#' cases this can be an error in the keyed process by IPD:
#' - in some mixed species, one category (0901) contains two 'species
#' of the category'. For example Lophis piscatorius and L. budegassa, everyone
#' with its own 'landing weight'. In the saved process in SIRENO, only the first of the
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
one_category_with_different_landing_weights <- function(df){
  df <- df[,c(BASE_FIELDS, "COD_ESP_MUE", "COD_CATEGORIA", "P_MUE_DESEM")]
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
#
export_to_excel <- function(df){
  month_in_spanish <- c("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")
  
  filename = paste("MUESTREOS_IPD_", month_in_spanish[as.integer(MONTH_STRING)], "_", YEAR, ".xlsx", sep="")
  filepath = paste(PATH_DATA, filename, sep = "/")
  
  colnames(df) <- c("FECHA","PUERTO","BUQUE","ARTE","ORIGEN","METIER","PROYECTO",
                    "TIPO MUESTREO","NRECHAZOS","NBARCOS MUESTREADOS","CUADRICULA",
                    "LAT DECIMAL","LON DECIMAL","DIAS_MAR","PESO_TOTAL","COD_ESP_TAX",
                    "TIPO_MUESTREO","PROCEDENCIA","COD_CATEGORIA","PESO","COD_ESP_MUE",
                    "SEXO","PESO MUESTRA","MEDIDA","TALLA","NEJEMPLARES","COD_PUERTO_DESCARGA",
                    "FECHA_DESEM", "OBSERVACIONES", "COD_MUESTREADOR", "COD_PAIS")
  
  df[["FECHA"]] <- as.character(df[["FECHA"]]) #it's mandatory convert date to character. I don't know why.
  df[["FECHA_DESEM"]] <- as.character(df[["FECHA_DESEM"]])  
  write.xlsx(df, filepath, keepNA=TRUE, colnames=TRUE)
}


# ---- function to add country variable ----------------------------------------
#
#' Add variable country code
#
#' Add variable country code (COD_PAIS) at the end of the dataframe
#' @param df: dataframe to modify
#' @return Return a dataframe with the country code variable
#'
create_variable_code_country <- function(df){
  ships <- maestro_flota_sireno[,c("BARCOD", "PAICOD")]
  with_country <- merge(x = df, y = ships, by.x = "COD_BARCO", by.y = "BARCOD", all.x = TRUE)
  #the order of the variables has been changed in the merge (I think), so
  #we need to reorder:
  with_country <- with_country[,c(2,3,1,4:length(with_country))]
  names(with_country)[names(with_country) == "PAICOD"] <- "COD_PAIS"
  
  export_log_file("Add", "COD_PAIS")
  
  return (with_country)
}

# ---- function to fix MEDIDA variable -----------------------------------------
#
#' Change the content of variable MEDIDA to "T" ("Tallas", lenghts).
#' 
#' All the data are lenthts samples so this variable can't be "P" ("Pesos", weights)
#' or empty. 
#
#' @param df: dataframe to modify
#' @return Return a dataframe with the MEDIDA variable fixed
#'
fix_medida_variable <- function (df) {
  
  if ("MEDIDA" %in% colnames(df)){
    df[["MEDIDA"]] <- "T"
    
    export_log_file("change", "MEDIDA", "all rows", "T")
    
    return(df)
    
    
  } else {
    stop(paste0("TALL.PESO doesn't exists in ", substitute(df)))
  }
  
}

# ---- function to Change the 000000 COD_BARCO from VORACERA_GC ----------------
#' Change the 000000 COD_BARCO ('DESCONOCIDO' ship) from VORACERA_GC with 205509
#' ('DESCONOCIDO VORAZ LONJA')
#' 
#' @param df: dataframe to modify
#' @return Return a dataframe fixed
#' 
recode000000Ship <- function(df){
  
  if (nrow(df[df["COD_BARCO"]=="000000" & df["ESTRATO_RIM"]=="VORACERA_GC",])!=0){
    df[["COD_BARCO"]] <- as.character(df[["COD_BARCO"]])
    df[df["COD_BARCO"]=="000000" & df["ESTRATO_RIM"]=="VORACERA_GC",]["COD_BARCO"] <- "205509"
    df[["COD_BARCO"]] <- as.factor(df[["COD_BARCO"]])
    
    export_log_file("Change", "COD_BARCO", "000000", "205509", "ESTRATO_RIM", "VORACERA_GC")
    
  } else {
    warning("There aren't ships to recode")
  }
  
  return(df)
  
}
