#' Check if the value of variables are consistent whit the SIRENO masters.
#' It's only available for variables with a data source (master): ESTRATO_RIM, COD_PUERTO,
#' COD_ORIGEN, COD_ARTE, COD_PROCEDENCIA and TIPO_MUESTREO
#' @param variable: one of this variables: ESTRATO_RIM, COD_PUERTO, COD_ORIGEN,
#' COD_ARTE or COD_PROCEDENCIA
#' @param df: dataframe to check
#' @return Return a dataframe with samples containing erroneous variables
checkVariableWithMaster <- function (variable, df){

  valid_variables = c("ESTRATO_RIM","COD_PUERTO","COD_ORIGEN","COD_ARTE","PROCEDENCIA")
  if (!(variable %in% valid_variables)) {
    stop(paste("This function is not available for ", variable))
  }

  # look if the variable begin with "COD_". In this case, the name of the data source
  # is the name of the variable without "COD_"
  data_source_name <- variable
  if (grepl("^COD_", data_source_name)){
    data_source_name <- strsplit(data_source_name, "COD_")
    data_source_name <- data_source_name[[1]][2]
  }
  data_source_name <- tolower(data_source_name)

  errors <- anti_join(x = df, y = get(data_source_name))

  #prepare to return
  fields_to_filter <- c("COD_PUERTO", "FECHA", "COD_BARCO", variable)

  if(nrow(errors)>0){

    errors <- errors[, fields_to_filter]
    errors <- unique(errors)
    errors <- errors[with(errors,order(fields_to_filter)),]
    errors <-

    #return
    return(errors)
  } else {
    return(data.frame("no error" = NULL ))
  }

}


#' Remove MT1 trips with foreign vessels.
#' @param df: dataframe
#' @return Dataframe without the deleted trips
remove_MT1_trips_foreing_vessels <- function(df){

  #obtain MT1 trips with foreign vessels
  mt1_foreing <- df %>%
    filter( as.integer(as.character(COD_BARCO)) >= 800000 & COD_TIPO_MUE == "MT1A")

  #remove trips
  df <- df %>%
    #ATENTION to the ! and ():
    filter( !(as.integer(as.character(COD_BARCO)) >= 800000 & COD_TIPO_MUE == "MT1A"))

  # return
  return(df)
}


#' TODO: remove this function, don't have any sense because in the import process
#' the month is selected
#' Check if all the data in the dataframe belongs to the same month, allocated in
#' MONTH variable
#' @param df: Dataframe to check
#' @return Dataframe with the samples of incorrect month
check_month <- function(df){
  df$months <- sapply (as.Date(df[["FECHA"]], "%d/%m/%Y"), function(x){format(x, "%m")})
  erroneus_months <- as.data.frame(unique(df$months))
  colnames(erroneus_months) <- c("FECHA")
  erroneus_months <- erroneus_months %>%
    filter(FECHA != MONTH_AS_CHARACTER)
  erroneus_samples <- merge(x = df, y = erroneus_months, by.x = "months", by.y = "FECHA", all.y = TRUE)
  return(erroneus_samples)
}


#' Check the type of sample.
#' @param df: dataframe to check
#' @return Dataframe with samples which ESTRATEGIA != to "CONCURRENTE EN LONJA",
#' except VORACERA_GC which must be "EN BASE A ESPECIE"
check_strategy <- function(df){

  errors_not_voracera <- records[ which(records[["ESTRATO_RIM"]] != "VORACERA_GC"
                & records[["ESTRATEGIA"]] != "CONCURRENTE EN LONJA"), ]

  errors_voracera <- records[ which(records[["ESTRATO_RIM"]] == "VORACERA_GC"
                              & records[["ESTRATEGIA"]] != "EN BASE A ESPECIE"), ]

  errors <- rbind(errors_not_voracera, errors_voracera)

  if (nrow(errors)>0) {

    errors <- errors[,c("FECHA", "COD_PUERTO", "COD_BARCO", "ESTRATO_RIM",
                        "COD_TIPO_MUE", "ESTRATEGIA")]
    errors["error"] <- "Todos los muestreos tienen que ser CONCURRENTE EN LONJA,
                      excepto VORACERA_GC que ha de ser EN BASE A ESPECIE"

    return(errors)

  } else {

    return(errors)

  }


}


#' Search duplicate samples by type of sample (between MT1 and MT2)
#' @param df: dataframe where find duplicate samples
#' @return Dataframe with duplicate samples
check_duplicates_type_sample <- function(df){
  mt1 <- df[df["COD_TIPO_MUE"]=="MT1A",c("COD_PUERTO","FECHA","COD_BARCO")]
  mt1 <- unique(mt1)
  mt2 <- df[df["COD_TIPO_MUE"]=="MT2A",c("COD_PUERTO","FECHA","COD_BARCO")]
  mt2 <- unique(mt2)

  duplicated <- merge(x = mt1, y = mt2)

  return(duplicated)
}

#' Search false mt2 samples: samples with COD_TIPO_MUE as MT2A and without any
#' length.
#' @param df: dataframe to check.
#' @return Dataframe with erroneous samples
check_false_mt2 <- function(df){
  dataframe <- df
  mt2_errors <- dataframe %>%
    filter(COD_TIPO_MUE=="MT2A") %>%
    group_by(COD_PUERTO, FECHA, COD_BARCO, ESTRATO_RIM) %>%
    summarise(summatory = sum(EJEM_MEDIDOS)) %>%
    filter(summatory == 0)

  return(mt2_errors)
}

#' Search false mt1 samples: samples with COD_TIPO_MUE as MT1A and lengths
#' @param df: dataframe to check.
#' @return Dataframe with erroneous samples
check_false_mt1 <- function(df){
  dataframe <- df
  mt1_errors <- dataframe %>%
    filter(COD_TIPO_MUE=="MT1A") %>%
    group_by(COD_PUERTO, FECHA, COD_BARCO, ESTRATO_RIM) %>%
    summarise(summatory = sum(EJEM_MEDIDOS)) %>%
    filter(summatory != 0)

  return(mt1_errors)
}

#' Search foreign ships
#' The BAR_COD code in the foreign ships begins with an 8 followed by 5 digits.
#' @param df: dataframe
#' @return Dataframe with foreign ships and COD_TIPO_MUE
check_foreing_ship <- function(df){
  dataframe <- df
  dataframe$COD_BARCO <- as.character(dataframe$COD_BARCO)
  # ships <- dataframe %>%
  #   filter(grepl("^8\\d{5}",COD_BARCO)) %>%
  #   group_by(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM) %>%
  #   count(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM)

  ships <- dataframe[grepl("^8\\d{5}", dataframe$COD_BARCO), ]

  if(nrow(ships) != 0){

    ships <- ships %>%
      group_by(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM) %>%
      count(FECHA, COD_TIPO_MUE, COD_BARCO, COD_PUERTO, COD_ARTE, COD_ORIGEN, ESTRATO_RIM)

    return(ships)

  }


  # if(nrow(ships) == 0) {
  #   print("There aren't foreing ships.")
  # } else if(ships$COD_TIPO_MUE != 1) {
  #   warning("there are some MT2A with foreings ship!!!")
  # }
  #
  # return(ships[, c("FECHA", "COD_TIPO_MUE", "COD_BARCO", "COD_PUERTO", "COD_ARTE", "COD_ORIGEN", "ESTRATO_RIM")])

}

# TODO: function to search ships not active
# ships <- as.data.frame(unique(records[,c("COD_BARCO" )]))
# colnames(ships) <- "COD_BARCO"
# ships_sireno <- merge(x=ships, y=maestro_flota_sireno, by.x = "COD_BARCO", by.y = "BARCOD", all.x = TRUE)

#' Check mixed species saved as non mixed species: in COD_ESP_MUE
#' there are codes from mixed species
#' @param df: dataframe to check
#' @return  Dataframe with the samples with species saved as non mixed species.
errorsMixedSpeciesAsNotMixed <- function(df){
  non_mixed <- merge(x=df, y=especies_mezcla["COD_ESP_CAT"], by.x = "COD_ESP_MUE", by.y = "COD_ESP_CAT")
  return(non_mixed)
}

#' Check if the categories in the IPD file are in the categories master of the
#' SIRENO
#' @param df: dataframe to check
#' @return  Data frame of samples with erroneous categories
check_categories <- function(df){

  categorias[["CONTROL"]] <- "OK"
  #errors <- merge(x = df, y = maestro_categorias, by.x = c("COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA"), by.y = c("COD_PUERTO", "COD_ESP", "COD_CATEGORIA"), all.x = TRUE)
  errors <- merge(x = df, y = categorias, by.x = c("COD_PUERTO", "COD_ESP_MUE", "COD_CATEGORIA"), by.y = c("COD_PUERTO", "COD_ESP", "COD_CATEGORIA"), all.x = TRUE)
  errors <- errors %>%
    filter(is.na(CONTROL)) %>%
    select(COD_PUERTO, COD_ESP_MUE, COD_CATEGORIA) %>%
    arrange(COD_PUERTO, COD_ESP_MUE, COD_CATEGORIA)
  errors <- unique(errors)


  return (errors)
}

#' Check if any length has the EJEM_MEDIDOS as NA.
#' @param df: data frame to check
#' @return data frame with errors
check_measured_individuals_na <- function(df){
  errors <- df %>%
    filter(is.na(EJEM_MEDIDOS))

  return (errors)
}

#' Check if one category has two or more different P_MUE_DES.
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

  # df_filtrado <- df %>%
  #   distinct() %>%
  #   group_by(across(all_of(fields_to_count)))%>%
  #   count() %>%
  #   filter(n>1)

  df <- unique(df)
  fmla <- as.formula(paste("P_MUE_DESEM ~", paste(fields_to_count, collapse = " + ")))
  err <- aggregate(fmla, data = df, length)
  err <- err[err[["P_MUE_DESEM"]] > 1, ]
  names(err)[names(err) == "P_MUE_DESEM"] <-  "number_P_MUE_DESEM"
  return(err)

}

#' Export file to excel.
#' @param df: data frame to export
#' @path: path to save the file
#' @note If this error is returned:
#'    Error: zipping up workbook failed. Please make sure Rtools is installed or a zip application is available to R.
#'    Try installr::install.rtools() on Windows.
#' run:Sys.setenv("R_ZIPCMD" = "C:/Rtools/bin/zip.exe") ## path to zip.exe
#' source: https://github.com/awalker89/openxlsx/issues/111
#' @return Excel file in the path defined in the GLOBAL VARIABLES section
#
export_to_excel <- function(df, path){
  month_in_spanish <- c("enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre")

  filename = paste("MUESTREOS_IPD_", month_in_spanish[as.integer(MONTH_AS_CHARACTER)], "_", YEAR, "_ICES.xlsx", sep="")
  filepath = paste(path, filename, sep = "/")

  colnames(df) <- c("FECHA","PUERTO","BUQUE","ARTE","ORIGEN","METIER","PROYECTO",
                    "TIPO MUESTREO","NRECHAZOS","NBARCOS MUESTREADOS","CUADRICULA",
                    "LAT DECIMAL","LON DECIMAL","DIAS_MAR","PESO_TOTAL","COD_ESP_TAX",
                    "ESTRATEGIA","PROCEDENCIA","COD_CATEGORIA","PESO","COD_ESP_MUE",
                    "SEXO","PESO MUESTRA","MEDIDA","TALLA","NEJEMPLARES","COD_PUERTO_DESCARGA",
                    "FECHA_DESEM", "OBSERVACIONES", "COD_MUESTREADOR", "COD_PAIS")

  df[["FECHA"]] <- as.character(df[["FECHA"]]) #it's mandatory convert date to character. I don't know why.
  df[["FECHA_DESEM"]] <- as.character(df[["FECHA_DESEM"]])
  write.xlsx(df, filepath, keepNA=TRUE, colnames=TRUE)
}


#' Change the content of variable MEDIDA to "T" ("Tallas", lenghts).
#' All the data are lengths samples so this variable can't be "P" ("Pesos", weights)
#' or empty.
#' @param df: data frame to modify
#' @return Return a data frame with the MEDIDA variable fixed
fix_medida_variable <- function (df) {

  if ("MEDIDA" %in% colnames(df)){
    df[["MEDIDA"]] <- "T"

    return(df)

  } else {
    stop(paste0("TALL.PESO doesn't exists in ", substitute(df)))
  }

}

#' Check code: 1064
#' Check variable with prescriptions dataset. Use the
#' prescripciones_rim_mt2_coherencia dataset from sapmuebase.
#' @param df Dataframe where the variable to check is.
#' @param variable Variable to check as character. Allowed variables:
#' ESTRATO_RIM, COD_PUERTO, COD_ORIGEN, COD_ARTE, METIER_DCF and CALADERO_DCF.
#' @return dataframe with errors
checkVariableWithPrescriptions <- function(df, variable) {

  valid_variables = c("ESTRATO_RIM","COD_PUERTO","COD_ORIGEN","COD_ARTE",
                      "METIER_DCF", "CALADERO_DCF")
  if (!(variable %in% valid_variables)) {
    stop(paste("This function is not available for variable", variable))
  }

  allowed <- sapmuebase::prescripciones_rim_mt2_coherencia[,variable]

  df <- df[!(df[[variable]] %in% allowed), ]


  fields <- BASE_FIELDS

  if (!(variable %in% BASE_FIELDS)) {
    fields <- c(BASE_FIELDS, variable)
  }

  df <- df[, fields]

  df <- unique(df)

  return(df)

}

#' Check code: 1067
#' Check if the code type sample is different of MT1A or MT2A
#' @return dataframe with errors
checkCodeTypeSample <- function(df){
  errors <- df[!(df[["COD_TIPO_MUE"]] %in% c("MT1A", "MT2A")), ]
  if(nrow(errors) > 0){
    return(errors)
  } else {
    return(data.frame("no_error" = NULL))
  }
}

#' Check code: 1068
#' Check if the variables ESTRATO_RIM, COD_PUERTO, COD_ORIGEN and
#' COD_ARTE are coherent with MT2 rim prescriptions.
#' @return dataframe with errors.
coherencePrescriptionsRimMt2 <- function(df){

  df <- df[df[["COD_TIPO_MUE"]]=="MT2A", ]

  fields <- c("COD_PUERTO", "COD_ARTE", "COD_ORIGEN", "ESTRATO_RIM", "FECHA", "COD_BARCO", "COD_TIPO_MUE")

  errors <- unique(df[, fields])
  errors <- merge(errors,
                  sapmuebase::prescripciones_rim_mt2_coherencia,
                  by=c("COD_PUERTO", "COD_ARTE", "COD_ORIGEN", "ESTRATO_RIM"),
                  all.x = TRUE)
  if(nrow(errors)>0){
    # errors <- humanize(errors)
    errors <- errors[is.na(errors[["PESQUERIA"]]), c(fields)]
  }

}


#' Check code: 1072
#' Check if the dni of the sampler is in SIRENO database
#' @return dataframe with errors, if there are any.
checkDni <- function(df){
  #TODO: detect if doesn't exists the dni_rim.csv file, just in case

  dni_rim <- importCsvSAPMUE("./private/dni_rim.csv")
  dni_rim <- gsub("[a-zA-Z]+", "", dni_rim[,"nif"])

  err <- unique(df[,"COD_MUESTREADOR"])

  err <- data.frame("DNI" = err[!(err %in% dni_rim)])

  return(err)

}

#' Check code: 1073
#' Check if a SHIP / DATE combination have different port, gear, origin, rim
#' stratum, project code or type of sample.
#' Require records dataframe
checkShipDate <- function(){
  err <- records[, c("COD_BARCO", "FECHA", "COD_PUERTO", "COD_ARTE",
                     "COD_ORIGEN", "ESTRATO_RIM", "COD_PROYECTO",
                     "COD_TIPO_MUE")] %>%
    unique()%>%
    group_by(COD_BARCO, FECHA) %>%
    mutate(dups = n()>1) %>%
    filter(dups == TRUE)
}

#' Check code: 1074
#' function to check mixed species in species of the category: in COD_ESP_CAT
#' there are codes of mixed species.
#' @param df dataframe
#' @return dataframe with the samples with species saved as non mixed species
#' @note Usually this errors aren't fixed previously to the dump process. Only
#' when the errors are numerous, the data is fixed or returned to the contracted
#' company to fix it.
errorsMixedSpeciesInCategory <- function(df){
  mixed_sp <- unique(especies_mezcla$COD_ESP_MUE)
  err <- df[df[["COD_TIPO_MUE"]]=="MT2A", ]
  err <- err[err[["TALLA"]]!= 0, ]
  err <- err[err[["COD_ESP_CAT"]] %in% mixed_sp, ]
  err <- humanize(err)
  err <- unique(err[, c("FECHA", "PUERTO", "COD_TIPO_MUE", "ESP_MUE",
                        "COD_ESP_MUE", "COD_CATEGORIA", "ESP_CAT",
                        "COD_ESP_CAT")])
}

#' Check code: 1075
#' function to check mixed species saved as no-mixed species in species sampled:
#' in COD_ESP_MUE there are species codes from species instead of its grouped
#' taxon.
#' @param df dataframe.
#' @return dataframe with the samples with species saved as non mixed species.
#' @note Usually this errors aren't fixed previously to the dump process. Only
#' when the errors are numerous, the data is fixed or returned to the contracted
#' company to fix it.
errorsNoMixedSpeciesInSample <- function(df){
  mixed_sp <- unique(especies_mezcla$COD_ESP_CAT)
  err <- df[df[["COD_TIPO_MUE"]]=="MT2A", ]
  err <- df[df[["COD_ESP_MUE"]] %in% mixed_sp, ]
  err <- humanize(err)
}

#' Check code: 1083
#' Check variable with prescriptions data set. Use the metier_coherence data set
#' from sapmuebase.
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


#' Create identifier of the month/months, with suffix. Used to create the filenames
#' and folders.
#' @param month month or months used.
#' @param year year used.
#' @param month_as_character month as character.
#' @param suffix_multiple_months Suffix used when multiple months are used.
#' @param suffix Suffix used at the end of the file name. Usefull to have multiple error
#' detections of the same month or year.
createIdentifier <- function(month,
                             year,
                             month_as_character,
                             suffix_multiple_months,
                             suffix){

  suffix_complete <-  ""

  if(suffix != ""){
    suffix_complete <- paste0("_", suffix)
  }

  if (length(month) == 1 && month %in% seq(1:12)) {
    return(paste0(year, "_", month_as_character, suffix_complete))
  } else if (length(month) > 1 & all(month %in% seq(1:12))) {
    return(paste0(year, "_", suffix_multiple_months, suffix_complete))
  }

}


#' Copy all the error files generated to a shared folder.
#' Used to copy errors files generated to the shared folder
copyFilesToFolder <- function (path_errors_from, path_errors_to){

  # test if path_errors_from exists
  ifelse(!file.exists(path_errors_from), stop(paste("Folder", path_errors_from, "does not exists.")), FALSE)

  # test if path_errors_from have files
  ifelse(length(list.files(path_errors_from))==0, stop(paste("Folder", path_errors_from, "doesn't have files.")), FALSE)

  # if the share errors directory does not exists, create it:
  ifelse(!dir.exists(path_errors_to), dir.create(path_errors_to), FALSE)

  # test if there are files with the same name in folder. In this case,
  # nothing is saved.
  files_list_to <- list.files(path_errors_to)

  files_list_from <- list.files(path_errors_from)

  if(any(files_list_from %in% files_list_to)){
    ae <- which(files_list_from %in% files_list_to)
    ae <- paste(files_list_from[ae], collapse = ", ")
    stop(paste("The file(s)", ae, "already exist(s). Nothing has been saved" ))

  }

  files_list_from <- file.path(path_errors_from, files_list_from)
  file.copy(from=files_list_from, to=path_errors_to)

}


#' Send errors files by email.
#' @param accesory_email_info: df with two variables: AREA_INF (with the values GC,
#' GS, GN and AC) and INTERNAL_LINK, with the link to the file.
#' @param contacts: contacts data frame.
#' @param credentials_file: file created with the function creds_file() from
#' blastula package. Stored in private folder.
#' @details
#' The internal_links data frame must have two variables:
#' - AREA_INF: influence área with the values GC, GS, GN and AC, of the
#' - LINK: with the link to the error file in its AREA_INF. If there
#' aren't any error file of a certain AREA_INF, the LINK must be set
#' to "" or NA.
#'
#' The contacts data frame contains the different roles of the personal and its
#' email to send them the error files. The roles are:
#' - GC, GS, GN and AC: the supervisors of the influence areas. In the email,
#' correspond to "to" field.
#' - sender: person responsible for sending the files. In the email correspond
#' to "from" field.
#' - cc: related people to whom the email should also be sent. In the email
#' correspond to "cc" field.
#' This data set is obtained from the file contacts.csv stored in private folder
#' due to the confidential information contained in it. The contacts.csv file
#' must have a comma separated format with two fields: ROLE and EMAIL. The first
#' line must contain the name of the variables.
#'
#' @require
sendErrorsByEmail <- function(accesory_email_info, contacts, credentials_file,
                              identification_sampling){

  apply(accesory_email_info, 1, function(x){

    if(x[["LINK"]] != ""){

      to <- contacts[contacts[["ROLE"]] == x[["AREA_INF"]] | contacts[["ROLE"]] == "sender", "EMAIL"]
      from <- contacts[contacts[["ROLE"]] == "sender", "EMAIL"]
      cc <- contacts[contacts[["ROLE"]] == "cc", "EMAIL"]

      subject = paste0(identification_sampling, " ",
                       x[["AREA_INF"]],
                       " -- errores muestreos RIM")

      rmd_email <- render_email(EMAIL_TEMPLATE)

      smtp_send(email = rmd_email,
                to = to,
                from = from,
                cc = cc,
                subject = subject,
                credentials = creds_file(file.path(PRIVATE_FOLDER_NAME, credentials_file))
      )

    } else {
      print(paste("The", x[["AREA_INF"]], "influence area hasn't any error"))
    }

  })

}


#' Create the backup and the error folders in the case that they do not exist
#' @param path_directory path for the directory that you need. In our case
#' the backup's or the error's one.
#' @returns a message which notifies if the directory
#' has been created or just already exists.
createDirectory <- function(path_directory){
  if(!file.exists(path_directory)){
    dir.create(path_directory)
    print("Directory has been created correctly")
  } else {
    print("Directory just already exists")
  }
}

