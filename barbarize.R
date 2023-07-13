# ---- function to barbarize one variable
#' Barbarize variable
#
#' Add new column with the code of a descriptive variable ... I mean, of a
#' described column create a new one with its appropriate code.
#' It's available for variables with a data source: PUERTO, BARCO,
#' ARTE, ORIGEN, TIPO_MUE, ESP_MUE, ESP_CAT.
#' Before the barbarization, checks the correct format of the variable.
#'
#' @param df df wich contains the variable to humanize
#' @param variable one of this values: COD_PUERTO, COD_BARCO,
#' COD_ARTE, COD_ORIGEN, COD_TIPO_MUE, COD_ESP_MUE, COD_ESP_CAT
#' @return Return the df dataframe with the new variable added.
#' @export

# TO DO: Add CODSGPM. In maestro_flota_sireno dataset, one CODSGPM can be in various
# rows.
barbarizeVariable <- function(df, variable){
  
  #check if variable exists in the dataframe
  if(variable %in% colnames(df)){
    
    variable_data <- variables_to_humanize[variables_to_humanize[["HUMANIZED_VAR"]] %in% variable,]
    
    # check that there are just one record for the variable to humanize in variables_to_humanize dataset
    if (nrow(variable_data) > 1) {
      stop (paste("There is an error with the", variable, "variable.
                  Check if the variable exists in variables_to_humanize dataset or if
                  there are various records for", variable, "."))
    } else if (nrow(variable_data) == 0) {
      
      stop (paste(variable, " is not a variable available to humanize."))
      
    } else {
      
      # check the correct format of the variable
      result <- tryCatch({
        
        checkFormatVariable(df, variable)
        
      }, error = function(err){
        
        stop(err)
        
      }
      )
      
      # original_var is the variable argument in the function, but is extracted here
      # from variable_data for a better understanding
      original_var <- as.character(variable_data[["ORIGINAL_VAR"]])
      barbarized_var <- as.character(variable_data[["BARBARIZED_VAR"]])
      humanized_master_var <- as.character(variable_data[["HUMANIZED_MASTER_VAR"]])
      humanized_var <- as.character(variable_data[["HUMANIZED_VAR"]])
      master <- as.character(variable_data[["MASTER"]])
      
      # this is the dataset to join with the dataframe to humanize
      dataframe_to_join_with <- get(master)[, c(barbarized_var, humanized_master_var)]
      # dataframe_to_join_with[[humanized_master_var]] <- toupper(dataframe_to_join_with[[humanized_master_var]])
      
      
      # vector of with original column names (useful to reorder columns after merge)
      column_names_df <- colnames(df)
      
      if (original_var %in% column_names_df){
        
        warning(paste(original_var, "already exists"))
        return(df)
        
      } else {
        
        df <- merge(df, dataframe_to_join_with, by.x = humanized_var, by.y = humanized_master_var, all.x = TRUE )

        # order variables, the same order than original dataframe but with the
        # barbarize variable just after the original variable.
        index_hum_var <- which(column_names_df %in% humanized_var)
        index_bar_var <- which(colnames(df) %in% barbarized_var)
        
        if (index_hum_var == length(column_names_df)){
          new_column_names_df <- c(column_names_df[1:(index_hum_var)], barbarized_var)          
        } else {
          new_column_names_df <- c(column_names_df[1:(index_hum_var)], barbarized_var, column_names_df[(index_hum_var+1):length(column_names_df)])
        }
        
        
        df <- df[, new_column_names_df]

        
        return (df)
      }
      
    }
  } else {
    
    stop(paste("Variable", variable, "does not exists in dataframe"))
    
  }
}

library(sapmuebase)
library(dplyr)

# change MASTER_VAR to BARBARIZED_VAR in variables_to_humanize dataset.
# TO DO: change in sapmuebase and in humanizeVariable() function.
# TO DO: use the same method in humanizeVariable() to order the columns.
# names(variables_to_humanize)[names(variables_to_humanize) == "MASTER_VAR"]<- "BARBARIZED_VAR"


# pres_RIM <- barbarizeVariable(pres_RIM, "PUERTO")




