
#' Check code: 1086
#' Check coherence between ESTRATO_RIM and the origin using 
#' metier_coherence dataset from sapmuebase.
#' @param df Dataframe where the variable to check is.
#' @return dataframe with errors

checkMetierCoherence <- function(df){
  
  # Select work columns
  
  df <- df[, c("FECHA", "COD_PUERTO", "COD_BARCO","COD_ARTE",
                         "COD_ORIGEN", "ESTRATO_RIM")]
  
  # Import master dataset: metier_coherence
  
  metier_coherence_master <- sapmuebase::metier_coherence
  
  metier_coherence_master <- unique(metier_coherence_master[, c("COD_ORIGEN", 
                                                                "ESTRATO_RIM")])
  # Add column to test merge
  
  metier_coherence_master[, "TEST"] <- "T"
  
  # Merge both dataframes
  
  df <- merge(df, 
              metier_coherence_master,
              all.x = TRUE)
  
  df <- records[is.na(df$TEST), ] 
  
  df <- unique(df)
  
  if(nrow(df) > 0){
    
    df$TEST <- NULL
    
    return(df)
    
  }
  
}