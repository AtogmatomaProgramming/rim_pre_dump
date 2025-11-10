
#' Function to move a file from one directory to other
#' #' @param origin_folder folder where the file is stored originally
#' @param destiny_folder folder where the file will be stored
#' @param file_name name of the file that will be moved
#' @param extension extension of the file
#' @returns a message which notifies if the file where 
#' stored in the new place

move_file <- function(file_name,
                      origin_folder, 
                      destiny_folder){
  
  
  tryCatch({
    
    file.rename(paste0(origin_folder, 
                       "/", 
                       file_name), 
                paste0(destiny_folder, 
                       "/", 
                       file_name))
    
    message(paste0("File '", file_name, "' moved to '", destiny_folder, "' correctly."))
    
  }, error = function(e){
    
    cat("An error occurred:", conditionMessage(e), "\n")
    
  }, warning = function(w){
    
    cat("A warning occurred:", conditionMessage(w), "\n")
    
  })
  
}