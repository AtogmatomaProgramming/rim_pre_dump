
#' Function to create internal work folders in the case that they do
#' not exist
#' #' @param folder_name name of the folder that we need to create
#' @param base_path path of the base directory where we need to create the work
#' folders. By default, has empty value in the case you have the whole route. 
#' @returns a message which notifies if the directory
#' has been created or just already exists.

manage_work_folder <- function(folder_name,
                               base_path = ""){
  
  tryCatch({
    
    if(base_path != ""){
      
      folder_path <- file.path(base_path,
                               folder_name)
      
    } else {
      
      folder_path <- folder_name
      
    }
    
    if(dir.exists(folder_path)){
      message(paste0("Directory '", folder_name, "' already exists."))
    } else {
      dir.create(folder_path,
                 recursive = TRUE) # recursive = TRUE create all the folders
      # present in the final path
      message(paste0("Directory '", folder_name, "' has been correctly created."))
    }
    
    return(folder_path)
    
  }, error = function(e){
    
    cat("An error occurred:", conditionMessage(e), "\n")
    
  }, warning = function(w){
    
    cat("A warning occurred:", conditionMessage(w), "\n")
    
  }
  )
  
}


