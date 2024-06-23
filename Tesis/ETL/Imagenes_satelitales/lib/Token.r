require(futile.logger)
require(glue)
require(httr)
require(R6)

Token <- R6Class("Token",
  private = list(
    authUrl = NULL,
    clientId = NULL,
    clientSecret = NULL,
    currentToken = NULL,
    script = NULL,
    maxRetries = NULL
  ),
  public = list(
    initialize = function(authUrl, clientId, clientSecret, script, maxRetries = 3) {
      private$authUrl <- authUrl
      private$clientId <- clientId
      private$clientSecret <- clientSecret
      private$script <- script
      private$maxRetries <- maxRetries
    },
    
    authorizationHeader = function() {
      token <- self$get()
      if (! is.null(token)) {
        return(glue::glue("{token$token_type} {token$access_token}"))
      }
      return(NULL)
    },
    
    expired = function(timestamp) {
      if (! is.null(private$currentToken)) {
        age <- as.integer(difftime(timestamp, private$currentToken$timestamp, units = "secs"))
        return(age >= (private$currentToken$expires_in - 60)) # 1 minuto menos para asegurar
      } else {
        return(TRUE)
      }
    },
    
    get = function() {
      timestamp <- Sys.time()
      newToken  <- self$expired(timestamp)
      if (newToken) {
        private$currentToken <- self$aquireNew(timestamp)
      }
      
      return(private$currentToken)
    },
    
    aquireNew = function(timestamp) {
      nTry        <- 0
      token       <- NULL
      credentials <- list(
        "client_id" = private$clientId,
        "client_secret" = private$clientSecret,
        "grant_type" = "client_credentials"
      )
      while ((nTry < private$maxRetries) && is.null(token)) {
        tryCatch({
          access_token_response = httr::POST(
            url = private$authUrl,
            body = credentials,
            encode = "form"
          )   
          if (httr::http_error(access_token_response)) {
            script$error(glue::glue("Error al obtener el token: {httr::message_for_status(access_token_response)}"))
          } else {
            token <- httr::content(access_token_response)
            token$timestamp <- timestamp
          }
        }, error = function(e) {
          script$error(glue::glue("Error al obtener el token: {e$error}"))
        })
        
        nTry <- nTry + 1
      }
      
      return(token)
    },
    
    token = function() {
      return(private$currentToken)
    }
  )
)