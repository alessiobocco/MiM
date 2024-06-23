require(R6)
require(dplyr)
require(dbplyr)
require(RColorBrewer)
require(grDevices)

Facade <- R6Class("Facade",
	private = list(
	  con     = NULL,
	  filtrar = function(registros, filtros) {
	    for (col.name in names(filtros)) {
	      if (! is.null(filtros[[col.name]]) && (length(filtros[[col.name]]) > 0)) {
	        field.name <- rlang::sym(col.name)
	        if (length(filtros[[col.name]]) > 1) {
	          field.values <- filtros[[col.name]]
	          registros    <- registros %>% dplyr::filter(UQ(field.name) %in% field.values)
	        } else {
	          field.value <- filtros[[col.name]]
	          if (! is.na(field.value)) {
	            registros  <- registros %>% dplyr::filter(UQ(field.name) == field.value)
	          } else {
	            registros  <- registros %>% dplyr::filter(is.na(UQ(field.name)))
	          }
	        }
	      }
	    }
	    return (registros)
	  },
	  generarPaletaColores = function(cantidad, paleta) {
	    # Si hay menos de 3 colores, obtener esos 3 y me quedo con la cantidad necesaria
	    # Si hay mas de 12 colores (maxima cantidad que colores de Color Brewer),
	    # entonces generar mas colores con colorRampPalette
	    cantidad_ajustada <- min(max(cantidad, 3), 12)
	    colores_default   <- RColorBrewer::brewer.pal(n = cantidad_ajustada, name = paleta)
	    if (cantidad_ajustada > cantidad) {
	      # Quitar colores
	      colores_default <- colores_default[1:cantidad]
	    } else if (cantidad_ajustada < cantidad) {
	      # Agregar mas colores con colorRampPalette
	      colores_default <- grDevices::colorRampPalette(colores_default)(cantidad)
	    }
	    return(colores_default)
	  }
	),
	public = list(
	  getConnection = function() {
	    return (private$con)
	  },
	  setConnection = function(new.con) {
	    private$con <- new.con
	  },
	  executeSelectStatement = function(query, parameters) {
	    res.query <- DBI::dbSendStatement(private$con, query)
	    DBI::dbBind(res.query, parameters)
	    resultados <- DBI::dbFetch(res.query)
	    DBI::dbClearResult(res.query)
	    return (resultados)
	  },
	  executeStatement = function(query, parameters) {
	    res.query <- DBI::dbSendStatement(private$con, query)
	    DBI::dbBind(res.query, parameters)
	    DBI::dbClearResult(res.query)
	  }
	)
)
