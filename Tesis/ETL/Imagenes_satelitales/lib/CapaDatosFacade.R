require(R6)
require(dplyr)
require(dbplyr)
require(magrittr)
require(raster)
require(sf)

CapaDatosFacade <- R6Class("CapaDatosFacade",
	inherit = Facade,
	private = list(
	  campo.facade = NULL
	),
	public = list(
	  initialize = function(con, campo.facade) {
	    private$con          <- con
	    private$campo.facade <- campo.facade
	  },
	  
		buscar = function(campo, id = NULL, nombre = NULL, cultivo = NULL, campana = NULL, 
		                  fecha = NULL, variable = NULL, fecha.desde = NULL, fecha.hasta = NULL,
		                  con.datos = TRUE) {
		  # 1. Buscar metadatos
		  filtros <- list(campo_id = campo$id, id = id, nombre = nombre, cultivo = cultivo, 
		                  fecha = as.character(fecha), campana = campana)
		  capas   <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'capa_datos')), filtros)
		  
		  # 1bis. Filtrar por fecha desde y fecha hasta si corresponde
		  if (! is.null(fecha.desde)) {
		    capas %<>% dplyr::filter(fecha >= fecha.desde)
		  }
		  if (! is.null(fecha.hasta)) {
		    capas %<>% dplyr::filter(fecha <= fecha.hasta)
		  }
		  
		  # 2. Datos
		  if (con.datos) {
		    filtros.datos <- list(variable = variable)
		    datos         <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'capa_datos_variable')), filtros.datos) %>%
  		    dplyr::inner_join(capas, by = c("capa_dato_id" = "id")) %>%
  		    dplyr::select(nombre, cultivo, campana, fecha, celda_id, variable, valor) %>%
  		    dplyr::collect()
  		  return (datos)
		  } else {
		    return (dplyr::collect(capas))
		  }
		},
		
		buscarVariablesCapaDatos = function(id) {
		  variables <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'capa_datos_variable')) %>%
		    dplyr::filter(capa_dato_id == id) %>%
		    dplyr::distinct(variable) %>%
		    dplyr::arrange(variable) %>%
		    dplyr::collect() %>%
		    dplyr::pull(variable)
		  return (variables)
		},
		
		borrar = function(capa_dato_id) {
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    result.delete <- DBI::dbSendStatement(conn = private$con, statement = "DELETE FROM capa_datos WHERE id = $1")
		    DBI::dbBind(res = result.delete, params = list(capa_dato_id))
		    DBI::dbClearResult(res = result.delete)
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al borrar la capa de datos: [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (NULL)
		},
		
		obtenerColumnasImportables = function(shape.capa, inverso = FALSE) {
		  shape.capa.df        <- shape.capa %>%
		    sf::st_set_geometry(NULL)
		  columnas.importables <- purrr::keep(
		    .x = colnames(shape.capa.df),
		    .p = function(cname) {
		      field.name      <- rlang::sym(cname)
		      valores.columna <- dplyr::pull(shape.capa.df, UQ(field.name))  
		      incluir         <- all(is.numeric(valores.columna))
		      return (ifelse(! inverso, incluir, ! incluir))
		    }
		  )
		  return (columnas.importables)
		},
		
		insertar = function(campo, shape.capa, nombre, cultivo, campana, fecha, conversion.unidades = NULL, interpolar = TRUE) {
		  # a) Cambiar CRS de shape y obtener raster del campo
		  shape.capa    <- sf::st_transform(shape.capa, crs = campo$grilla_proj4string)
		  shape.capa.sp <- sf::as_Spatial(shape.capa)
		  raster.celdas <- private$campo.facade$celdas(campo, devolver.raster = TRUE)
		  celdas        <- private$campo.facade$celdas(campo, devolver.raster = FALSE)
		  
		  # b) Obtener las columnas que no representen geometria. 
		  #    Para cada una de ellas, rasterizar y transformar a Data Frame.
		  capa.datos.datos <- purrr::map_dfr(
		    .x = self$obtenerColumnasImportables(shape.capa),
		    .f = function(cname) {
		      # 1. Rasterizar datos originales a grilla regular definida originalmente
		      raster.final <- raster::rasterize(
		        x = shape.capa.sp,
		        y = raster.celdas,
		        field = shape.capa[[cname]],
		        fun = mean
		      ) %>% raster::mask(raster.celdas)
		      
		      # 2. Interpolar (si es aplicable) para eliminar posibles huecos utilizando malla de 3x3
		      if (interpolar) {
  		      fill.na <- function(x, i = 5) {
  		        if (is.na(x)[i]) {
  		          return (mean(x, na.rm = TRUE))
  		        } else {
  		          return (x[i])
  		        }
  		      }
  		      raster.interpolado <- focal(raster.final, w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
		      } else {
		        raster.interpolado <- raster.final
		      }
		      
		      # 3. Pasar a data frame
		      raster.df <- as.data.frame(x = raster::rasterToPoints(raster.interpolado),
		                                 stringsAsFactors = FALSE) %>%
		        dplyr::as_tibble() %>%
		        tidyr::gather(key = variable, value = valor, -x, -y) 
		      raster.df.completo <- raster.df %>%
		        dplyr::left_join(celdas, by = c("x", "y")) %>%
		        dplyr::rename(celda_id = id) %>%
		        dplyr::mutate(variable = cname) %>%
		        dplyr::filter(! is.na(campo_id) & ! is.na(valor)) %>%
		        dplyr::select(celda_id, variable, valor)
		      
		      # 4. Convertir unidades si es necesario
		      if (! is.null(conversion.unidades) && ! is.null(conversion.unidades[[cname]])) {
		        conversion.unidad           <- conversion.unidades[[cname]]
		        raster.df.completo$variable <- conversion.unidad$nueva.unidad
		        raster.df.completo$valor    <- raster.df.completo$valor * conversion.unidad$factor
		      }
		      
		      return (raster.df.completo)
		    }
	    ) %>% as.data.frame()
		  
		  # c) Ejecutar queries en forma transaccional
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # ii. Insertar metadatos
		    DBI::dbSendQuery(conn = private$con, statement = "LOCK TABLE capa_datos IN EXCLUSIVE MODE")
		    result.metadatos <- DBI::dbSendStatement(conn = private$con, statement = "INSERT INTO capa_datos(campo_id, nombre, cultivo, campana, fecha) VALUES($1, $2, $3, $4, $5)")
		    DBI::dbBind(res = result.metadatos, params = list(campo$id, nombre, cultivo, campana, fecha))
		    DBI::dbClearResult(res = result.metadatos)
		    
		    # iii. Buscar id de capa de datos
		    capa.datos.metadatos <- self$buscar(campo = campo, nombre = nombre, cultivo = cultivo, campana = campana, fecha = fecha, con.datos = FALSE)
		    if (nrow(capa.datos.metadatos) == 1) {
		      capa_dato_id <- capa.datos.metadatos %>%
		        dplyr::pull(id)
		    } else {
		      stop("Error al buscar los datos recien insertados.")
		    }
		    
		    # iv. Insertar
		    df.capa.datos.variable <- capa.datos.datos %>%
		      dplyr::mutate(capa_dato_id = capa_dato_id) %>%
		      dplyr::select(capa_dato_id, celda_id, variable, valor)
		    DBI::dbWriteTable(conn = private$con, name = "capa_datos_variable", append = TRUE, value = df.capa.datos.variable)
		    
		    # v. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    # vi. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al insertar la capa de datos ", campana, "-", cultivo, "-", nombre, "-", fecha, " para el campo ", campo$nombre, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (NULL)
		},
		
		insertarRaster = function(campo, raster.capa, nombre, cultivo, campana, variable, fecha) {
		  # a) Reproyectar raster
		  celdas            <- private$campo.facade$celdas(campo, devolver.raster = FALSE)
		  raster.celdas     <- private$campo.facade$celdas(campo, devolver.raster = TRUE)
		  raster.proyectado <- raster::projectRaster(from = raster.capa, to = raster.celdas)
		  
		  # b) Pasar a data frame
		  raster.df           <- as.data.frame(x = raster::rasterToPoints(raster.proyectado),
		                                       stringsAsFactors = FALSE)
		  colnames(raster.df) <- c("x", "y", "valor")
		  capa.datos          <- raster.df %>%
		    dplyr::left_join(celdas, by = c("x", "y")) %>%
		    dplyr::rename(celda_id = id) %>%
		    dplyr::filter(! is.na(campo_id) & ! is.na(valor)) %>%
		    dplyr::mutate(variable = variable) %>%
		    dplyr::select(celda_id, variable, valor)
		  rm(raster.df)
		  
		  # c) Ejecutar queries en forma transaccional
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # ii. Insertar metadatos
		    DBI::dbSendQuery(conn = private$con, statement = "LOCK TABLE capa_datos IN EXCLUSIVE MODE")
		    result.metadatos <- DBI::dbSendStatement(conn = private$con, statement = "INSERT INTO capa_datos(campo_id, nombre, cultivo, campana, fecha) VALUES($1, $2, $3, $4, $5)")
		    DBI::dbBind(res = result.metadatos, params = list(campo$id, nombre, cultivo, campana, fecha))
		    DBI::dbClearResult(res = result.metadatos)
		    
		    # iii. Buscar id de capa de datos
		    capa.datos.metadatos <- self$buscar(campo = campo, nombre = nombre, cultivo = cultivo, campana = campana, fecha = fecha, con.datos = FALSE)
		    if (nrow(capa.datos.metadatos) == 1) {
		      capa_dato_id <- capa.datos.metadatos %>%
		        dplyr::pull(id)
		    } else {
		      stop("Error al buscar los datos recien insertados.")
		    }
		    
		    # iv. Insertar
		    df.capa.datos.variable <- capa.datos %>%
		      dplyr::mutate(capa_dato_id = capa_dato_id) %>%
		      dplyr::select(capa_dato_id, celda_id, variable, valor)
		    DBI::dbWriteTable(conn = private$con, name = "capa_datos_variable", append = TRUE, value = df.capa.datos.variable)
		    
		    # v. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    # vi. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al insertar la capa de datos ", campana, "-", cultivo, "-", nombre, "-", fecha, " para el campo ", campo$nombre, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (NULL)
		},
		
		insertarRasterStack = function(campo, raster.stack, nombre, cultivo, campana, fecha) {
		  # a) Reproyectar raster
		  celdas            <- private$campo.facade$celdas(campo, devolver.raster = FALSE)
		  raster.celdas     <- private$campo.facade$celdas(campo, devolver.raster = TRUE)
		  raster.proyectado <- raster::projectRaster(from = raster.stack, to = raster.celdas)
		  
		  # b) Pasar a data frame
		  raster.df           <- as.data.frame(x = raster::rasterToPoints(raster.proyectado),
		                                       stringsAsFactors = FALSE)
		  capa.datos          <- raster.df %>%
		    tidyr::pivot_longer(cols = names(raster.proyectado), names_to = "variable", values_to = "valor") %>%
		    dplyr::left_join(celdas, by = c("x", "y")) %>%
		    dplyr::rename(celda_id = id) %>%
		    dplyr::filter(! is.na(campo_id) & ! is.na(valor)) %>%
		    dplyr::select(celda_id, variable, valor)
		  rm(raster.df)
		  
		  # c) Ejecutar queries en forma transaccional
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # ii. Insertar metadatos
		    DBI::dbSendQuery(conn = private$con, statement = "LOCK TABLE capa_datos IN EXCLUSIVE MODE")
		    result.metadatos <- DBI::dbSendStatement(conn = private$con, statement = "INSERT INTO capa_datos(campo_id, nombre, cultivo, campana, fecha) VALUES($1, $2, $3, $4, $5)")
		    DBI::dbBind(res = result.metadatos, params = list(campo$id, nombre, cultivo, campana, fecha))
		    DBI::dbClearResult(res = result.metadatos)
		    
		    # iii. Buscar id de capa de datos
		    capa.datos.metadatos <- self$buscar(campo = campo, nombre = nombre, cultivo = cultivo, campana = campana, fecha = fecha, con.datos = FALSE)
		    if (nrow(capa.datos.metadatos) == 1) {
		      capa_dato_id <- capa.datos.metadatos %>%
		        dplyr::pull(id)
		    } else {
		      stop("Error al buscar los datos recien insertados.")
		    }
		    
		    # iv. Insertar
		    df.capa.datos.variable <- capa.datos %>%
		      dplyr::mutate(capa_dato_id = capa_dato_id) %>%
		      dplyr::select(capa_dato_id, celda_id, variable, valor)
		    DBI::dbWriteTable(conn = private$con, name = "capa_datos_variable", append = TRUE, value = df.capa.datos.variable)
		    
		    # v. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    # vi. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al insertar la capa de datos ", campana, "-", cultivo, "-", nombre, "-", fecha, " para el campo ", campo$nombre, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (NULL)
		},
		
		insertarDataFrame = function(campo, nombre, cultivo, campana, fecha, datos.variable, sobrescribir = TRUE) {
		  # Ejecutar queries en forma transaccional
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # ii. Buscar si ya existia una capa de datos para ese campo con el mismo nombre, cultivo, campana y fecha
		    DBI::dbSendQuery(conn = private$con, statement = "LOCK TABLE capa_datos IN EXCLUSIVE MODE")
		    if (sobrescribir) {
		      capa.datos.anterior <- self$buscar(campo = campo, nombre = nombre, cultivo = cultivo, campana = campana, fecha = fecha, con.datos = FALSE)
		      if (nrow(capa.datos.anterior) == 1) {
		        result.delete <- DBI::dbSendStatement(conn = private$con, statement = "DELETE FROM capa_datos WHERE id = $1")
		        DBI::dbBind(res = result.delete, params = list(as.integer(capa.datos.anterior$id)))
		        DBI::dbClearResult(res = result.delete)
		      }
		    }
		    
		    if (nrow(datos.variable) > 0) {
  		    # iii. Insertar metadatos
  		    result.metadatos <- DBI::dbSendStatement(conn = private$con, statement = "INSERT INTO capa_datos(campo_id, nombre, cultivo, campana, fecha) VALUES($1, $2, $3, $4, $5)")
  		    DBI::dbBind(res = result.metadatos, params = list(campo$id, nombre, cultivo, campana, fecha))
  		    DBI::dbClearResult(res = result.metadatos)
  		    
  		    # iii. Buscar id de capa de datos
  		    capa.datos.metadatos <- self$buscar(campo = campo, nombre = nombre, cultivo = cultivo, campana = campana, fecha = fecha, con.datos = FALSE)
  		    if (nrow(capa.datos.metadatos) == 1) {
  		      capa_dato_id <- capa.datos.metadatos %>%
  		        dplyr::pull(id)
  		    } else {
  		      stop("Error al buscar los datos recien insertados.")
  		    }
  		    
  		    # iv. Insertar
  		    df.capa.datos.variable <- datos.variable %>%
  		      dplyr::mutate(capa_dato_id = capa_dato_id) %>%
  		      dplyr::select(capa_dato_id, celda_id, variable, valor)
  		    DBI::dbWriteTable(conn = private$con, name = "capa_datos_variable", append = TRUE, value = df.capa.datos.variable)
		    }
		    
		    # v. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    # vi. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al insertar la capa de datos ", campana, "-", cultivo, "-", nombre, "-", fecha, " para el campo ", campo$nombre, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (NULL)
		},
		
		referenciasCapasDatos = function(campo) {
		  user_id     <- as.integer(campo$user_id)
		  referencias <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'referencia_capa_datos')) %>%
		    dplyr::filter(user_id == !! user_id) %>%
		    dplyr::rename(referencia_capa_datos_id = id, descripcion = nombre, nombre = clave) %>%
		    dplyr::select(referencia_capa_datos_id, nombre, descripcion) %>%
		    dplyr::collect()
		  return(referencias)
		},
		
		referenciasVariables = function(campo, nombre_capa_datos) {
		  capa_datos_id <- self$referenciasCapasDatos(campo) %>%
		    dplyr::filter(nombre == nombre_capa_datos) %>%
		    dplyr::pull(referencia_capa_datos_id)
		    
		  if (length(capa_datos_id) > 0) {
  		  referencias_variables <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'referencia_capa_datos_variable')) %>%
  		    dplyr::filter(capa_datos_id %in% !! capa_datos_id) %>%
  		    dplyr::rename(unidades = unidad_de_medida) %>%
  		    dplyr::select(tipo, unidades, variable, factor_conversion) %>%
  		    dplyr::collect()
  		  return(referencias_variables)
		  }
		  return(NULL)
		}
	)
)
