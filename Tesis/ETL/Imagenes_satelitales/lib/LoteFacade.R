require(R6)
require(dplyr)
require(dbplyr)
require(magrittr)
require(raster)
require(sf)

LoteFacade <- R6Class("LoteFacade",
	inherit = Facade,
	private = list(
	  campo.facade = NULL,
	  poligono_proj4string = NA,
	  as.SimpleFeature = function(data) {
	    sf.object <- sf::st_as_sf(data, crs = campo.facade$crs_grilla(), wkt = "poligono_wkt")
	    return (sf.object)
	  },
	  cultivo.manejo.revision = list(
	    'CE/MZ'    = c('cebada', 'maiz'),
	    'CENTE/MZ' = c('centeno', 'maiz'),
	    'CENTENO'  = c('centeno'),
	    'CE/SJ'    = c('cebada', 'soja'),
	    'MZ TARD'  = c('maiz'),
	    'MZ TEMP'  = c('maiz'),
	    'SJ'       = c('soja'),
	    'SORGO'    = c('sorgo'),
	    'TRG/MZ'   = c('trigo', 'maiz'),
	    'TRG/SJ'   = c('trigo', 'soja'),
	    'TRIGO'    = c('trigo')
	  )
	),
	public = list(
	  initialize = function(con, campo.facade) {
	    private$con          <- con
	    private$campo.facade <- campo.facade
	  },
	  
		buscar = function(campo, codigo = NULL, sublote = NULL, campana = NULL, cultivo = NULL, con.celdas = TRUE, con.poligonos = FALSE, revisar.cultivos = FALSE, lote_id = NULL) {
		  filtros <- list(campo_id = campo$id, codigo = codigo, sublote = sublote, campana = campana, cultivo = cultivo, id = lote_id)
		  if (con.poligonos) {
		    lote.ids <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'lote')), filtros) %>%
		      dplyr::collect() %>%
		      dplyr::pull(id)
		    query    <- paste0("SELECT id, campo_id, codigo, sublote, campana, cultivo, ST_AsText(poligono) poligono_wkt FROM lote")
		    lotes    <- DBI::dbSendQuery(con = private$con, statement = query) %>%
		      DBI::dbFetch(n = -1) %>%
		      dplyr::filter(id %in% lote.ids)
		    return (private$as.SimpleFeature(dplyr::collect(lotes)))
		  } else {
		    # Buscar metadatos
		    lotes <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'lote')), filtros)
		    
		    # Agregar datos de celdas
		    if (con.celdas) {
    		  lotes.celdas <- dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'lote_celda')) %>%
    		    dplyr::inner_join(lotes, by = c("lote_id" = "id")) %>%
    		    dplyr::select(-poligono) %>%
    		    dplyr::collect()
    		  
    		  if (revisar.cultivos) {
    		    # Revisar que para cada dato de cultivo manejo haya al menos un dato de la capa de rotacion correspondiente
    		    # Primero busco los cultivos cuyas capas de rendimiento se buscaran
    		    cultivos.rendimiento <- purrr::map(
    		      .x = unique(lotes.celdas$cultivo),
    		      .f = function(manejo) {
    		        return (private$cultivo.manejo.revision[[manejo]])
    		      }
    		    ) %>% unlist() %>% unique()
    		    
    		    # Buscar capas de rendimiento
    		    filtros.capas.datos <- list(campo_id = campo$id, nombre = "cosecha", cultivo = cultivos.rendimiento, 
    		                                campana = campana)
    		    filtros.datos       <- list(variable = "RTO.KG.HA")
    		    capas.rendimiento   <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'capa_datos')), 
    		                                           filtros.capas.datos)
    		    datos.rendimiento   <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'capa_datos_variable')), 
    		                                           filtros.datos) %>%
    		      dplyr::inner_join(capas.rendimiento, by = c("capa_dato_id" = "id")) %>%
    		      dplyr::select(nombre, cultivo, campana, fecha, celda_id, variable, valor) %>%
    		      dplyr::collect() %>%
    		      dplyr::select(celda_id, cultivo)
    		    
    		    # Efectuar revision de celdas
    		    if (nrow(datos.rendimiento) > 0) {
      		    lotes.celdas.revisadas <- purrr::map_dfr(
      		      .x = unique(lotes.celdas$cultivo),
      		      .f = function(manejo) {
      		        cultivos <- private$cultivo.manejo.revision[[manejo]]
      		        if (! is.null(cultivos)) {
        		        lotes.celdas.manejo  <- dplyr::filter(lotes.celdas, cultivo == manejo)
        		        rendimientos.cultivo <- dplyr::filter(datos.rendimiento, cultivo %in% cultivos) %>%
        		          dplyr::distinct(celda_id)
        		        lotes.celda.ok       <- lotes.celdas.manejo %>%
        		          dplyr::inner_join(rendimientos.cultivo, by = c("celda_id"))
        		        return (lotes.celda.ok)
      		        }
      		        return (NULL)
      		      }
      		    )
      		    lotes.celdas <- lotes.celdas.revisadas
    		    } else {
    		      # No hay datos de rendimiento, devuelvo NULL
    		      return (NULL)
    		    }
    		  }
    		  
          return (lotes.celdas)
  		  } else {
  		    return (dplyr::collect(lotes))
  		  }
		  }
		},
		
		buscarUltimaCampanaRotacion = function(campo) {
		  query     <- paste0("SELECT max(campana) ultima_campana FROM lote WHERE campo_id = $1")
		  resultado <- self$executeSelectStatement(query, list(as.integer(campo$id)))
		  if (nrow(resultado) > 0) {
		    return(dplyr::pull(resultado, ultima_campana))
		  } else {
		    return (NULL)
		  }
		},
		
		insertarCapaLotes = function(campo, campana, shape.lotes) {
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  
		  tryCatch({
		    purrr::walk(
		      .x = seq(from = 1, to = nrow(shape.lotes)),
		      .f = function(seq_index) {
		        shape.lote <- shape.lotes[seq_index,]
		        codigo     <- as.character(shape.lote$codigo)
		        cultivo    <- as.character(shape.lote$cultivo)
		        self$insertar(campo = campo, codigo = codigo, campana = campana,
		                      cultivo = cultivo, shape.lote = shape.lote, con.transaccion = FALSE)
		      }
		    )    
		    
		    # ii. Hacer commit
		    DBI::dbCommit(conn = private$con)
		  }, error = function(e) {
		    # iii. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)
		    stop(paste0("Error al insertar la capa de lotes para la campaña ", campana, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		},
		
		borrarCapaLotes = function(campo, campana) {
		  # i. Iniciar transaccion
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # ii. Eliminar capas correspondientes a campana indicada
		    result.delete <- DBI::dbSendStatement(conn = private$con, 
		                                          statement = "DELETE FROM lote where campo_id = $1 AND campana = $2")
		    DBI::dbBind(res = result.delete, params = list(campo$id, campana))
		    DBI::dbClearResult(res = result.delete)
		    
		    # iii. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    # iv. Hacer rollback porque hubo un error
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al eliminar el la capa de lotes del campo: [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		},
		
		insertar = function(campo, codigo, sublote = 1, campana, cultivo, shape.lote, con.transaccion = TRUE) {
		  # a) Cambiar CRS de shape y obtener raster del campo
		  shape.lote.proj    <- sf::st_transform(shape.lote, crs = campo$grilla_proj4string)
		  raster.celdas      <- private$campo.facade$celdas(campo, devolver.raster = TRUE)
		  celdas             <- private$campo.facade$celdas(campo, devolver.raster = FALSE)
		  
		  # b) Enmascarar el raster de celdas al poligono del lote
		  raster.lote <- raster::mask(x = raster.celdas, mask = sf::as_Spatial(shape.lote.proj))
		  
		  # c) Ejecutar queries en forma transaccional
		  if (con.transaccion) {
  		  # i. Iniciar transaccion
  		  DBI::dbBegin(conn = private$con)
		  }
		  tryCatch({
		    # ii. Determinar si el nombre del lote esta duplicado
		    lote.metadatos <- self$buscar(campo = campo, codigo = codigo, sublote = sublote, campana = campana, con.celdas = FALSE)
		    while (nrow(lote.metadatos) > 0) {
		      sublote        <- sublote + 1
		      lote.metadatos <- self$buscar(campo = campo, codigo = codigo, sublote = sublote, campana = campana, con.celdas = FALSE)
		    }
		    
		    # iii. Insertar metadatos
		    DBI::dbSendQuery(conn = private$con, statement = "LOCK TABLE lote IN EXCLUSIVE MODE")
		    result.metadatos <- DBI::dbSendStatement(conn = private$con, statement = "INSERT INTO lote(campo_id, codigo, sublote, campana, cultivo, poligono) VALUES($1, $2, $3, $4, $5, ST_GeomFromText($6, 4326))")
		    DBI::dbBind(res = result.metadatos, params = list(campo$id, codigo, sublote, campana, cultivo, sf::st_as_text(sf::st_geometry(shape.lote))))
		    DBI::dbClearResult(res = result.metadatos)
		    
		    # iv. Buscar id de capa de datos
		    lote.metadatos <- self$buscar(campo = campo, codigo = codigo, sublote = sublote, campana = campana, con.celdas = FALSE)
		    if (nrow(lote.metadatos) == 1) {
		      lote_id <- lote.metadatos %>%
		        dplyr::pull(id)
		    } else {
		      stop("Error al buscar los datos recien insertados.")
		    }
		    
		    # v. Insertar
		    df.lote.celdas <- base::as.data.frame(x = raster::rasterToPoints(raster.lote), 
		                                          stringsAsFactors = FALSE) %>%
		      dplyr::inner_join(celdas, by = c("x", "y")) %>%
		      dplyr::rename(celda_id = id) %>%
		      dplyr::mutate(lote_id = lote_id) %>%
		      dplyr::select(lote_id, celda_id)
		    DBI::dbWriteTable(conn = private$con, name = "lote_celda", append = TRUE, value = df.lote.celdas)
		    
		    if (con.transaccion) {
  		    # vi. Hacer commit
  		    DBI::dbCommit(conn = private$con)
		    }
		  }, error = function(e) {
		    if (con.transaccion) {
  		    # vii. Hacer rollback porque hubo un error
  		    DBI::dbRollback(conn = private$con)
		    }
		    stop(paste0("Error al insertar los datos del lote ", codigo, " (", sublote, ") para la campaña ", campana, ": [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (self$buscar(campo))
		},
		
		coloresReferencia = function(user_id, campo, devolver_lista = FALSE, paleta_faltantes = "Paired") {
		  # Buscar colores de referencia definidos por el usuario
		  filtros     <- list(user_id = user_id)
		  referencias <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'referencia_rotacion')), filtros) %>%
		    dplyr::rename(nombre = descripcion) %>%
		    dplyr::select(cultivo, nombre, color, orden) %>%
		    dplyr::arrange(orden) %>%
		    dplyr::collect()
		  
		  # Buscar cultivos/manejo cargados para el campo
		  cultivos_manejo <- self$buscar(campo, con.celdas = FALSE, con.poligonos = FALSE) %>%
		    dplyr::distinct(cultivo) %>%
		    dplyr::pull(cultivo)
		  
		  # Para los cultivos/manejo que tienen definido un color, utilizar ese color
		  # Para los demas cultivos/manejo, utilizar la paleta de colores definida como parametro
		  manejos_con_color <- referencias %>%
		    dplyr::pull(cultivo)
		  manejos_sin_color <- setdiff(cultivos_manejo, intersect(manejos_con_color, cultivos_manejo))
		  referencias_default <- NULL
		  if (length(manejos_sin_color) > 0) {
		    referencias_default <- tibble::tibble(
		      cultivo = manejos_sin_color,
		      nombre = "", 
		      color = private$generarPaletaColores(cantidad = length(manejos_sin_color),
		                                           paleta = paleta_faltantes)
		    ) %>%
		      dplyr::mutate(orden = dplyr::row_number() + nrow(referencias))
		  }
		  
		  # Agregar ambiente default
		  if (nrow(referencias) > 0) {
		    referencias <- dplyr::bind_rows(
		      referencias,
		      referencias_default
		    )
		  } else if (! is.null(referencias_default)) {
		    referencias <- referencias_default
		  }
		  
		  if (devolver_lista) {
		    referencias <- as.list(tibble::deframe(dplyr::select(referencias, cultivo, color)))
		  }
		  
		  return (referencias)
		}
	)
)