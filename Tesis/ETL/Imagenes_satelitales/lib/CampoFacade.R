require(R6)
require(dplyr)
require(dbplyr)
require(magrittr)
require(raster)
require(sf)

CampoFacade <- R6Class("CampoFacade",
	inherit = Facade,
	private = list(
	  grilla_proj4string = NULL,
	  grilla_srid = NULL,
	  poligono_proj4string = NULL,
	  as.SimpleFeature = function(data) {
	    sf.object <- sf::st_as_sf(data, crs = private$poligono_proj4string, wkt = "poligono_wkt")
	    return (sf.object)
	  }
	),
	public = list(
	  initialize = function(con, poligono_proj4string, grilla_proj4string, grilla_srid) {
	    private$con                  <- con
	    private$poligono_proj4string <- poligono_proj4string
	    private$grilla_proj4string   <- grilla_proj4string
	    private$grilla_srid          <- grilla_srid
	  },
	  
		buscar = function(id = NULL, nombre = NULL, unidad_administrativa_id = NULL) {
		  filtros   <- list(id = id, nombre = nombre, unidad_administrativa_id = unidad_administrativa_id)
		  campo.ids <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'campo')), filtros) %>%
		    dplyr::collect() %>%
		    dplyr::pull(id)
		  query     <- paste0("SELECT id, nombre, grilla_proj4string, unidad_administrativa_id, ST_AsText(poligono) poligono_wkt FROM campo")
		  campos    <- DBI::dbSendQuery(con = private$con, statement = query) %>%
		    DBI::dbFetch(n = -1) %>%
		    dplyr::filter(id %in% campo.ids)
  		return (private$as.SimpleFeature(campos))
		},
		
		insertar = function(nombre, shape.campo, unidad_administrativa_id) {
		  query      <- paste0("INSERT INTO campo(nombre, grilla_proj4string, poligono, unidad_administrativa_id, grilla_srid) VALUES($1, $2, ST_GeomFromText($3, 4326), $4, $5)")
		  parameters <- list(nombre, private$grilla_proj4string, sf::st_as_text(sf::st_geometry(shape.campo)), unidad_administrativa_id, private$grilla_srid)
		  result.set <- DBI::dbSendStatement(private$con, query)
		  DBI::dbBind(result.set, parameters)
		  DBI::dbClearResult(result.set)
		  return (self$buscar(nombre = nombre))
		},
		
		grillar = function(campo, resolucion, shape.altimetria) {
		  # a. Generar grilla regular vacia
		  shape.campo.utm <- sf::st_transform(x = campo, crs = campo$grilla_proj4string)
		  grilla.regular  <- sf::st_make_grid(x = shape.campo.utm, what = "centers",
		                                      cellsize = c(resolucion, resolucion))
		  grilla.xyz      <- sf::st_coordinates(grilla.regular) %>%
		    dplyr::as_data_frame() %>%
		    dplyr::mutate(z = 1)
		  raster.vacio <- raster::rasterFromXYZ(grilla.xyz) %>%
		    raster::mask(campo)
		  raster::crs(raster.vacio) <- campo$grilla_proj4string
		  
		  if (! is.null(shape.altimetria)) {
  		  # b. Rasterizar shape de elevacion
  		  crs.altimetria <- sf::st_crs(shape.altimetria)
  		  if (is.na(crs.altimetria$proj4) || (crs.altimetria$proj4string != campo$grilla_proj4string)) {
  		    shape.altimetria <- sf::st_transform(shape.altimetria, crs = campo$grilla_proj4string)
  		  }
  		  raster.altimetria <- raster::rasterize(
  		    x = sf::as_Spatial(shape.altimetria),
  		    y = raster.vacio,
  		    field = shape.altimetria[[1]],
  		    fun = mean
  		  )
  		  
  		  # c. Interpolar para eliminar posibles huecos utilizando malla de 3x3
  		  fill.na <- function(x, i = 5) {
  		    if (is.na(x)[i]) {
  		      return (mean(x, na.rm = TRUE))
  		    } else {
  		      return (x[i])
  		    }
  		  }
  		  raster.interpolado <- focal(x = raster.altimetria, w = matrix(1,3,3), fun = fill.na, pad = TRUE, na.rm = FALSE)
  		  df.altimetria      <- base::as.data.frame(x = raster::rasterToPoints(raster.interpolado), stringsAsFactors = FALSE)
  		  colnames(df.altimetria) <- c("x", "y", "elevacion")
		  } else {
		    # Generar grilla con altitud 0
		    raster.altimetria <- raster::rasterize(
		      x = sf::as_Spatial(shape.campo.utm),
		      y = raster.vacio
		    )
		    df.altimetria <- base::as.data.frame(x = raster::rasterToPoints(raster.altimetria), stringsAsFactors = FALSE) %>%
		      dplyr::mutate(layer = as.double(0))
		    colnames(df.altimetria) <- c("x", "y", "elevacion")
		  }
		  
		  # c. Insertar celdas en base de datos
		  DBI::dbBegin(conn = private$con)
		  tryCatch({
		    # i. Borrar celdas existentes para ese campo
		    result.delete <- DBI::dbSendStatement(conn = private$con, statement = "DELETE FROM celda WHERE campo_id = $1")
		    DBI::dbBind(res = result.delete, params = list(campo$id))
		    DBI::dbClearResult(res = result.delete)
		    
		    # ii. Insertar nuevas celdas
		    df.celdas <- df.altimetria %>%
		      dplyr::mutate(campo_id = campo$id) %>%
		      dplyr::select(campo_id, x, y, elevacion)
		    DBI::dbWriteTable(conn = private$con, name = "celda", append = TRUE, value = df.celdas)
		    
		    # iii. Hacer commit
		    DBI::dbCommit(conn = private$con)  
		  }, error = function(e) {
		    DBI::dbRollback(conn = private$con)  
		    stop(paste0("Error al grillar el campo: [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
		  })
		  
		  return (self$celdas(campo))
		},
		
		celdas = function(campo, devolver.raster = FALSE) {
		  filtros <- list(campo_id = campo$id)
		  celdas  <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'celda')), filtros) %>%
		    dplyr::collect()
		  if (devolver.raster) {
		    celdas.xyz    <- celdas %>%
		      dplyr::mutate(z = elevacion) %>%
		      dplyr::select(x, y, z)
		    celdas.raster <- raster::rasterFromXYZ(xyz = celdas.xyz, crs = campo$grilla_proj4string)
		    return (celdas.raster)
		  } else {
		    return (celdas)  
		  }
		},
		
		celdas_favoritas = function(campo, poligonos_favoritos) {
		  # Obtener celdas
		  celdas <- self$celdas(campo = campo, devolver.raster = FALSE)
		  
		  # Generar raster a partir de celdas
		  raster_celdas <- celdas %>%
		    dplyr::mutate(z = as.double(id)) %>%
		    dplyr::select(x, y, z) %>%
		    raster::rasterFromXYZ(crs = campo$grilla_proj4string)
		  
		  # Filtrar celdas
		  celdas_favoritas <- purrr::map_dfr(
		    .x = dplyr::pull(poligonos_favoritos, id),
		    .f = function(id) {
		      poligono_favorito <- dplyr::filter(poligonos_favoritos, id == !! id)
		      raster_poligono   <- raster::mask(raster_celdas, poligono_favorito)
		      celdas_poligono   <- raster::rasterToPoints(raster_poligono) %>%
		        as.data.frame() %>%
		        dplyr::mutate(celda_id = as.integer(z), poligono = poligono_favorito$nombre) %>%
		        dplyr::select(celda_id, poligono)
		    }
		  )
		  
		  # Devolver celdas
		  return(celdas_favoritas)
		},
		
		crs_grilla = function() {
		  return (private$grilla_proj4string)
		},
		
		# lote_ids_ultima_campana = list(lote1_id, lote2_id, ...)
		usoTierraCampana = function(campo, lote_ids_ultima_campana = list()) {
		  query      <- NULL
		  parameters <- NULL
		  if (length(lote_ids_ultima_campana) > 0) {
		    # Aplicar busqueda sobre algunos lotes en particular
		    lote_phs1  <- paste0(paste0("$", seq_along(lote_ids_ultima_campana) + 1), collapse = ", ")
		    lote_phs2  <- paste0(paste0("$", (seq_along(lote_ids_ultima_campana) + length(lote_ids_ultima_campana) + 1)), collapse = ", ")
		    query      <- glue::glue("select l.campana, l.cultivo, count(*)::int cantidad, (select count(*) as total from lote_celda where lote_id in ({lote_phs1})) cantidad_total from lote l join lote_celda lc on (l.campo_id = $1) and (l.id = lc.lote_id) where lc.celda_id in (select lc1.celda_id from lote_celda lc1 where lc1.lote_id in ({lote_phs2})) group by l.campana, l.cultivo")
		    parameters <- as.list(c(as.integer(campo$id), unlist(lote_ids_ultima_campana), unlist(lote_ids_ultima_campana)))
		  } else {
		    # Aplicar busqueda sobre todos los lotes
		    query      <- "select l.campana, l.cultivo, count(*)::int cantidad, (select count(*)::int from celda where campo_id = $1) cantidad_total from lote l join lote_celda ld on (l.id = ld.lote_id) where l.campo_id = $1 group by l.campana, l.cultivo"
		    parameters <- list(as.integer(campo$id))
		  }
		  
		  resultados <- self$executeSelectStatement(query = query, parameters = parameters)
		  if (! is.null(resultados)) {
		    resultados %<>% dplyr::mutate(proporcion = 100 * cantidad / cantidad_total)
		  }
		  return (resultados)
		},
		
		# lote_ids_ultima_campana = list(lote1_id, lote2_id, ...)
		usoTierraCampanaCultivos = function(campo, cultivos, lote_ids_ultima_campana = list()) {
		  cultivos.str <- paste0("'", cultivos, "'", collapse = ", ")
		  if (length(lote_ids_ultima_campana) > 0) {
		    lote_phs   <- paste0(paste0("$", seq_along(lote_ids_ultima_campana) + 1), collapse = ", ")
		    query      <- glue::glue("select l.campana, l.cultivo, count(*)::int cantidad from lote l join lote_celda lc on (l.campo_id = $1) and (l.id = lc.lote_id) where l.cultivo in ({cultivos.str}) and lc.celda_id in (select lc1.celda_id from lote_celda lc1 where lc1.lote_id in ({lote_phs})) group by l.campana, l.cultivo")
		    parameters <- as.list(c(as.integer(campo$id), unlist(lote_ids_ultima_campana)))
		  } else {
		    query      <- glue::glue("select l.campana, l.cultivo, count(*)::int cantidad from lote l join lote_celda ld on (l.id = ld.lote_id) where l.campo_id = $1 and cultivo in ({cultivos.str}) group by l.campana, l.cultivo")  
		    parameters <- list(as.integer(campo$id))
		  }
		  
		  return(self$executeSelectStatement(query = query, parameters = parameters))
		},
		
		# lote_ids_ultima_campana = list(lote1_id, lote2_id, ...)
		coberturaCampana = function(campo, agregado = TRUE, lote_ids_ultima_campana = list()) {
		  if (length(lote_ids_ultima_campana) > 0) {
		    # Aplicar busqueda sobre algunos lotes en particular
		    lote_phs1  <- paste0(paste0("$", seq_along(lote_ids_ultima_campana)), collapse = ", ")
		    lote_phs2  <- paste0(paste0("$", (seq_along(lote_ids_ultima_campana) + length(lote_ids_ultima_campana))), collapse = ", ")
		    parameters <- as.list(c(unlist(lote_ids_ultima_campana), unlist(lote_ids_ultima_campana)))
		    if (agregado) {
		      # Proporción por campaña sin distinguir cultivo  
		      query <- glue::glue("select c.campana, count(*)::int cantidad, (select count(*) as total from lote_celda where lote_id in ({lote_phs1})) cantidad_total from cobertura c join celda ce on (c.celda_id = ce.id) where ce.id in (select lc1.celda_id from lote_celda lc1 where lc1.lote_id in ({lote_phs2})) group by c.campana")
		    } else {
		      # Proporción por campaña y cultivo  
		      query <- glue::glue("select c.campana, c.cultivo, count(*)::int cantidad, (select count(*) as total from lote_celda where lote_id in ({lote_phs1})) cantidad_total from cobertura c join celda ce on (c.celda_id = ce.id) where ce.id in (select lc1.celda_id from lote_celda lc1 where lc1.lote_id in ({lote_phs2})) group by c.campana, c.cultivo")
		    }
		  } else {
		    # Aplicar busqueda sobre todos los lotes
		    parameters <- list(as.integer(campo$id))
		    if (agregado) {
  		    # Proporción por campaña sin distinguir cultivo  
    		  query <- "select c.campana, count(*)::int cantidad, (select count(*)::int from celda where campo_id = $1) cantidad_total from cobertura c join celda ce on (c.celda_id = ce.id) where ce.campo_id = $1 group by c.campana"
  		  } else {
  		    # Proporción por campaña y cultivo  
  		    query <- "select c.campana, c.cultivo, count(*)::int cantidad, (select count(*)::int from celda where campo_id = $1) cantidad_total from cobertura c join celda ce on (c.celda_id = ce.id) where ce.campo_id = $1 group by c.campana, c.cultivo"
  		  }
		  }
		  
		  # Ejecutar query y devolver resultados
		  resultados <- self$executeSelectStatement(query = query, parameters = parameters) %>%
		    dplyr::mutate(proporcion = 100 * cantidad / cantidad_total)
		  return (resultados)
		},
		
		# lote_ids_ultima_campana = list(lote1_id, lote2_id, ...)
		diasSecadoCoberturaCampana = function(campo, lote_ids_ultima_campana = list()) {
		  if (length(lote_ids_ultima_campana) > 0) {
		    # Aplicar busqueda sobre algunos lotes en particular
		    lote_phs   <- paste0(paste0("$", seq_along(lote_ids_ultima_campana)), collapse = ", ")
		    query      <- glue::glue("select c.campana, c.cultivo, avg(c.fecha_secado - c.fecha_siembra) dias_secado from cobertura c where c.celda_id in (select lc1.celda_id from lote_celda lc1 where lc1.lote_id in ({lote_phs})) group by c.campana, c.cultivo")
		    parameters <- lote_ids_ultima_campana
		  } else {
		    # Aplicar busqueda sobre todos los lotes
		    query      <- "select c.campana, c.cultivo, avg(c.fecha_secado - c.fecha_siembra) dias_secado from cobertura c where c.celda_id in (select id from celda where campo_id = $1) group by c.campana, c.cultivo"
		    parameters <- list(as.integer(campo$id))
		  }
  		  
		  return(self$executeSelectStatement(query = query, parameters = parameters))
		},
		
		cartaSuelos = function(campo) {
		  resultados <- self$executeSelectStatement(query = "SELECT cs.id, cs.ucart, cs.ip, ST_AsText(cs.poligono) as poligono_wkt FROM carta_suelos cs, campo c WHERE c.id = $1 AND ST_Intersects(cs.poligono, ST_Envelope(c.poligono))", 
		                                            parameters = list(as.integer(campo$id)))
		  suelos     <- private$as.SimpleFeature(resultados)
		  return (sf::st_intersection(suelos, sf::st_geometry(sf::st_make_valid(campo))))
		}
	)
)