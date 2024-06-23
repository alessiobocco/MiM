require(R6)
require(dplyr)
require(dbplyr)
require(stringr)

ResumenCapaDatosFacade <- R6Class("ResumenCapaDatosFacade",
  inherit = Facade,
  private = list(
    # Reemplaza placeholders (?) en query por placeholdes de tipo $1, $2, $3, etc. 
    reemplazar_placeholders = function(query, parameters) {
      # Validar cantidad de placeholders contra cantidad de parametros
      cantidad_placeholders <- stringr::str_count(string = query, pattern = "\\?")
      cantidad_parametros   <- length(parameters)
      base::stopifnot(cantidad_placeholders == cantidad_parametros)
      
      # Reemplazar placeholders
      nuevo_query <- query
      for(n in seq_len(cantidad_placeholders)) {
        nuevo_query <- stringr::str_replace(string = nuevo_query, pattern = "\\?", replacement = glue::glue("${n}"))
      }
      
      # Devolver nuevo query
      return(nuevo_query)
    },
    
    # Cuenta los datos satelitales (RGB, NDVI y NDRE)
    contar_datos_satelitales = function(tabla, campo_id) {
      # Validar no-nulidad y tipo de dato de campo_id y tabla
      base::stopifnot(! is.null(campo_id) && ! is.na(campo_id) && is.numeric(campo_id))
      base::stopifnot(! is.null(tabla) && ! is.na(tabla) && is.character(tabla))
      
      # Generar query
      query <- glue::glue("SELECT cd.campana, cd.nombre as capa_datos, cdv.variable as variable, count(1) as cantidad FROM {tabla} cdv JOIN capa_datos cd ON (cdv.capa_dato_id = cd.id) WHERE cd.campo_id = ? GROUP BY cd.campana, capa_datos, variable")
      parameters <- list(campo_id)
      self$executeSelectStatement(query = private$reemplazar_placeholders(query, parameters),
                                  parameters = parameters) %>%
        dplyr::mutate(campo_id = campo_id, categoria = 'Satelitales') %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
    },
    
    # Obtiene la campaña a partir de una fecha
    obtener_campana_fecha = function(fecha) {
      ano            <- lubridate::year(fecha)
      campana_inicio <- as.Date(sprintf("%d-%d-%d", ano, 5, 1))
      campana        <- sprintf("%d-%d", ano, ano+1)
      return(as.character(ifelse(fecha < campana_inicio, sprintf("%d-%d", ano-1, ano), campana)))
    }
  ),
  
  public = list(
    initialize = function(con) {
      private$con <- con
    },
                         
    # Cuenta los datos correspondientes a capas de NDVI
    contar_datos_ndvi = function(campo_id) {
      private$contar_datos_satelitales(campo_id = campo_id, tabla = "capa_datos_variable_ndvi")
    },
    
    # Cuenta los datos correspondientes a capas de NDRE
    contar_datos_ndre = function(campo_id) {
      private$contar_datos_satelitales(campo_id = campo_id, tabla = "capa_datos_variable_ndre")
    },
    
    # Cuenta los datos correspondientes a capas RGB
    contar_datos_rgb = function(campo_id) {
      private$contar_datos_satelitales(campo_id = campo_id, tabla = "capa_datos_variable_rgb")
    },
    
    # Cuenta los datos correspondientes a capas de ambientes, cobertura y rotacion
    contar_datos_mapas = function(campo_id) {
      # Validar no-nulidad y tipo de dato de campo_id
      base::stopifnot(! is.null(campo_id) && ! is.na(campo_id) && is.numeric(campo_id))
      
      # Contar datos de ambientes
      query_ambientes <- glue::glue("SELECT count(1) as cantidad FROM ambiente a JOIN celda cd ON (a.celda_id = cd.id) WHERE a.fecha_baja is null and cd.campo_id = ?")
      parameters_ambientes <- list(campo_id)
      datos_ambientes <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_ambientes, parameters_ambientes),
                                                     parameters = parameters_ambientes) %>%
        dplyr::mutate(campo_id = campo_id, campana = as.character(NA), categoria = 'Mapas', capa_datos = 'Ambiente', variable = as.character(NA)) %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
        
      
      # Contar datos de rotacion
      query_rotacion <- glue::glue("SELECT l.campana, count(1) as cantidad FROM lote l JOIN lote_celda lc ON (l.id = lc.lote_id)  WHERE l.campo_id = ? GROUP BY l.campana")
      parameters_rotacion <- list(campo_id)
      datos_rotacion <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_rotacion, parameters_rotacion),
                                                    parameters = parameters_rotacion) %>%
        dplyr::mutate(campo_id = campo_id, categoria = 'Mapas', capa_datos = 'Rotación', variable = as.character(NA)) %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
      
      # Contar datos de cobertura
      query_cobertura <- glue::glue("SELECT co.campana, count(1) as cantidad FROM cobertura co JOIN celda cd ON (co.celda_id = cd.id) WHERE cd.campo_id = ? GROUP BY co.campana")
      parameters_cobertura <- list(campo_id)
      datos_cobertura <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_cobertura, parameters_cobertura),
                                                     parameters = parameters_cobertura) %>%
        dplyr::mutate(campo_id = campo_id, categoria = 'Mapas', capa_datos = 'Cobertura', variable = as.character(NA)) %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
      
      # Devolver datos
      dplyr::bind_rows(datos_ambientes, datos_rotacion, datos_cobertura)
    },
    
    # Cuenta los datos correspondientes a sensores automaticos
    contar_datos_sensores_automaticos = function(campo_id, sensores) {
      # Validar no-nulidad y tipo de dato de campo_id y sensores
      base::stopifnot(! is.null(campo_id) && ! is.na(campo_id) && is.numeric(campo_id))
      base::stopifnot(! is.null(sensores) && (nrow(sensores) > 0))
      
      # Contar datos de sensores
      sensor_id_placeholder <- paste0(rep("?", nrow(sensores)), collapse = ", ")
      query_registros_sensores <- glue::glue("SELECT sensor_id, fecha_hora, v.nombre as variable FROM registro_sensor_variable r JOIN variable v ON (r.variable_id = v.id) WHERE r.valor is not null and r.sensor_id IN ({sensor_id_placeholder})")
      parameters_registros_sensores <- as.list(dplyr::pull(sensores, id))
      datos_registros_sensores <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_registros_sensores, parameters_registros_sensores),
                                                              parameters = parameters_registros_sensores) %>%
        dplyr::inner_join(sensores, by = c("sensor_id" = "id")) %>%
        dplyr::rename(capa_datos = nombre) %>%
        dplyr::mutate(campo_id = campo_id, campana = private$obtener_campana_fecha(fecha_hora), categoria = 'Sensores') %>%
        dplyr::group_by(campo_id, campana, categoria, capa_datos, variable) %>%
        dplyr::summarise(cantidad = dplyr::n())
      
      # Devolver datos
      return(datos_registros_sensores)
    },
    
    # Cuenta los datos correspondientes a capas informacion rasterizada
    # Se incluyen datos de altimetria 
    # Se excluyen datos satelitales
    contar_datos_rasterizados = function(campo_id) {
      # Validar no-nulidad y tipo de dato de campo_id
      base::stopifnot(! is.null(campo_id) && ! is.na(campo_id) && is.numeric(campo_id))
      
      # Contar datos de ambientes
      query_altimetria <- glue::glue("SELECT count(1) as cantidad FROM celda WHERE campo_id = ?")
      parameters_altimetria <- list(campo_id)
      datos_altimetria <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_altimetria, parameters_altimetria),
                                                      parameters = parameters_altimetria) %>%
        dplyr::mutate(campo_id = campo_id, campana = as.character(NA), categoria = 'Capas de datos', capa_datos = 'Altimetría', variable = as.character(NA)) %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
      
      # Contar datosrasterizados a excepcion de datos satelitales
      query_datos_rasterizados <- glue::glue("SELECT cd.campana, cd.nombre as capa_datos, cdv.variable as variable, count(1) as cantidad FROM capa_datos cd JOIN capa_datos_variable_otros cdv ON (cd.id = cdv.capa_dato_id) WHERE cd.campo_id = ? GROUP BY cd.campana, capa_datos, variable")
      parameters_datos_rasterizados <- list(campo_id)
      datos_rasterizados <- self$executeSelectStatement(query = private$reemplazar_placeholders(query_datos_rasterizados, parameters_datos_rasterizados),
                                                        parameters = parameters_datos_rasterizados) %>%
        dplyr::mutate(campo_id = campo_id, categoria = 'Capas de datos') %>%
        dplyr::select(campo_id, campana, categoria, capa_datos, variable, cantidad)
      
      # Devolver datos
      dplyr::bind_rows(datos_altimetria, datos_rasterizados)
    },
    
    # Finder
    buscar = function(user_id, campo_id = NULL, campana_seleccionada = NULL) {
      # Buscar campos del usuario si el campo no esta definido
      filtros <- list(campo_id = campo_id)
      if (is.null(filtros$campo_id)) {
        filtros$campo_id <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'campo')),
                                            list(user_id = user_id)) %>%
          dplyr::pull(id)
      }
      
      # Buscar registros de resumen
      resumen <- private$filtrar(dplyr::tbl(src = private$con, dbplyr::in_schema('public', 'resumen_capa_datos')), filtros)
      
      # Filtrar por compana si es aplicable
      if (! is.null(campana_seleccionada)) {
        resumen <- resumen %>%
          dplyr::filter(is.na(campana) || (campana == campana_seleccionada))
      }
      
      # Devolver resultados
      return (dplyr::collect(resumen))
    },
    
    # Actualizar resumen de datos
    actualizar = function(resumen) {
      DBI::dbBegin(conn = private$con)
      tryCatch({
        # Sobrescribir la tabla resumen_capa_datos
        DBI::dbSendQuery(conn = private$con, statement = "TRUNCATE TABLE resumen_capa_datos")
        DBI::dbWriteTable(conn = private$con, name = "resumen_capa_datos", append = TRUE, value = resumen)
        
        # Hacer commit
        DBI::dbCommit(conn = private$con)  
      }, error = function(e) {
        # Hacer rollback porque hubo un error
        DBI::dbRollback(conn = private$con)  
        stop(paste0("Error al actualizar la tabla resumen_capa_datos [ Llamada: ", e$call, ", Mensaje: ", e$message, " ]"))
      })
    }
  )
)
