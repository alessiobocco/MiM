# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
# -----------------------------------------------------------------------------#
rm(list = ls()); gc()
list.of.packages <- c("dplyr", "terra", "lubridate", "magrittr", "raster", 
                      "sf", "sp", "utils", "yaml", "foreach", "leaflet",
                      "httr")
for (pack in list.of.packages) {
  if (!require(pack, character.only = TRUE)) {
    stop("Paquete no instalado: ", pack)
  }
}
options(timeout = 300)
rm(pack); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivo de configuracion ----
# -----------------------------------------------------------------------------#
args <- base::commandArgs(trailingOnly = TRUE)
archivo.config <- args[1]
if (is.na(archivo.config)) {
  # No vino el archivo de configuracion por linea de comandos. Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion_ndvi.yml")
}

if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml.load_file(archivo.config)
}

rm(archivo.config, args); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3. Inicializar script ----
# -----------------------------------------------------------------------------#

# i. Cargar librerias
source(paste0(config$dir$lib, "/Facade.R"))
source(paste0(config$dir$lib, "/CampoFacade.R"))
source(paste0(config$dir$lib, "/CapaDatosFacade.R"))
source(paste0(config$dir$lib, "/Script.R"))
source(paste0(config$dir$lib, "/LoteFacade.R"))
source(paste0("./funciones_propias.R"))


# ii. Conectarse a base de datos
con <- DBI::dbConnect(drv = RPostgres::Postgres(),
                      dbname = config$db$name,
                      user = config$db$user,
                      password = config$db$password,
                      host = config$db$host,
                      port = config$db$port
)

# iii. Crear facades
campo.facade      <- CampoFacade$new(con = con, poligono_proj4string = config$proj4string$latlon,
                                     grilla_proj4string = config$proj4string$utm,
                                     grilla_srid = config$proj4string$grilla_srid)
capa.datos.facade <- CapaDatosFacade$new(con = con, campo.facade = campo.facade)

# iv. Crear objeto de clase Script e iniciar
script <- Script$new(run.dir = config$dir$run, name = "descargando_capas", create.appender = T)
script$start()

# v. Conectar al servicio web. 
# Se necesitan las credenciales de EarthData
earthdatalogin::edl_netrc(username = "alessio.bocco",
                          password = "Bananatree_1990")

# vi. Configurar RGDAL para realizar el procesamiento espacial
rgdal::setCPLConfigOption(ConfigOption = "GDAL_HTTP_UNSAFESSL", value = "YES")
rgdal::setCPLConfigOption(ConfigOption = "GDAL_HTTP_COOKIEFILE", value = ".rcookies")
rgdal::setCPLConfigOption(ConfigOption = "GDAL_HTTP_COOKIEJAR", value = ".rcookies")
rgdal::setCPLConfigOption(ConfigOption = "GDAL_DISABLE_READDIR_ON_OPEN", value = "YES")
rgdal::setCPLConfigOption(ConfigOption = "CPL_VSIL_CURL_ALLOWED_EXTENSIONS", value = "TIF")

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 4. Realizar procesamiento para cada campo y finalizar ----
# -----------------------------------------------------------------------------#

# URL para buscar las imagenes disponibles 
search_URL <- 'https://cmr.earthdata.nasa.gov/stac/LPCLOUD/search'
# Productos a buscar:
# HLSS30.v2.0 corresponde a Sentinel 2
# HLSL30.v2.0 corresponde a Landsat
# Ambos productos estan en la misma resolucion y con las distintas
# bandas sincronizadas
# Si se definen los dos se descargar una serie temporal con ambos productos.
HLS_col <- list("HLSS30.v2.0", "HLSL30.v2.0")

# a) Buscar campos a procesar
# Buscar campos y filtrarlos en caso de que se haya especificado
campos <- campo.facade$buscar()
if (! is.null(config$descarga$campos$incluir)) {
  campos %<>% dplyr::filter(id %in% config$campos.ignorados)
} else if (! is.null(config$campos.ignorados)) {
  campos %<>% dplyr::filter(! id %in% config$campos.ignorados)
}

# Iterar por campo
for (i in 1:nrow(campos)) {
  campo     <- campos[i, ]
  campo.utm <- sf::st_transform(x = campo, crs = config$proj4string$utm)
  if (! sf::st_is_valid(campo.utm)) {
    campo.utm <- sf::st_make_valid(campo.utm)
  }
  raster_base <- campo.facade$celdas(campo)
  script$info(paste0("Procesando campo ", campo$nombre))
  
  # ii. Determinar rango de fechas para descarga
  if (! is.null(config$descarga$fechas$incremental)) {
    fecha.desde    <- NULL
    fecha.hasta    <- Sys.Date()
    capas.producto <- capa.datos.facade$buscar(campo = campo, nombre = "ndvi", con.datos = FALSE)
    if (! is.null(capas.producto) && (nrow(capas.producto) > 0)) {
      ultima.fecha <- capas.producto %>%
        dplyr::pull(fecha) %>%
        max()
      fecha.desde <- ultima.fecha + lubridate::days(1)
      rm (ultima.fecha)
    } else {
      # No hay capas descargadas para el producto. Comenzar a procesar desde la fecha minima
      # La fecha minima esta definida como un offset en relacion a la fecha.hasta
      fecha.desde <- fecha.hasta - lubridate::days(config$descarga$maxima.cantidad.dias)
    }
    rm(capas.producto)
  } else if (! is.null(config$descarga$fechas$fijo)) {
    fecha.desde <- as.Date(config$descarga$fechas$fijo[1])
    fecha.hasta <- as.Date(config$descarga$fechas$fijo[2])
    if (is.null(fecha.desde) || is.null(fecha.hasta) || (fecha.desde >= fecha.hasta)) {
      script$error("Cuando el intervalo de fechas es fijo debe especificarse un vector de 2 fechas (desde y hasta), considerando que la primera debe ser anterior a la última")
      DBI::dbDisconnect(con)
      script$stop()   
    }
  } else {
    # Finalizar script
    script$error("Debe especificar un intervalor de fechas (incremental o fijo)")
    DBI::dbDisconnect(con)
    script$stop() 
  }
  
  # Definir la extension del campo a partir de la capa para hacer la busqueda
  roi <- raster::extent(campo)
  bounding_box <- paste(roi[1], roi[3], roi[2], roi[4], sep = ',')
  
  
  # Definir fechas para hacer la busqueda
  # Crear una secuencia de fechas con intervalos de seis meses
  fechas <- seq(from = fecha.desde, to = fecha.hasta, by = "6 months")
  
  # Truncar la secuencia para asegurarse de que no exceda fecha_hasta
  fechas <- fechas[fechas <= fecha.hasta]
  
  # Si la última fecha en la secuencia es menor que fecha_hasta, añadir fecha.hasta
  if (fechas[length(fechas)] < fecha.hasta) {
    fechas <- c(fechas, fecha.hasta)
  }
  
  # Crear el data frame con los rangos de fechas
  rango.fechas <- tibble(
    fecha.desde = fechas[-length(fechas)],
    fecha.hasta = fechas[-1]
  )
  
  # Generar todos los registros para el rango de fechas deseado
  registros <- purrr::map_dfr(
    .x = 1:nrow(rango.fechas),
    .f = function(rango) {
      
      # Filtrar el rango de fecha sobre el que iterar
      rango.fecha <- rango.fechas[rango,]
      
      # Formato: # YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ
      # Tenemos que ver porque la consulta puede devolver hasta 250 
      # imagenes por descarga.
      # Formatear las fechas en el formato deseado
      # Combinar las fechas en un solo string
      fechas.buscar <- paste0(format(rango.fecha$fecha.desde, "%Y-%m-%dT%H:%M:%SZ"), "/", format(rango.fecha$fecha.hasta, "%Y-%m-%dT%H:%M:%SZ"))
      
      # Body para hacer el request a la API
      poigono_search_body <- list(limit = 150, # Cantidad maxima de imagenes a devolver. Tenemos que explorar mas este numero. 
                                  datetime=fechas.buscar, # Fechas a buscar
                                  bbox= bounding_box, # Area de interes
                                  collections= HLS_col) # Productos a descargar. 
      # Buscar imagenes para descargar del campo 
      poligono_search_request <- httr::POST(search_URL, body = poigono_search_body, encode = "json") %>% 
        httr::content(as = "text") %>% 
        jsonlite::fromJSON()
      
      # Extraer features del request
      search_features <- poligono_search_request$features
      
      # Preparar lista con las imagenes/bandas a descargar para el calculo del NDVI. 
      # Se deberian descargar otras para el calculo del NDRE
      granule_list <- list()
      n <- 1
      for (imagen in row.names(search_features)) {                       # Extraer NIR, Red y Quality band layer 
        # Las bandas para el calculo de NDVI y Landast son diferente. 
        # El espectro electromagnetico esta homogeneizado pero el 
        # nombre de las bandas es diferente
        if (search_features[imagen,]$collection == 'HLSS30.v2.0') {
          bandas <- c('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'B08', 'B8A', 'B11', 'B12', 'Fmask')
        } else{
          bandas <- c('B01', 'B02', 'B03', 'B04', 'B05', 'B06', 'B07', 'Fmask')
        }
        # Iterar por las bandas
        for(banda in bandas) {
          # Imagen o feature a descargar
          f <- search_features[imagen,]
          # Extraer url correspondiente a la banda
          b_assets <- f$assets[[banda]]$href
          # Crear data frame con las imagenes y bandas a descargar
          registro <- data.frame(Collection = f$collection,                   
                                 Granule_ID = f$id,
                                 Cloud_Cover = f$properties$`eo:cloud_cover`,
                                 band = banda,
                                 fecha =  f$properties$datetime,
                                 Asset_Link = b_assets, stringsAsFactors=FALSE)
          granule_list[[n]] <- registro
          n <- n + 1
        }
      }
      
      registros <- do.call(rbind, granule_list)
    }
  )
  
  # Filtrar las imagenes con mas de 80% de cobertura nubosa
  registros <- registros[registros$Cloud_Cover < 80, ]
  
  # Filtrar registros para evitar descargas dentro del 
  # mismo día. Si hay datos para ambas colecciones,
  # elegir Sentinel. Además se filtran las imágenes
  # con el menor numero de nubes. Ademas, ante bandas 
  # duplicadas, seleccionar la imagen más reciente
  registros %<>%
    dplyr::group_by(fecha_dia = as.Date(fecha)) %>%
    dplyr::mutate(cantidad_imagenes = length(unique(Granule_ID))) %>%
    dplyr::filter(cantidad_imagenes == 1 |
                    (cantidad_imagenes > 1 &
                       Collection == "HLSS30.v2.0" &
                       Cloud_Cover == min(Cloud_Cover) &
                       fecha == min(fecha)))
  
  # Determinar fechas sobre las que iterar
  fechas.procesables <- unique(registros$fecha) 
  
  # Descargar imagenes
  for (fecha in fechas.procesables) {
    
    # Extraer bandas para esa fecha
    registro <- registros %>%
      dplyr::filter(fecha == !!fecha)
    
    # Si hay mas de una imagen para la misma coleccion,
    # y tienen la misma cantidad de nubes, quedarse
    # con la primera
    if (anyDuplicated(registro$band) > 0) {
      registro %<>%
        dplyr::group_by(band) %>%
        dplyr::slice(1)
    }
    
    script$info(paste0("... Descargando imagen para fecha ", as.Date(fecha)))
    
    bandas.campo <- list()
    for (banda in seq(length(registro$band))) {
      
      imagen <- NULL
      while (is.null(imagen)) {
        tryCatch({
          # Intentar descargar el raster
          imagen <- terra::rast(registro$Asset_Link[banda]) %>%
            terra::project(config$proj4string$utm)
        }, error = function(e) {
          # En caso de error, concatenar el mensaje de error
          error_message <- paste("Error al descargar o proyectar el raster:", e$message)
          # Llamar al método warn de script
          script$warn(error_message)
          Sys.sleep(5)  # Esperar 5 segundos antes de intentar nuevamente (puedes ajustar este valor)
        })
      }
      
      # Recortar imagen usando el shape del campo
      imagen <- raster::mask(raster::crop(imagen, extent(campo.utm)), campo.utm)
      # Extraer nombre de la imagen y banda
      nombre <- basename(registro$Asset_Link[banda])
      # Agregar dimension temporal al raster
      terra::time(imagen) <- lubridate::as_datetime(fecha)
      # Renombrar banda
      if (stringr::str_detect(nombre, "Fmask")) {
        names(imagen) <- "Fmask"
      }
      # Guardar banda 
      bandas.campo[[banda]] <- imagen
    }
    bandas.campo <- terra::rast(bandas.campo)
    
    # Aplicar QA
    if (!is.null(bandas.campo)) {
      
      script$info(paste0("... Aplicando QA para fecha ", as.Date(fecha)))
      
      # Obtener los valores de calidad
      calidad_values <- values(bandas.campo$Fmask)
      
      # Decodificar los bits relevantes
      bit_nube <- 1
      bit_sombra <- 3
      bit_nieve <- 4
      bit_adyacente <- 2
      
      # Crear máscaras para cada condición
      nubes_mask <- decode_bit(calidad_values, bit_nube) == 1
      sombra_mask <- decode_bit(calidad_values, bit_sombra) == 1
      nieve_mask <- decode_bit(calidad_values, bit_nieve) == 1
      adyacente_mask <- decode_bit(calidad_values, bit_adyacente) == 1
      
      # Combinar las máscaras para obtener una máscara de píxeles no deseados
      mask_combined <- nubes_mask | sombra_mask | nieve_mask | adyacente_mask 
      
      # Invertir la máscara para obtener píxeles limpios
      mask_clean <- !mask_combined
      
      # Crear un raster a partir de la máscara
      mask_clean_raster <- rast(matrix(mask_clean, nrow = nrow(bandas.campo$Fmask), ncol = ncol(bandas.campo$Fmask)), 
                                crs = crs(bandas.campo$Fmask), extent = ext(bandas.campo$Fmask))
      
      # Aplicar QA
      bandas.campo <- mask(bandas.campo, mask_clean_raster, maskvalue = 0)
      
      # Eliminar objetos innecesarios
      rm(mask_clean_raster, mask_clean, mask_combined, nubes_mask, sombra_mask, nieve_mask, adyacente_mask); gc(); terra::free_RAM()
      
      # Eliminar las bandas con la mascara
      bandas.campo <- bandas.campo[[!names(bandas.campo) %in% "Fmask"]]
      
    }
    
    # Calcular NDVI
    if ('ndvi' %in% config$producto_descarga) {
      # Obtener capa de NDVI
      ndvi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.ndvi <- c("Red", "NIR_Narrow")
            stack.ndvi <- terra::subset(bandas.campo, bandas.ndvi)
            ndvi.output <- calcular_NDVI(stack.ndvi$NIR_Narrow, stack.ndvi$Red)
          } else {
            bandas.ndvi <- c("Red", "NIR")
            stack.ndvi <- terra::subset(bandas.campo, bandas.ndvi)
            ndvi.output <- calcular_NDVI(stack.ndvi$NIR, stack.ndvi$Red)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de NDVI para ", unique(registro$Granule_ID), ": ", e$message))
          ndvi.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(ndvi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- ndvi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de NDVI para fecha ", as.Date(fecha)))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.ndvi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                        nombre = "ndvi", 
                                                        campana = campana, 
                                                        fecha = as.Date(fecha),
                                                        con.datos = FALSE)
            if (! is.null(capa.datos.ndvi) && (nrow(capa.datos.ndvi) > 0)) {
              script$info(sprintf("... Datos de NDVI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.ndvi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'ndvi', 
                                             cultivo = NA, campana = campana, variable = 'NDVI', fecha = as.Date(fecha))
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de NDVI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de NDVI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de NDVI nulo ", unique(registro$Granule_ID)))
      }
      rm(ndvi.output)
    }
    
    # Calcular NDRE
    if ('ndre' %in% config$producto_descarga) {
      # Obtener capa de NDRE
      ndre.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.ndre <- c("Red_Edge1", "NIR_Narrow")
            stack.ndre <- terra::subset(bandas.campo, bandas.ndre)
            ndre.output <- calcular_NDRE(stack.ndre$NIR_Narrow, stack.ndre$Red_Edge1)
          } else {
            break
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de NDVI para ", unique(registro$Granule_ID), ": ", e$message))
          ndre.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(ndre.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- ndre.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de NDRE para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.ndre <- capa.datos.facade$buscar(campo, id = NULL, 
                                                        nombre = "ndre", 
                                                        campana = campana, 
                                                        fecha = fecha,
                                                        con.datos = FALSE)
            if (! is.null(capa.datos.ndre) && (nrow(capa.datos.ndre) > 0)) {
              script$info(sprintf("... Datos de NDRE ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.ndre$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'ndre', 
                                             cultivo = NA, campana = campana, variable = 'NDRE', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de NDRE: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de NDRE por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de NDRE nulo ", unique(registro$Granule_ID)))
      }
      rm(ndre.output)
    }
    
    # Calcular GNDVI
    if ('gndvi' %in% config$producto_descarga) {
      # Obtener capa de NDVI
      gndvi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.gndvi <- c("Green", "NIR_Narrow")
            stack.gndvi <- terra::subset(bandas.campo, bandas.gndvi)
            gndvi.output <- calcular_GNDVI(stack.gndvi$NIR_Narrow, stack.gndvi$Green)
          } else {
            bandas.ndvi <- c("Green", "NIR")
            stack.gndvi <- terra::subset(bandas.campo, bandas.gndvi)
            gndvi.output <- calcular_GNDVI(stack.gndvi$NIR, stack.gndvi$Green)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de GNDVI para ", unique(registro$Granule_ID), ": ", e$message))
          ndvi.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(gndvi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- gndvi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de GNDVI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.gndvi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                         nombre = "gndvi", 
                                                         campana = campana, 
                                                         fecha = fecha,
                                                         con.datos = FALSE)
            if (! is.null(capa.datos.gndvi) && (nrow(capa.datos.gndvi) > 0)) {
              script$info(sprintf("... Datos de GNDVI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.gndvi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'gndvi', 
                                             cultivo = NA, campana = campana, variable = 'GNDVI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de GNDVI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de GNDVI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de GNDVI nulo ", unique(registro$Granule_ID)))
      }
      rm(gndvi.output)
    }
    
    # Calcular CIGreen
    if ('ci-green' %in% config$producto_descarga) {
      # Obtener capa de NDVI
      cigreen.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.cigreen <- c("Green", "NIR_Narrow")
            stack.cigreen <- terra::subset(bandas.campo, bandas.cigreen)
            cigreen.output <- calcular_CIgreen(stack.cigreen$NIR_Narrow, stack.cigreen$Green)
          } else {
            bandas.cigreen <- c("Green", "NIR")
            stack.cigreen <- terra::subset(bandas.campo, bandas.cigreen)
            cigreen.output <- calcular_CIgreen(stack.cigreen$NIR, stack.cigreen$Green)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de CiGreen para ", unique(registro$Granule_ID), ": ", e$message))
          cigreen.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(cigreen.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- cigreen.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de CiGreen para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.cigreen <- capa.datos.facade$buscar(campo, id = NULL, 
                                                           nombre = "ci-green", 
                                                           campana = campana, 
                                                           fecha = fecha,
                                                           con.datos = FALSE)
            if (! is.null(capa.datos.cigreen) && (nrow(capa.datos.cigreen) > 0)) {
              script$info(sprintf("... Datos de CiGreen ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.cigreen$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'ci-green', 
                                             cultivo = NA, campana = campana, variable = 'Ci-Green', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de CiGreen: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de CiGreen por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de CiGreen nulo ", unique(registro$Granule_ID)))
      }
      rm(cigreen.output)
    }
    
    # Calcular MCARI
    if ('mcari' %in% config$producto_descarga) {
      # Obtener capa de NDVI
      mcari.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.mcari <- c("Green", "Red", "NIR_Narrow")
            stack.mcari <- terra::subset(bandas.campo, bandas.mcari)
            mcari.output <- calcular_MCARI(stack.mcari$Red, stack.mcari$Green, stack.mcari$NIR_Narrow)
          } else {
            bandas.mcari <- c("Green", "Red", "NIR")
            stack.mcari <- terra::subset(bandas.campo, bandas.mcari)
            mcari.output <- calcular_MCARI(stack.mcari$Red, stack.mcari$Green, stack.mcari$NIR)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de MCARI para ", unique(registro$Granule_ID), ": ", e$message))
          mcari.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(mcari.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- mcari.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de MCARI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.mcari <- capa.datos.facade$buscar(campo, id = NULL, 
                                                         nombre = "mcari", 
                                                         campana = campana, 
                                                         fecha = fecha,
                                                         con.datos = FALSE)
            if (! is.null(capa.datos.mcari) && (nrow(capa.datos.mcari) > 0)) {
              script$info(sprintf("... Datos de MCARI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.mcari$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'mcari', 
                                             cultivo = NA, campana = campana, variable = 'MCARI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de MCARI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de MCARI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de MCARI nulo ", unique(registro$Granule_ID)))
      }
      rm(mcari.output)
    }
    
    # Calcular NDWI
    if ('ndwi' %in% config$producto_descarga) {
      # Obtener capa de NDVI
      ndwi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.ndwi <- c("Green", "NIR_Narrow")
            stack.ndwi <- terra::subset(bandas.campo, bandas.ndwi)
            ndwi.output <- calcular_NDWI(stack.ndwi$NIR_Narrow, stack.ndwi$Green)
          } else {
            bandas.ndwi <- c("Green", "NIR")
            stack.ndwi <- terra::subset(bandas.campo, bandas.mcari)
            ndwi.output <- calcular_NDWI(stack.ndwi$Green, stack.ndwi$NIR)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de NDWI para ", unique(registro$Granule_ID), ": ", e$message))
          ndwi.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(ndwi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- ndwi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de NDWI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.ndwi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                        nombre = "ndwi", 
                                                        campana = campana, 
                                                        fecha = fecha,
                                                        con.datos = FALSE)
            if (! is.null(capa.datos.ndwi) && (nrow(capa.datos.ndwi) > 0)) {
              script$info(sprintf("... Datos de NDWI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.ndwi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'ndwi', 
                                             cultivo = NA, campana = campana, variable = 'NDWI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de NDWI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de NDWI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de NDWI nulo ", unique(registro$Granule_ID)))
      }
      rm(ndwi.output)
    }
    
    # Calcular NDMI
    if ('ndmi' %in% config$producto_descarga) {
      # Obtener capa de NDMI
      ndmi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular NDVI
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.ndmi <- c("SWIR1", "NIR_Narrow")
            stack.ndmi <- terra::subset(bandas.campo, bandas.ndmi)
            ndmi.output <- calcular_NDMI(stack.ndmi$NIR_Narrow, stack.ndmi$SWIR1)
          } else {
            bandas.ndwi <- c("SWIR1", "NIR")
            stack.ndmi <- terra::subset(bandas.campo, bandas.ndmi)
            ndmi.output <- calcular_NDMI(stack.ndwi$NIR, stack.ndwi$SWIR1)
          }
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de NDMI para ", unique(registro$Granule_ID), ": ", e$message))
          mcari.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(ndmi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- ndmi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de NDMI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.ndmi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                        nombre = "ndmi", 
                                                        campana = campana, 
                                                        fecha = fecha,
                                                        con.datos = FALSE)
            if (! is.null(capa.datos.ndmi) && (nrow(capa.datos.ndmi) > 0)) {
              script$info(sprintf("... Datos de NDMI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.ndmi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'ndmi', 
                                             cultivo = NA, campana = campana, variable = 'NDMI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de NDMI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de NDMI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de NDMI nulo ", unique(registro$Granule_ID)))
      }
      rm(ndmi.output)
    }
    
    # Calcular MSI
    if ('msi' %in% config$producto_descarga) {
      # Obtener capa de NDMI
      msi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.msi <- c("SWIR1", "NIR_Broad")
            stack.msi <- terra::subset(bandas.campo, bandas.msi)
            msi.output <- calcular_MSI(stack.msi$NIR_Broad, stack.msi$SWIR1)
          } else {
            break
          }
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de MSI para ", unique(registro$Granule_ID), ": ", e$message))
          msi.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(msi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- msi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de MSI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.msi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                       nombre = "msi", 
                                                       campana = campana, 
                                                       fecha = fecha,
                                                       con.datos = FALSE)
            if (! is.null(capa.datos.msi) && (nrow(capa.datos.msi) > 0)) {
              script$info(sprintf("... Datos de MSI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.msi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'msi', 
                                             cultivo = NA, campana = campana, variable = 'MSI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de MSI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de MSI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de MSI nulo ", unique(registro$Granule_ID)))
      }
      rm(msi.output)
    }
    
    # Calcular SIPI
    if ('sipi' %in% config$producto_descarga) {
      # Obtener capa de NDMI
      sipi.output   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          if (unique(registro$Collection) == "HLSS30.v2.0") {
            bandas.sipi <- c("Coastal_Aerosol", "NIR_Broad", "Red")
            stack.sipi <- terra::subset(bandas.campo, bandas.sipi)
            sipi.output <- calcular_SIPI(stack.sipi$Coastal_Aerosol, stack.sipi$NIR_Broad, stack.sipi$Red)
          } else {
            break
          }
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de SIPI para ", unique(registro$Granule_ID), ": ", e$message))
          sipi.output <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(sipi.output)) {
        # Transformar a raster y obtener la fecha
        raster.campo <- sipi.output
        fecha        <- terra::time(raster.campo)
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.campo, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo)
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de SIPI para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.sipi <- capa.datos.facade$buscar(campo, id = NULL, 
                                                        nombre = "sipi", 
                                                        campana = campana, 
                                                        fecha = fecha,
                                                        con.datos = FALSE)
            if (! is.null(capa.datos.sipi) && (nrow(capa.datos.sipi) > 0)) {
              script$info(sprintf("... Datos de SIPI ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.sipi$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRaster(campo = campo, raster.capa = raster::raster(raster.campo), nombre = 'sipi', 
                                             cultivo = NA, campana = campana, variable = 'SIPI', fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de SIPI: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de SIPI por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.campo, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de SIPI nulo ", unique(registro$Granule_ID)))
      }
      rm(sipi.output)
    }
    
    # Calcular RGB
    if ('rgb' %in% config$producto_descarga) {
      # Obtener capa de NDMI
      stack.rgb   <- NULL
      if (! is.null(bandas.campo)) {
        tryCatch({
          # Calcular RGB
          bandas.rgb <- c("Red", "Green", "Blue")
          stack.rgb <- terra::subset(bandas.campo, bandas.rgb)
          
        }, error = function(e) {
          script$error(paste0("Error al calcular la capa de RGB para ", unique(registro$Granule_ID), ": ", e$message))
          stack.rgb <<- NULL
        })
      }
      
      # Almacenar capa de datos
      if (! is.null(stack.rgb)) {
        # Transformar a raster y obtener la fecha
        raster.rgb   <- stack.rgb %>% raster::stack()
        fecha        <- terra::time(bandas.campo) %>% unique()
        ano.inicio   <- ifelse(lubridate::month(fecha) <= 4, lubridate::year(fecha) - 1, lubridate::year(fecha))
        campana      <- paste0(ano.inicio, "-", (ano.inicio+1))
        
        # Definir nombres de variables
        variables         <- c("rojo", "verde", "azul")
        names(raster.rgb) <- variables
        
        # Extraer datos dentro del campo y contar la cantidad de NAs
        datos.campo   <- unlist(raster::extract(x = raster.rgb, y = campo.utm))
        porcentaje.na <- 100 * length(which(is.na(datos.campo))) / length(datos.campo) 
        
        if (porcentaje.na < config$maximo.porcentaje.faltantes$campo) {
          # Guardar raster en base de datos
          script$info(paste0("... Guardando datos de RGB para fecha ", fecha))
          tryCatch({
            # Determinar si la fecha ya existe en la base de datos
            capa.datos.rgb <- capa.datos.facade$buscar(campo, id = NULL, 
                                                       nombre = "rgb", 
                                                       campana = campana, 
                                                       fecha = fecha,
                                                       con.datos = FALSE)
            if (! is.null(capa.datos.rgb) && (nrow(capa.datos.rgb) > 0)) {
              script$info(sprintf("... Datos de RGB ya existentes para fecha %s. Reemplazando.", 
                                  as.character(fecha)))
              
              # Borrar
              capa.datos.facade$borrar(capa_dato_id = as.integer(capa.datos.rgb$id))
            }
            
            # Insertar datos
            capa.datos.facade$insertarRasterStack(campo = campo, raster.stack = raster.rgb, nombre = 'rgb', 
                                                  cultivo = NA, campana = campana, fecha = fecha)
          }, error = function(e) {
            script$error(paste0("Error al insertar el raster de RGB: ", as.character(e)))
          })
        } else {
          script$info(paste0("... Descartando datos de RGB por cantidad de faltantes ", unique(registro$Granule_ID)))
        }
        rm(raster.rgb, fecha, datos.campo, porcentaje.na)  
      } else {
        script$info(paste0("... Descartando datos por producir archivo de RGB nulo ", unique(registro$Granule_ID)))
      }
    }
  }
  # Limpiar variables
  gc(full = TRUE)
  
}

# Cerrar conexion a base de datos
DBI::dbDisconnect(con)

# Finalizar script
script$stop()
# ------------------------------------------------------------------------------