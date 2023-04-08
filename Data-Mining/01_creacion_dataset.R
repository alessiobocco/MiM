# -----------------------------------------------------------------------------#
# --- PASO 1. Cargar paquetes necesarios ----
rm(list = ls()); gc()
Sys.setenv(TZ = "UTC")
list.of.packages <- c("dplyr", "magrittr", "glue", "purrr", "stringr", 
                      "yaml", "readr")
for (pack in list.of.packages) {
  if (!require(pack, character.only = TRUE)) {
    stop(paste0("Paquete no encontrado: ", pack))
  }
}
rm(pack, list.of.packages); gc()
# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 2. Leer archivos YML de configuracion y parametros ----

# i. Leer YML de configuracion
args <- base::commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  archivo.config <- args[1]
} else {
  # No vino el archivo de configuracion por linea de comandos.
  # Utilizo un archivo default
  archivo.config <- paste0(getwd(), "/configuracion.yml")
}

if (! file.exists(archivo.config)) {
  stop(paste0("El archivo de configuracion de ", archivo.config, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de configuracion ", archivo.config, "...\n"))
  config <- yaml::yaml.load_file(archivo.config)
}

# ii. YML de parametros para los controles de calidad
if (length(args) > 1) {
  archivo.params <- args[2]
} else {
  # No vino el archivo de parametros por linea de comandos.
  # Utilizo un archivo default
  archivo.params <- paste0(getwd(), "/parametros.yml")
}
if (! file.exists(archivo.params)) {
  stop(paste0("El archivo de parametros de ", archivo.params, " no existe\n"))
} else {
  cat(paste0("Leyendo archivo de parametros ", archivo.params, "...\n"))
  config$params <- yaml::yaml.load_file(archivo.params)
}

rm(archivo.config, archivo.params, args); gc()

# ------------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# --- PASO 3: Leer archivos ----
# -----------------------------------------------------------------------------#

# Si el archivo de entrenamiento no existe, el código continúa ejecutándose.
if (!fs::file_exists(glue::glue("{config$dir$data}/{config$params$data$training$training_data}"))) {
  
  # Se define la ruta del archivo ZIP que contiene los datos de entrenamiento comprimidos.
  archivo_zip <- glue::glue("{config$dir$data}/{config$params$data$compressed}")
  # Se extrae la lista de contenido del archivo ZIP y se convierte en un dataframe.
  contenido_zip <- unzip(zipfile = archivo_zip, list = TRUE) %>%
    as.data.frame()
  # Se filtra el nombre del archivo que contiene la variable objetivo.
  target_file <- contenido_zip %>%
    dplyr::filter(stringr::str_detect(Name, "train_contacts")) %>%
    dplyr::pull(Name)
  # Se lee el archivo que contiene la variable objetivo.
  con <- unz(description = archivo_zip, filename = target_file)
  target <- read.csv(con)
  # Se filtran los nombres de archivo que contienen las características de los anuncios.
  features_file <- contenido_zip %>%
    dplyr::filter(stringr::str_detect(Name, "^competition_data/ads_data/.+$")) %>%
    dplyr::pull(Name)
  # Lectura de los features para cada pais y mes/año
  training_data <- purrr::map_dfr(
    .x = features_file,
    .f = function(archivo) {
      # Se lee cada archivo y se une con la variable objetivo.
      
      # Se definen los tipos de datos de las columnas del dataframe.
      tipos_columna <- cols(.default = "c", ad_id = "i", lat = "d", lon = "d", price = "d",
                            price_usd = "d", rooms = "i", bedrooms = "i",
                            bathrooms = "i", surface_total = "i", surface_covered = "i",
                            created_on = "T", property_is_development = "l")
      
      # Se leen los datos de cada archivo, se filtran las filas según la fecha especificada,
      # se unen los datos de la variable objetivo, y se seleccionan las columnas deseadas.
      con <- unz(description = archivo_zip, filename = archivo)
      datos <- readr::read_delim(con, col_types = tipos_columna, delim = "\t") %>%
        dplyr::filter(created_on <= as.POSIXct("2022-09-15 00:00:00", format = "%Y-%m-%d %H:%M:%S")) %>%
        dplyr::left_join(target) %>%
        dplyr::select(contacts, ad_id, everything())
       
    }
  )
  # Se reemplazan los valores NA de la variable objetivo con 0.
  training_data %<>%
    dplyr::mutate(contacts = if_else(is.na(contacts), 0L, contacts))
  # Se escribe el dataframe en formato FST en la ruta especificada.
  fst::write_fst(training_data, 
                 glue::glue("{config$dir$data}/{config$params$data$training$training_data}"),
                 compress = 99)
} else{
  # Leer archivo de entrenamiento
  training_data <- fst::read.fst(glue::glue("{config$dir$data}/{config$params$data$training$training_data}"))
}

# -----------------------------------------------------------------------------
