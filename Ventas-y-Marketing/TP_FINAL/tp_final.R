# ---------------------------------------------------------------------------- #
# Paso 0: Limpiar espacio de trabajo ----
# ---------------------------------------------------------------------------- #

rm(list = ls()); gc()

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 1: Cargar paquetes necesarios ----
# ---------------------------------------------------------------------------- #

# Instalar el paquete pacman que permite instalar y/o cargar los paquetes necesarios
if (!require("pacman")) install.packages("pacman", repos = 'http://cran.us.r-project.org')

# Instalar o cargar los paquetes necesarios
pacman::p_load("dplyr", "readxl", "ggplot2", "knitr", "modelsummary",
               "equatiomatic", "magrittr", "furrr", "papeR", "purrr", "conjoint",
               "makedummies", "lessR", "forcats", "factoextra", "NbClust", "DT",
               "stringr")

options(bitmapType="cairo")

# Agregar funciones propias
p_report <- function(x){
  
  # create an object "e" which contains x, the p value you are reporting,
  # rounded to 3 decimal places
  
  e <- round(x, digits = 3)
  
  # the next two lines of code prints "< .001" if x is indeed less than .001
  
  if (x < 0.001)
    print(paste0("<", " ", ".001"))
  
  # if x is greater than .001, the code below prints the object "e"
  # with an "=" sign, and with the preceeding zero removed
  
  else
    print(
      paste0("=",
             " ",
             sub("^(-?)0.", "\\1.", sprintf("%.3f",e))))
  
}

# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------#
# PASO 2. Leer archivos YML de configuracion y parametros ----

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

# ---------------------------------------------------------------------------- #
# Paso 3: Generar perfiles para el cuestionario ----
# ---------------------------------------------------------------------------- #

# En esta sección se general el diseño factorial

# Generar perfiles si no han sido ya generados
if (!fs::is_file(glue::glue("{config$dir$output}/{config$params$data$cuestionario}"))) {
  
  # Crear vector con los atributos del producto
  atributos <- expand.grid(
    Leche <- config$params$atributos$leche,
    Tratamiento <- config$params$atributos$tratamiento,
    Textura <- config$params$atributos$textura,
    Sabor <- config$params$atributos$sabor,
    Packaging <- config$params$atributos$packaging,
    Tamaño <- config$params$atributos$tamaño,
    Precio <- config$params$atributos$precio) %>%
    purrr::set_names(c("Leche", "Tratamiento", "Textura", "Sabor", "Packaging", "Tamaño", "Precio"))
  
  # Crear diseño factorial
  perfiles <- conjoint::caFactorialDesign(data = atributos, type="fractional")
  codificacion <- conjoint::caEncodedDesign(perfiles)
  diseño.codificacdo <- data.frame(perfiles, codificacion)
  
  # Guardar resultados 
  write.csv(diseño.codificacdo, glue::glue("{config$dir$output}/{config$params$data$cuestionario}"))
  
}
# ---------------------------------------------------------------------------- 

# ---------------------------------------------------------------------------- #
# Paso 4: Cargar resultados del cuestionario ----
# ---------------------------------------------------------------------------- #

resultados.cuestionario <- readxl::read_xlsx(glue::glue("{config$dir$data}/{config$params$data$respuestas}")) %>%
  dplyr::select(-`Marca temporal`) %>%
  t() %>%
  tibble::as_tibble() %>%
  purrr::set_names(lessR::to("invidiuo.", until = 19)) %>%
  dplyr::mutate(perfil = paste0("perfil.", 1:n())) %>%
  dplyr::select(perfil, everything())

perfiles <- readr::read_csv(glue::glue("{config$dir$output}/{config$params$data$cuestionario}")) %>%
  dplyr::select(Leche, Tratamiento,  Textura, Sabor, Packaging, Tamaño, Precio) %>%
  purrr::set_names(stringr::str_to_lower(names(.)))%>%
  dplyr::mutate(perfil = paste0("perfil.", 1:n())) %>%
  dplyr::select(perfil, everything())

# ---------------------------------------------------------------------------- 

# ---------------------------------------------------------------------------- #
# Paso 5: Prepara la matriz de datos para el analisis ----
# ---------------------------------------------------------------------------- #

# Manipilar la base de datos para realizar los análisis
datos <- perfiles %>%
  dplyr::left_join(resultados.cuestionario) %>%
  tidyr::pivot_longer(cols = starts_with("invidiuo"), names_to = "individuo", values_to = "rating") %>% 
  # Corregir guiones o espacios en blanco en variables
  dplyr::mutate(across(where(is.character), stringr::str_replace_all, pattern = "-", replacement = "_")) %>%
  dplyr::mutate(across(where(is.character), stringr::str_replace_all, pattern = fixed(" "), replacement = "_")) %>%
  dplyr::mutate(dplyr::across(!c(rating, individuo), factor)) 

# Crear variables dummy. Se excluye el nivel de base
datos_modelo <- datos %>%
  dplyr::select(-perfil) %>%
  fastDummies::dummy_cols(remove_first_dummy = TRUE, 
                          remove_selected_columns = TRUE,
                          select_columns = names(config$params$atributos)) 
# ---------------------------------------------------------------------------- 

# ---------------------------------------------------------------------------- #
# Paso 6: Análisis de los encuestados individuales ----
# ---------------------------------------------------------------------------- #

# Modelo general
modelo_general <- datos_modelo %>%
  dplyr::filter(individuo == "invidiuo.01") %>%
  dplyr::select(-individuo) %>%
  lm(rating ~ ., data = .) 


# Iterar por los distintos individuos para ajustar modelos individuales
regresiones.individuales <- purrr::map_dfr(
  .x = unique(datos_modelo$individuo),
  .f = function(individuo) {
    
    datos_modelo %>%
      dplyr::filter(individuo == !!individuo) %>%
      dplyr::select(-individuo) %>%
      lm(rating ~ ., data = .) %>%
      broom::tidy() %>%
      dplyr::mutate(encuestado = individuo) %>%
      dplyr::select(encuestado, everything())
  }
)

boxplot.coeficientes <- regresiones.individuales %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = term, y = estimate)) +
  ggplot2::geom_boxplot() +
  ggplot2::theme_bw() +
  ggplot2::labs(x = "Coeficiente", y = "Valor") +
  theme(axis.text.x = element_text(angle = 45,  hjust=1))

significancia.coeficientes <- regresiones.individuales %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = `p.value`)) +
  ggplot2::geom_density() +
  ggplot2::facet_wrap(~term) +
  ggplot2::geom_vline(xintercept = 0.05, linetype = 'dashed') +
  ggplot2::theme_bw() +
  ggplot2::labs(x = "p-valor", y = "Densidad") +
  theme(axis.text.x = element_text(angle = 45,  hjust=1))

# ---------------------------------------------------------------------------- 

# ---------------------------------------------------------------------------- #
# Paso 7: Importancia relativa de cada atributo ----
# ---------------------------------------------------------------------------- #

atributos <- purrr::map_dfr(
  .x = unique(datos_modelo$individuo),
  .f = function(individuo) {
    
    data.frame(factor = unlist(config$params$atributos)) %>%
      dplyr::mutate(atributo = stringr::str_remove_all(row.names(.), "[:digit:]")) %>%
      `rownames<-`(NULL) %>%
      dplyr::mutate(coeficiente = paste0(atributo, "_", factor)) %>%
      dplyr::select(variable = atributo, coeficiente, factor) %>%
      dplyr::mutate(across(where(is.character), stringr::str_replace_all, pattern = "-", replacement = "_")) %>%
      dplyr::mutate(across(where(is.character), stringr::str_replace_all, pattern = fixed(" "), replacement = "_")) %>%
      dplyr::mutate(encuestado = !!individuo) %>%
      dplyr::select(encuestado, everything())

  }
)


utilidad.encuestado <- atributos  %>%
  dplyr::full_join(regresiones.individuales %>% dplyr::rename(coeficiente = term)) %>%
  dplyr::mutate(individuo = str_match(encuestado, "[0-9]+") %>% unlist %>% as.numeric) %>%
  dplyr::arrange(individuo, coeficiente) %>%
  dplyr::mutate(estimate = if_else(is.na(estimate), 0, estimate)) %>%
  dplyr::select(encuestado, variable, nivel = factor, coeficiente, estimate, p.value) %>%
  dplyr::mutate(variable = if_else(is.na(variable) & variable != '(Intercept)', str_extract(variable, "[^_]+"), variable))
  

importancia.individual.atributos <- utilidad.encuestado %>%
  dplyr::filter(coeficiente != "(Intercept)") %>%
  dplyr::select(encuestado, variable, estimate) %>%
  dplyr::group_by(encuestado, variable) %>%
  dplyr::mutate(rango_variable = max(estimate) - min(estimate)) %>%
  dplyr::group_by(encuestado) %>%
  dplyr::select(-estimate) %>%
  dplyr::distinct() %>%
  dplyr::mutate(rango_total = sum(rango_variable)) %>%
  dplyr::select(variable, rango_variable, rango_total) %>%
  dplyr::mutate(importancia = rango_variable/rango_total) %>%
  dplyr::mutate(individuo = str_match(encuestado, "[0-9]+") %>% unlist %>% as.numeric) %>%
  dplyr::arrange(individuo, desc(importancia))%>%
  dplyr::select(-individuo) %>%
  dplyr::ungroup()

figura.importancia.individual.atributos <- importancia.individual.atributos %>%
  #dplyr::filter(variable != "precio") %>%
  dplyr::mutate(variable = stringr::str_to_title(variable)) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = forcats::fct_inorder(variable, importancia), y = importancia)) +
  ggplot2::geom_bar(stat = 'identity') +
  ggplot2::theme_bw() +
  ggplot2::labs(x = 'Atributo', y = "Importancia relativa") +
  ggplot2::facet_wrap(~encuestado, scales = 'free', ncol = 2)


importancia.individual.atributos.global <- importancia.individual.atributos %>%
  dplyr::select(encuestado, variable, importancia) %>%
  tidyr::spread( variable, importancia) %>%
  dplyr::mutate(across(where(is.numeric), ~ round(. *100, 2))) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(total = as.integer(sum(c_across(where(is.numeric))))) %>%
  dplyr::ungroup()

textcol <- "grey40"
figura.heatmap.importancia <- importancia.individual.atributos %>%
  dplyr::mutate(variable = stringr::str_to_title(variable)) %>%
  ggplot2::ggplot(data = ., aes(x=encuestado, y= variable, fill=importancia)) + 
  ggplot2::geom_tile(colour="white", size=0.25) +
  ggplot2::labs(x ="Individuos", y = "Variables")+
  ggplot2::scale_fill_distiller(name = "Importancia") +
  ggplot2::coord_equal()+
  ggplot2::theme(legend.position="right", legend.direction="vertical",
                 legend.margin=margin(grid::unit(0, "cm")),
                 legend.text=element_text(colour=textcol, size=7, face="bold"),
                 legend.key.height=grid::unit(0.8, "cm"),
                 legend.key.width=grid::unit(0.2, "cm"),
                 axis.text.x=element_text(size=10, colour=textcol, angle = 45, hjust = 1),
                 axis.text.y=element_text(vjust=0.2, colour=textcol),
                 axis.ticks=element_line(size=0.4),
                 plot.background=element_blank(),
                 panel.border=element_blank())

figura.importancia.individual.atributos.media <- importancia.individual.atributos %>%
  dplyr::group_by(encuestado) %>%
  dplyr::mutate(prioridad = 1:n()) %>%
  dplyr::mutate(variable = stringr::str_to_title(variable)) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(prioridad))) +
  ggplot2::geom_bar(aes(y = ..prop.., group = 1)) +
  ggplot2::facet_wrap(~variable, scales = 'free')  +
  ggplot2::scale_y_continuous(labels = scales::percent,  breaks = seq(0, 1, .1)) +
  ggplot2::theme_bw() +
  ggplot2::labs(x = "Prioridad", y = 'Prop. de encuestados')
# ---------------------------------------------------------------------------
  
# --------------------------------------------------------------------------- #
# Paso 8: Valores asociados al precio ----
# --------------------------------------------------------------------------- #

figura.variacion.precio.individua <- utilidad.encuestado %>%
  dplyr::filter(variable == "precio") %>%
  dplyr::mutate(encuestado = stringr::str_to_title(encuestado)) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(nivel), y = estimate, group = 1)) +
  ggplot2::geom_line() +
  ggplot2::theme_bw() +
  ggplot2::labs(x = 'Atributo', y = "Importancia relativa") +
  ggplot2::facet_wrap(~encuestado, scales = 'free', ncol = 2)


# --------------------------------------------------------------------------- 

# --------------------------------------------------------------------------- #
# Paso 9: Disposición a pagar de los consumidores ante cambios en el atributo ----
# --------------------------------------------------------------------------- #

# Precios del producto
precios <- as.numeric(config$params$atributos$precio)
# Rango de precios del producto
diferencia.precios <- max(precios) - min(precios)

cambios.utilidad.individual <- purrr::map_dfr(
  .x = unique(importancia.individual.atributos$encuestado),
  .f = function(individuo) {
    
    variable.mas.importante <- importancia.individual.atributos %>%
      dplyr::filter(encuestado == individuo, variable != "precio") %>%
      dplyr::filter(importancia == max(importancia)) %>%
      dplyr::pull(variable)
    
    # Rango de la variable precio
    rango.precio <- importancia.individual.atributos %>%
      dplyr::filter(encuestado == individuo, variable == "precio") %>%
      dplyr::pull(rango_variable)
    
    nivel.base <-   utilidad.encuestado %>%
      dplyr::filter(encuestado == individuo, variable == variable.mas.importante) %>%
      dplyr::filter(estimate == 0) %>%
      dplyr::pull(nivel)
    
    utilidad.encuestado %>%
      dplyr::filter(encuestado == individuo, 
                    variable == variable.mas.importante,
                    nivel != nivel.base) %>%
      dplyr::select(variable, nivel, utilidad = estimate) %>%
      dplyr::mutate(cambio.utilidad = utilidad * diferencia.precios/rango.precio) %>%
      dplyr::mutate(encuestado = individuo,
                    nivel.base = !!nivel.base) %>%
      dplyr::select(encuestado, variable, nivel, nivel.base, utilidad, cambio.utilidad)
  }
)

# --------------------------------------------------------------------------- 

# --------------------------------------------------------------------------- #
# Paso 10: Segmentacion de encuestados ----
# --------------------------------------------------------------------------- #

datos_cluesting <- regresiones.individuales %>%
  dplyr::select(encuestado, term, estimate) %>%
  tidyr::spread(term, estimate)


# Fijar semilla para asegurar reproducibilidad de resultados
# Determinar el numero de centroides por el metodo del codo
set.seed(1)
wcss <- vector()

figura.metodo.codo <- purrr::map_dfr(
  .x = seq(1, 10, 1),
  .f = function(cluster) {
    
    clustering.resultado <- datos_cluesting %>%
      dplyr::select(-encuestado) %>%
      kmeans(., cluster) 
    wcss <- sum(clustering.resultado$withinss)
    
    data.frame(cluster = cluster,
               wccs = wcss)
    
  }
) %>%
  ggplot2::ggplot(data  = ., ggplot2::aes(x = as.factor(cluster), y = wccs, group = 1)) +
  ggplot2::geom_point() +
  ggplot2::geom_line() +
  ggplot2::labs(x = "Número de centroides", y = "WCSS") +
  ggplot2::theme_bw()

  

indices.clustering <- NbClust::NbClust(datos_cluesting %>%
                   dplyr::select(-encuestado), method = 'kmeans', distance = 'euclidean',
                 min.nc=2, max.nc=5)

figura.indices.clustering <- indices.clustering$Best.nc %>%  
  t() %>%
  as.data.frame() %>%
  dplyr::mutate(index = row.names(.)) %>%
  dplyr::select(index, cluster.optimo = 1, valor = 2) %>%
  `rownames<-`(NULL) %>%
  dplyr::group_by(cluster.optimo) %>%
  dplyr::summarise(cantidad = n()) %>%
  dplyr::filter(cluster.optimo != 0) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(cluster.optimo), y = cantidad)) +
  ggplot2::geom_bar(stat = 'identity') +
  ggplot2::labs(x = "Cantidad de clusters", y = 'Cantidad de índices') +
  ggplot2::theme_bw()

clusters <- datos_cluesting %>%
  dplyr::select(-encuestado) %>%
  kmeans(., 3)

datos.clustering.medios <- datos_cluesting %>%
  dplyr::mutate(cluster = clusters$cluster) %>%
  tidyr::gather(coeficiente, valor, -encuestado, -cluster) %>%
  dplyr::group_by(cluster, coeficiente) %>%
  dplyr::summarise(valor = mean(valor)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(variable = stringr::str_to_sentence(stringr::str_extract(coeficiente, ".+?(?=_)"))) %>%
  dplyr::mutate(nivel = stringr::str_to_sentence(stringr::str_extract(coeficiente, "(?<=_).+"))) %>%
  dplyr::mutate(label = stringr::str_replace_all(nivel, "_", " "),
                # Solo considerar 10 caracteres por linea
                label = stringr::str_wrap(label, 10)) %>%
  dplyr::mutate(variable = if_else(is.na(variable), "Intercepto", variable),
                label = if_else(is.na(label), "Intercepto", label))
    

figura.valores.cluster <- ggplot2::ggplot(data = datos.clustering.medios, ggplot2::aes(x = label, y = valor, fill = factor(cluster))) +
  ggplot2::geom_bar(stat = 'identity', position =  "dodge") +
  ggplot2::scale_fill_brewer(name = 'Grupos') +
  ggplot2::facet_wrap(~variable, scale = 'free') +
  ggplot2::theme_bw() +
  ggplot2::labs(x = 'Coeficiente',  y ="Valor") +
  ggplot2::theme(axis.text.x = element_text(angle = 0, vjust = 0),
                 legend.position = "bottom") 
  

datos_cluesting %>%
  dplyr::mutate(cluster = clusters$cluster) %>%
  dplyr::group_by(cluster) %>%
  dplyr::summarise(cantidad = n())


figura.clusters <- factoextra::fviz_cluster(clusters, data = datos_cluesting %>%
               dplyr::select(-encuestado),
             palette = c("#2E9FDF", "#00AFBB", "#E7B800"), 
             geom = "point",
             ellipse.type = "convex", 
             ggtheme = theme_bw())
# --------------------------------------------------------------------------- 

# --------------------------------------------------------------------------- #
# Paso 11: Nuevos perfiles de productos ----
# --------------------------------------------------------------------------- #

utilidad.competencia <- purrr::map_dfr(
  .x = 1:length(config$params$producto.competencia),
  .f = function(producto) {
    
    # Evaluar producto
    utilidad.poducto.competencia <- config$params$producto.competencia[[producto]] %>%
      as.data.frame() %>%
      t() %>%
      as.data.frame() %>%
      dplyr::mutate(variable = rownames(.),
                    valor = 1) %>%
      dplyr::select(variable, nivel = 1, valor) %>%
      `rownames<-`(NULL) %>%
      dplyr::mutate(coeficiente = paste(variable, nivel),
                    coeficiente = stringr::str_replace_all(coeficiente, fixed(" "), "_")) %>%
      dplyr::add_row(coeficiente = "(Intercept)", valor = 1) %>%
      dplyr::select(coeficiente, valor) %>%
      dplyr::full_join(utilidad.encuestado) %>%
      dplyr::mutate(valor = tidyr::replace_na(valor, 0)) %>%
      dplyr::mutate(utilidad_parcial = estimate*valor) %>%
      dplyr::group_by(encuestado) %>%
      dplyr::summarise(utilidad = sum(utilidad_parcial)) %>%
      #tidyr::drop_na() %>%
      dplyr::ungroup() %>%
      dplyr::mutate(producto = names(config$params$producto.competencia[producto]))
    
  }
)

eleccion.competencia <- utilidad.competencia %>%
  dplyr::group_by(encuestado) %>%
  dplyr::mutate(producto_label = LETTERS[seq(from = 1, to = length(config$params$producto.competencia))]) %>%
  dplyr::arrange(desc(utilidad)) %>%
  dplyr::slice(1) %>%
  dplyr::mutate(producto = paste("Producto ", producto_label))


figura.eleccion.competencia <- ggplot2::ggplot(eleccion.competencia, ggplot2::aes(x = producto)) +
  ggplot2::geom_bar(aes(y = ..prop.., group = 1)) +
  ggplot2::scale_y_continuous(labels = scales::percent,  breaks = seq(0, 1, .1)) +
  ggplot2::theme_bw() +
  ggplot2::labs(x = "Producto", y = 'Prop. de encuestados')
# --------------------------------------------------------------------------- 

# -------------------------------------------------------- #
# ---- Paso n: Generar reporte ----
# -------------------------------------------------------- #


output.dir <- config$dir$output
rmarkdown::render(
  input = glue::glue("{output.dir}/reporte.Rmd"),
  output_file = glue::glue("{output.dir}/informe_final.html"),
  output_dir = output.dir
)

# ----------------------------------------------------------

