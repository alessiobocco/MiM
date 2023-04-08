# ---------------------------------------------------------------------------- #
# Paso 0: Limpieza del espacio de trabajo ----
# ---------------------------------------------------------------------------- #

# Eliminar objetos y limpiar memoria
rm(list = ls()); gc()

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 1: Cargar paquetes necesarios ----
# ---------------------------------------------------------------------------- #

Sys.setenv(TZ = "UTC")
list.of.packages <- c("dplyr", "magrittr", "ggplot2", "purrr", "magrittr",
                      "lubridate", "tidyr", "readr", "funModeling", "kableExtra",
                      "dlookr", "pwr", "gstools", "equatiomatic", "margins",
                      "rsample")
for (pack in list.of.packages) {
  if (!require(pack, character.only = TRUE)) {
    stop(paste0("Paquete no encontrado: ", pack))
  }
}

options(bitmapType="cairo")

rm(pack); gc()

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 2: Lectura del dataset ----
# ---------------------------------------------------------------------------- #

data <- readr::read_delim("./data/ebay_data.csv", delim = ';') %>%
  # Corregir formato de fecha 
  dplyr::mutate(date = lubridate::dmy(date)) 

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 3: Exploración del dataset ----
# ---------------------------------------------------------------------------- #

# Tipos de variables
tipos_datos <- dlookr::diagnose(data)

# Diagnóstico numerico
diagnostico_variables_numericas <- dlookr::diagnose_numeric(data)

# Diagnóstico categorico
diagnostico_variables_categoricas <- dlookr::diagnose_category(data %>%
                                                                 dplyr::select(category), add_date = TRUE)

  
## Análisis univariado

# Cantidad de ventas por plataforma
ventas_plataforma <- ggplot2::ggplot(data = data, ggplot2::aes(x = as.factor(desktop))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Plataforma") + ggplot2::ylab("Cantidad de operaciones") +
  ggplot2::scale_x_discrete(label = c('Móvil', 'Desktop'))
ventas_plataforma_concretadas <- ggplot2::ggplot(data = data %>%
                                                   dplyr::filter(itemsold == 1),
                                                 ggplot2::aes(x = as.factor(desktop))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Plataforma") + ggplot2::ylab("Cantidad de ventas") +
  ggplot2::scale_x_discrete(label = c('Móvil', 'Desktop'))


# Cantidad de ventas por momento
ventas_momento <- ggplot2::ggplot(data = data, ggplot2::aes(x = as.factor(post))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Momento de compra") + ggplot2::ylab("Cantidad de operaciones") +
  ggplot2::scale_x_discrete(label = c('Pre', 'Post'))
ventas_momento_concretadas <- ggplot2::ggplot(data = data %>%
                                                dplyr::filter(itemsold == 1),
                                              ggplot2::aes(x = as.factor(post))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Momento de compra") + ggplot2::ylab("Cantidad de ventas") +
  ggplot2::scale_x_discrete(label = c('Pre', 'Post'))

# Cantidad de ventas por categoria
ventas_categoria <- ggplot2::ggplot(data = data, ggplot2::aes(x = category)) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Categorías") + ggplot2::ylab("Cantidad de ventas")

# Cantidad de mensajes intercambiados
ventas_mensajes <- ggplot2::ggplot(data = data, ggplot2::aes(x = as.factor(message))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Mensajes enviados?") + ggplot2::ylab("Cantidad de ventas") +
  ggplot2::scale_x_discrete(label = c('No', 'Si'))

# Cantidad de condicion del producto
condicion_venta <- ggplot2::ggplot(data = data, ggplot2::aes(x = as.factor(condition))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Condición \ndel producto") + ggplot2::ylab("Cantidad de operaciones") +
  ggplot2::scale_x_discrete(label = c('Usado', 'Nuevo'))
condicion_venta_concretada <- ggplot2::ggplot(data = data %>%
                                                dplyr::filter(itemsold == 1), 
                                              ggplot2::aes(x = as.factor(condition))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Condición \ndel producto") + ggplot2::ylab("Cantidad de ventas") +
  ggplot2::scale_x_discrete(label = c('Usado', 'Nuevo'))

# Densidad del precio escala natural
densidad_precio <- ggplot2::ggplot(data = data, ggplot2::aes(x = askingprice)) +
  ggplot2::geom_density() +
  ggplot2::geom_rug() +
  ggplot2::xlab("Asking price") + ggplot2::ylab("Densidad") +
  ggplot2::theme_bw()
# Densidad del precio escala log10
densidad_precio_log <-ggplot2::ggplot(data = data, ggplot2::aes(x = askingprice)) +
  ggplot2::geom_density() +
  ggplot2::geom_rug(alpha = 0.2) +
  ggplot2::scale_x_log10() +
  ggplot2::xlab("Asking price") + ggplot2::ylab("Densidad") +
  ggplot2::theme_bw()

## Efecto de las condiciones meteorológicas

# Temperatura 
pctiles <- seq(0, 1, 0.20)

temperatura_venta <- data %>%
  mutate(percentile = gtools::quantcut(temp, q=seq(0, 1, by=0.2)))  %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(percentile))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Percentil de temperatura") + ggplot2::ylab("Cantidad de operaciones")
temperatura_venta_concretada <- data %>%
  mutate(percentile = gtools::quantcut(temp, q=seq(0, 1, by=0.2)))  %>%
  dplyr::filter(itemsold == 1) %>%
  ggplot2::ggplot(data = ., 
                  ggplot2::aes(x = as.factor(percentile))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Percentil de temperatura") + ggplot2::ylab("Cantidad de ventas")

# Precipitación
precipitacion_venta <- data %>%
  mutate(dia_lluvioso = if_else(precipitation > 0.5, "Lluvioso", "Seco"))  %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(dia_lluvioso))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Día lluvioso") + ggplot2::ylab("Cantidad de operaciones")
precipitacion_venta_concretada <- data %>%
  mutate(dia_lluvioso = if_else(precipitation > 0.5, "Lluvioso", "Seco"))  %>%
  dplyr::filter(itemsold == 1) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = as.factor(dia_lluvioso))) +
  ggplot2::geom_bar() +
  ggplot2::theme_bw() +
  ggplot2::xlab("Día lluvioso") + ggplot2::ylab("Cantidad de ventas")


# Diagnóstico de outliers
diagnostico_outliers <- dlookr::diagnose_outlier(data %>%
                                                   dplyr::select(askingprice, temp, precipitation))

data %>%
  dplyr::select(askingprice) %>%
  dplyr::mutate(lower.bound = median(askingprice) - 3 * mad(askingprice, constant = 1), # Da valores negativos. Tiene significado? 
                upper.bound = median(askingprice) + 3 * mad(askingprice, constant = 1),
                outlier = if_else(askingprice >= upper.bound | askingprice <= lower.bound, 1, 0)) %>%
  ggplot2::ggplot(data = ., ggplot2::aes(x = askingprice, fill = outlier)) +
  ggplot2::geom_density()

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 4: Creación de intervalos de confianza ----
# ---------------------------------------------------------------------------- #

# Funcion para el calculo de la media
meanfun <- function(data, i){
  d <- data[i]
  return(mean(d))   
}

# Definir semilla
set.seed(1234)

combinaciones <- data %>%
  dplyr::select(desktop, post) %>%
  dplyr::distinct() %>%
  dplyr::arrange(desktop) %>%
  dplyr::mutate(seed = runif(4, min = 1000, max = 9999))

bootstrap_media <- purrr::map2(
  .x = combinaciones$desktop,
  .y = combinaciones$post,
  .f = function(aplicacion, momento) {
    
    
    # Semilla para cada iteracion
    combinaciones %>%
      dplyr::filter(desktop == aplicacion, post == momento) %>%
      dplyr::pull(seed) %>%
      set.seed()
    
    # Seleccion y filtrado de la variable de interes
    ventas <- data %>%
      dplyr::filter(desktop == aplicacion, post == momento) %>%
      dplyr::pull(itemsold) 
    
    # Remuestreo de la media de ventas
    boot_media <- boot::boot(data = ventas, statistic = meanfun, R = 1000)
    
    # Creacion de variable para nombrar la lista resultante
    nombre_lista <- paste("Desktop:" , aplicacion, "| Post:", momento)
    # Crear objeto para guardar resultados
    resultados <- list(boot_media) 
    # Renombrar objeto resultado
    resultados %<>% purrr::set_names(nombre_lista)
    
  }
) %>% unlist(., recursive = FALSE) # Eliminar un nivel de la lista. 


bootstrap_media_conf <- purrr::map_dfr(
  .x = unique(names(bootstrap_media)),
  .f = function(combinacion) {
    
    boot_media <- bootstrap_media[[combinacion]]
    
    broom::tidy(boot_media, conf.int = T) %>%
      dplyr::mutate(desktop = stringr::str_sub(combinacion, start = 10, end = 10),
                    post = stringr::str_sub(combinacion, start = -1, end = -1)) %>%
      dplyr::select(desktop, post, media = statistic, sesgo = bias, error = std.error,
                    conf.inf = conf.low, conf.sup = conf.high)
    
  }
)

intervalos_confianza_plataforma_momento <- ggplot(data = bootstrap_media_conf, ggplot2::aes(x = desktop, y = media, color = post)) +
  ggplot2::geom_point() +
  ggplot2::scale_x_discrete("Plataforma", labels = c("Móvil", "Escritorio")) +
  ggplot2::scale_color_discrete("Momento", labels = c("Pre", "Post")) +
  ggplot2::geom_errorbar(aes(ymin=conf.inf, ymax=conf.sup), width = .1) +
  ggplot2::theme_bw() +
  ggplot2::xlab("Plataforma") + ggplot2::ylab("Media")
# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 5: Evaluación empírica de ventas + potencia del test ----
# ---------------------------------------------------------------------------- #

ventas <- data %>%
  dplyr::filter(desktop == 1, 
                post == 1,
                condition == 1) %>%
  dplyr::pull(itemsold)

# Test de hipotesis sobre la media
prueba_media <- prop.test(x = sum(ventas), p = 0.5, n = length(ventas),
                          alternative = "greater", correct = FALSE)

prueba_media_tabla <- broom::tidy(prueba_media) 

# Definición de funcion para la creación de secuencias logaritmicas
seq_log <- function(from = 1, to = 100000, by = 1, length.out = log10(to/from)+1) {
  tmp <- exp(seq(log(from), log(to), length.out = length.out))
  tmp[seq(1, length(tmp), by)]  
}

# Definicion de funcion para el calculo de la potencia del test
potencia_prueba_test <- function(null.pi, true.pi, n, alpha = 0.05, alternative = "not equal") { 
  # TO DO: Faltan incluir controles de ejecución
  
  # Seleccion del tipo de prueba a realizar: cola izquierda o derecha 
  # o a dos colas
  # A partir de del tipo de prueba se obtiene el z critico
  z.critico = switch(alternative, "less" = qnorm(alpha), 
                     "greater" = qnorm(1-alpha), qnorm(1-alpha/2)) 
  
  # Calculo del cuantil para el calculo de la probabilidad
  cuantil.una.cola = (z.critico*sqrt(null.pi*(1-null.pi)/n) + null.pi - true.pi) / sqrt(true.pi*(1-true.pi)/n) 
  
  # Corrección para dos colas
  if (alternative == "not equal") {
    z.critico = qnorm(alpha/2)
    # Calculo del cuantil para el calculo de la probabilidad
    cuantil.dos.colas = (z.critico*sqrt(null.pi*(1-null.pi)/n) + null.pi - true.pi) / sqrt(true.pi*(1-true.pi)/n) 
  }
  
  # Calculo de la potencia
  potencia = switch(alternative,
                    "less" = pnorm(cuantil.una.cola),
                    "greater" = 1-pnorm(cuantil.una.cola),
                    pnorm(cuantil.dos.colas) + (1-pnorm(cuantil.una.cola))
  )
  # Devolver potencia
  return(potencia)
}

#potencia_prueba_test(null.pi=0.45, true.pi=c(.5, .6, .8), n= c(10), alternative="greater")


# Evaluar la potencia del test para distintas 
potencia_test <- purrr::map_dfr(
  .x = seq_log(from = 100, to = 100000),
  .f = function(n) {
    
    # Vector de valores de pi
    pistar <- seq(from = 0.5, to=.52, by=0.001)
    
    potencia <- potencia_prueba_test(null.pi = 0.5, true.pi = pistar, n = n,
                      alpha = 0.05, alternative = 'greater')
    
    data.frame(tamano_muestral = factor(n),
               pistar = pistar,
               potencia = potencia)
    
  }
)


potencia_prueba <- ggplot2::ggplot(data = potencia_test, ggplot2::aes(x = pistar, y = potencia, color = tamano_muestral)) +
  ggplot2::geom_line() +
  ggplot2::scale_color_discrete(name = 'Tamaño muestral') +
  ggplot2::theme_bw() +
  ggplot2::xlab(bquote(pi^'*')) + ggplot2::ylab("Potencia")


# Prueba de potencia para una distribucion binomial
#pwr::pwr.p.test(n = 5000, h = 0.5, alternative = "greater")


# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 6: Modelo de regresión lineal básico ----
# ---------------------------------------------------------------------------- #

# Independiente de la condicion del producto

# Formula del modelo lineal
formula_modelo <- formula("itemsold~desktop + post + desktop * post")

# Regresion por MCO
modelo_regresion_basico <- lm(formula_modelo, data = data) 

# Promediode los residuos == 0 
mean(modelo_regresion_basico$residuals)

# Regresion por MCO haciendo bootstrap para conocer la distribución de 
# los coeficientes del modelo

# Semilla para el remuestreo
set.seed(1234) 

# Para el remuestreo se utiliza el paquete rsample. Forma parte de la suite
# de tidyverse y permite utilizar la funcionalidad de los paquetes relacionados

# Se crean N remuestreos del dataset original
# TO DO: Elegir in R lo suficientemente grande, se usa 100 solo para pruebas
bootstrapped_samples_basico <- rsample::bootstraps(data, times = 1000)


# Definición de función para ajustar el modelo lineal
lm_coefs <- function(splits, ...) {
  # se `analysis` para extraer el data frame correspondiente a 
  # cada muestra
  lm(..., data = rsample::analysis(splits)) %>%
    broom::tidy() # tidy permite extraer los coeficientes ajustados 
}

# Se itera sobre cada una de los remuestreos para el ajuste del modelo 
# lineal y la extracción de los coeficientes
bootstrapped_samples_basico$model <- purrr::map(
                          .x = bootstrapped_samples_basico$splits, 
                          .f = lm_coefs, formula_modelo)

# Extraer los coeficientes ajustados para cada muestra y convertir en un 
# data frame para poder graficar
lm_coef_basico <- bootstrapped_samples_basico %>%
  dplyr::select(-splits) %>%
  # Apilar los tibbles en la variable model
  tidyr::unnest(model) %>%
  # Seleccionar las variables de interes
  dplyr::select(id, term, estimate, std.error, statistic,  p.value) %>%
  dplyr::mutate(term = factor(term, levels = c('(Intercept)', 'desktop', 'post', 'desktop:post')))

# Intervalos percentiles
p_ints_basico <- rsample::int_pctl(bootstrapped_samples_basico, model)

histograma_coeficientes_basico <- ggplot2::ggplot(data = lm_coef_basico, ggplot2::aes(x = estimate)) +
  ggplot2::geom_density() +
  ggplot2::facet_wrap(~term, scales = 'free') +
  ggplot2::geom_vline(data = p_ints_basico, aes(xintercept = .lower), col = "red") + 
  ggplot2::geom_vline(data = p_ints_basico, aes(xintercept = .upper), col = "red") + 
  ggplot2::theme_bw() +
  ggplot2::xlab("Coeficiente") + ggplot2::ylab("Cantidad")


relacion_coeficientes <- lm_coef_basico %>%
  dplyr::select(id, term, estimate) %>%
  # Put different parameters in columns
  tidyr::spread(term, estimate) %>% 
  # Keep only numeric columns
  dplyr::select(-id) %>%
  GGally::ggscatmat(alpha = .25) +
  ggplot2::theme_bw() +
  ggplot2::xlab("Valor del estimador (eje x)") +
  ggplot2::ylab("Valor del estimador (eje y)")

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 7: Modelo de regresión lineal por condicion de producto ----
# ---------------------------------------------------------------------------- #

# Formula del modelo lineal
formula_modelo_condicion <- formula("itemsold~desktop + post + desktop * post")

# Discriminando entre producto nuevo y usado
modelo_regresion_condicion <- purrr::map(
  .x = unique(data$condition),
  .f = function(condicion) {
    
    # Filtrar datos por condicion
    data_condicion <- data %>%
      dplyr::filter(condition == condicion)
    
    modelo_regresion <- lm(formula_modelo_condicion, data = data_condicion) 
    
    #broom::tidy(modelo_regresion) %>%
    #  dplyr::mutate(signif = p.value < 0.05,
    #    condition = condicion)
    
    # Creacion de variable para nombrar la lista resultante
    nombre_lista <- paste("Condicion:" , condicion)
    # Crear objeto para guardar resultados
    resultados <- list(modelo_regresion) 
    # Renombrar objeto resultado
    resultados %<>% purrr::set_names(nombre_lista)
    
  }
) %>% unlist(., recursive = FALSE) # Eliminar un nivel de la lista. 

# Remuestreo de coeficientes discriminando entre producto nuevo y usado
bootstrap_model_condition <- purrr::map_dfr(
  .x = unique(data$condition),
  .f = function(condicion) {
    
    set.seed(condicion)
    bt_resamples <-  rsample::bootstraps(data %>%
                                 dplyr::filter(condition == condicion), times = 1000)
    
    bt_resamples$model <- purrr::map(.x = bt_resamples$splits, 
                              .f = lm_coefs, 
                              formula_modelo_condicion)
    
    lm_coef <- 
      bt_resamples %>%
      dplyr::select(-splits) %>%
      # Turn it into a tibble by stacking the `models` col
      unnest() %>%
      dplyr::mutate(condition = condicion) %>%
      # Get rid of unneeded columns
      dplyr::select(id, condition, term, estimate, std.error, statistic,  p.value) 
    
  }
)

histograma_coeficientes_condicion <- ggplot2::ggplot(data = bootstrap_model_condition, 
                                           ggplot2::aes(x = estimate, fill = as.factor(condition))) +
  ggplot2::geom_density(alpha = 0.4) +
  ggplot2::scale_fill_discrete(name = 'Condición') +
  ggplot2::facet_wrap(.~term, scales = 'free') +
  ggplot2::theme_bw() +
  ggplot2::xlab("Coeficiente") + ggplot2::ylab("Cantidad obs.") +
  ggplot2::theme(legend.position = 'bottom')


bootstrap_model_condition %>%
  dplyr::group_by(condition, term) %>%
  dplyr::summarise(estimate = mean(estimate))

# -----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 8: Modelo de regresión lineal con condiciones climaticas ----
# ---------------------------------------------------------------------------- #

# Incorporacion de variables climáticas

# Formula del modelo lineal
formula_modelo_clima <- formula("itemsold~desktop + post + desktop * post + precipitation + temp")

# Regresion por MCO
modelo_regresion_clima <- lm(formula_modelo_clima, data = data) 

# Se crean N remuestreos del dataset original
# TO DO: Elegir in R lo suficientemente grande, se usa 100 solo para pruebas
bootstrapped_samples_clima <- rsample::bootstraps(data, times = 1000)


# Se itera sobre cada una de los remuestreos para el ajuste del modelo 
# lineal y la extracción de los coeficientes
bootstrapped_samples_clima$model <- purrr::map(.x = bootstrapped_samples_clima$splits, 
                                         .f = lm_coefs, formula_modelo_clima)

# Extraer los coeficientes ajustados para cada muestra y convertir en un 
# data frame para poder graficar
lm_coef_clima <- bootstrapped_samples_clima %>%
  dplyr::select(-splits) %>%
  # Apilar los tibbles en la variable model
  tidyr::unnest(model) %>%
  # Seleccionar las variables de interes
  dplyr::select(id, term, estimate, std.error, statistic,  p.value) %>%
  dplyr::mutate(term = factor(term, levels = c('(Intercept)', 'desktop', 'post', 'desktop:post', "temp", 'precipitation')))


histograma_coeficientes_clima <- ggplot2::ggplot(data = lm_coef_clima, 
                                                     ggplot2::aes(x = estimate)) +
  ggplot2::geom_density(alpha = 0.4) +
  ggplot2::scale_fill_discrete(name = 'Condición') +
  ggplot2::facet_wrap(.~term, scales = 'free') +
  ggplot2::theme_bw() +
  ggplot2::xlab("Coeficiente") + ggplot2::ylab("Cantidad obs.") +
  ggplot2::theme(legend.position = 'bottom')



# Discriminando entre producto nuevo y usado
modelo_regresion_condicion_clima <- purrr::map(
  .x = unique(data$condition),
  .f = function(condicion) {
    
    # Filtrar datos por condicion
    data_condicion <- data %>%
      dplyr::filter(condition == condicion)
    
    modelo_regresion <- lm(formula_modelo_clima, data = data_condicion) 
    
    #broom::tidy(modelo_regresion) %>%
    #  dplyr::mutate(signif = p.value < 0.05,
    #                condition = condicion)
    
    # Creacion de variable para nombrar la lista resultante
    nombre_lista <- paste("Condicion:" , condicion)
    # Crear objeto para guardar resultados
    resultados <- list(modelo_regresion) 
    # Renombrar objeto resultado
    resultados %<>% purrr::set_names(nombre_lista)
    
  }
) %>% unlist(., recursive = FALSE) # Eliminar un nivel de la lista. 
# ----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# Paso 9: Bootstrap manual ----
# ---------------------------------------------------------------------------- #

# Definir semilla
set.seed(1234)

combinaciones <- data %>%
  dplyr::select(desktop, post) %>%
  dplyr::distinct() %>%
  dplyr::arrange(desktop) %>%
  dplyr::mutate(seed = runif(4, min = 1000, max = 9999))

B = 1000 # Cantidad de repeticiones

bootstrap_media_manual <- purrr::map2_dfr(
  .x = combinaciones$desktop,
  .y = combinaciones$post,
  .f = function(aplicacion, momento) {
    
    # Semilla para cada iteracion
    # Se filtran cada una de las combinaciones deseadas
    # de aplicación y momento
    combinaciones %>%
      dplyr::filter(desktop == aplicacion, post == momento) %>%
      dplyr::pull(seed) %>% # Se coloca una semilla para asegurar
                            # la reproducibilidad
      set.seed()
    
    # Seleccion y filtrado de la variable de interes
    ventas <- data %>%
      dplyr::filter(desktop == aplicacion, post == momento) %>%
      dplyr::pull(itemsold) 
    
    results = c() # Vector para guardar resultados
    
    # Dentro del for se iteran B veces seleccionado distintas muestras del 
    # vector de ventas con reposición para asegurar un n constante 
    # sobre cada iteración
    # Luego se calcula la media de cada vector y se guarda en un objeto
    for(b in 1:B){
      bootSample = sample(ventas, size=length(ventas), replace=TRUE) # Remuestreo de los datos
      thetaHat = mean(bootSample) # Calculo de la media de la muestra
      results[b] = thetaHat # Guardar resultados
    }
    
    # Devuelve los resultados con cada media para cada una de las combinaciones
    return(data.frame(desktop = aplicacion, post = momento, media = results))
  } 
)

# Calculo de los intervalos de confianza
bootstrap_intervalos_manual <- bootstrap_media_manual %>%
  # Se agrupan los en función de las combinaciones deseadas
  dplyr::group_by(desktop, post) %>%
  # Se oredenan las medias estimadas en el paso anterior 
  # de menor a mayor para cada uno de los grupos
  dplyr::arrange(media) %>%
  # Sobre los grupos se seleccionan los valores que están 
  # en la vecindad del percentil 2.5 y 97.5, los límites del 
  # intervalo de confianza del 95%
  dplyr::summarise(low.int = map_dbl(round(0.025*B), ~ media[.x]),
                upp.int = map_dbl(round((1 - 0.025)*B), ~ media[.x])) %>%
  # Se desagrupan los resultados
  dplyr::ungroup() %>%
  # Corrección de nombres para visualizar
  dplyr::mutate(desktop = if_else(desktop == 1, 'Desktop', 'Móvil'),
                post = if_else(post == 1, 'Post', 'Pre'))

# Evaluación de los resultados
bootstrap_media_manual %>%
  dplyr::group_by(desktop, post) %>%
  dplyr::summarise(media = mean(media))


bootstrap_intervalos_manual_plot <- ggplot(data = bootstrap_media_manual %>%
                                             dplyr::mutate(desktop = if_else(desktop == 1, 'Desktop', 'Móvil'),
                                                           post = if_else(post == 1, 'Post', 'Pre')), ggplot2::aes(x = media)) +
  ggplot2::geom_density() +
  ggplot2::facet_wrap(desktop~post, scales = 'free') +
  ggplot2::geom_vline(data = bootstrap_intervalos_manual, ggplot2::aes(xintercept = low.int), color = 'red') +
  ggplot2::geom_vline(data = bootstrap_intervalos_manual, ggplot2::aes(xintercept = upp.int), color = 'red') +
  
  ggplot2::theme_bw() +
  ggplot2::ylab("Media")
# ----------------------------------------------------------------------------

# ---------------------------------------------------------------------------- #
# ---- Paso n: Generar reporte ----
# ---------------------------------------------------------------------------- #

# Generar informe en PDF

output.dir <- getwd()
rmarkdown::render(
  input = "./trabajo_practico.Rmd",
  output_file = "trabajo_practico.pdf",
  output_dir = output.dir
)

# -----------------------------------------------------------------------------
