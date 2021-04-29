source('./common/packages.R')

option_list = list(
  make_option(c("-l", "--logfile"),type="character",default='loggings.log',
              help="Fichero de log", metavar="character"),
  make_option(c("-c", "--client"),type="character",default='iberdrola',
              help="cliente", metavar="character")
);

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

#definimos cliente
cliente <- opt$client

#Definimos fichero de log
fileLog <- opt$logfile
#fileLog <- "v5.0_iberdrola/logs/salida.log"
### Validar tokens
# Cargar token para leer datos de analytics
loginfo("Loading credentials file ...")
load('./credenciales/token_file')
ValidateToken(token)
loginfo('Credentials validated...')

logReset()
# Load loggings
addHandler(writeToFile, file=fileLog)
loginfo('Execution initialized....................................................................')

### Seleccion de cliente
#cliente <- 'iberdrola'#'zara'#'pruebas'#
loginfo('Cliente : [%s]',cliente)

# Descargar las vistas del cliente seleccionado para las cuales se van a comprobar alertas
if(cliente == 'zara'){
  sheetId <- 0
}
if(cliente == 'pruebas')
{
  sheetId <- 1254736116
}
if(cliente == 'iberdrola')
{
  sheetId <- 1805132163
}

vistas <- as.data.frame(gsheet2tbl(paste0('https://docs.google.com/spreadsheets/d/1Zs4GaNFNJVb-x2OSVaWFml75Kj4Uf2bniIa_F1Uw2rk/edit#gid=',sheetId)))
loginfo('Views loaded...')

# Sustituir caracteres que no son correctamente interpretados en BigQuery
vistas$Nombre <- str_replace_all(vistas$Nombre,' ','_')
vistas$Nombre <- str_replace_all(vistas$Nombre,'[+]','and')
vistas$Nombre <- str_replace_all(vistas$Nombre,'[-]','_')

# Reducir el numero de vistas si se quieren hacer pruebas
num_vistas_escogidas <- nrow(vistas)#15#
loginfo('Number of views : [%d]',num_vistas_escogidas)
vistas <- vistas[1:num_vistas_escogidas,]

################ Creacion de modelos para cada vista y metrica
# Bucles anidados para la identificacion de alertas. El primero recorre las vistas
loginfo('On nested loop...')

# Métricas para analizar
metrics <- list(Sesiones = list(variable = 'sessions', segmento = NULL, filtro = NULL))#,
                # Registros = list(variable = 'uniqueEvents', segmento = NULL, filtro = 'ga:eventCategory==registro;ga:eventAction=~paso(-|_| )(cinco|cuatro-(pa|em));ga:eventLabel=~confirma-registro-email-titular|confirma-registro-email-no-titular|contactanos'),
                # Logins = list(variable = 'sessions', segmento = 'gaid::Dy1SzFl0SU-bQXyopfJqYQ', filtro = NULL),
                # Contrataciones =list(variable = 'goal9Completions', segmento = NULL, filtro = NULL),
                # Leads_click_to_call = list(variable = 'uniqueEvents', segmento = NULL, filtro = 'ga:eventCategory==lead-click-to-call;ga:eventLabel==ok'),
                # Leads_call_me_back = list(variable = 'uniqueEvents', segmento = NULL, filtro = 'ga:eventCategory==lead-call-me-back;ga:eventLabel=@ok'))

# Seleccionar periodos estacionales para comparar
retrasos <- 3
fecha_periodos_antes <- Sys.Date() - seq(7, 7*retrasos, 7)

# Si el script se ejecuta por primera vez, esta variable tiene que ser TRUE
primera_vez <- TRUE

# Webhooks
Rober <- "https://hooks.slack.com/services/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"
alertas_web <- 'https://hooks.slack.com/services/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'

while(TRUE){
  if(primera_vez==TRUE){
    ahora <- 0.1
    primera_vez <- FALSE
    }else{
      ahora <- as.numeric(str_replace(substr(as.character(Sys.time()), 12, 16), ':', '.'))
    }
  alert_fin <- FALSE
  alertas_enviar <- 'Estimado cliente, se han detectado alertas en '
  load('./credenciales/token_file')
  ValidateToken(token)
  # Descarga de datos anteriores cada vez que empieza un nuevo dia
  # if(ahora>0&ahora<0.15){
  #   ya_descargado <- FALSE
  #   fecha_hoy <- Sys.Date()
  #   tabla_periodos_antes <- data.frame(date = rep(fecha_hoy, 24*60), 
  #                                      hour = rep(0:23, each = 60),
  #                                      minute = rep(0:59, times = 24))
  #   historico_alertas <- list(Sesiones = c(),
  #                             Registros = c(),
  #                             Logins = c(),
  #                             Contrataciones = c(),
  #                             Leads_click_to_call = c(),
  #                             Leads_call_me_back = c())
  # }
  if(ahora>0&ahora<0.15){
    fecha_hoy <- Sys.Date()
    tabla_periodos_antes <- data.frame(date = rep(fecha_hoy, 24*60), 
                                       hour = rep(0:23, each = 60),
                                       minute = rep(0:59, times = 24))
    historico_alertas <- list(Sesiones = c(),
                              Registros = c(),
                              Logins = c(),
                              Contrataciones = c(),
                              Leads_click_to_call = c(),
                              Leads_call_me_back = c())
    idGa <- vistas$Vista[1]
    nombre_vista <- vistas$Nombre[which(vistas$Vista==idGa)]
    for(w in names(metrics)){
      trad_desc <- w
      variable_desc <- metrics[[w]]$variable
      segmento_desc <- metrics[[w]]$segmento
      filtro_desc <- metrics[[w]]$filtro
      datos_calcular_media <- data.frame(date = rep(Sys.Date(), 24*60), 
                                         hour = rep(0:23, each = 60), 
                                         minute = rep(0:59, times = 24))
      for(fecha in as.character(fecha_periodos_antes)){
        query_list <- Init(start.date = fecha,
                           end.date = fecha,
                           dimensions = c('ga:date', 'ga:hour', 'ga:minute'),
                           metrics = paste0('ga:',variable_desc),
                           segments = if(is.null(segmento_desc)) NULL else {segmento_desc},
                           filters = if(is.null(filtro_desc)) NULL else {filtro_desc},
                           max.results = 10000, 
                           table.id = paste0('ga:',idGa))
        
        ga_query <- QueryBuilder(query_list)
        datos <- GetReportData(ga_query,token)#, split_daywise = T)
        datos <- datos[order(as.Date(datos$date, format = '%Y%m%d')),]
        
        # Tenemos que sumar estas métricas a las anteriores ya que están divididas
        if(w=='Leads_click_to_call')
        {
          loginfo('Adding data for some metrics...')
          query_list <- Init(start.date = fecha,
                             end.date = fecha,
                             dimensions = c('ga:date', 'ga:hour', 'ga:minute'),
                             metrics = paste0('ga:',variable_desc),
                             segments = if(is.null(segmento_desc)) NULL else {segmento_desc},
                             filters = 'ga:eventCategory==conversion;ga:eventAction=~solicitudes|^llamanos$;ga:eventLabel=~ok|llamanos.*',
                             max.results = 10000, 
                             table.id = paste0('ga:',idGa))
          
          ga_query <- QueryBuilder(query_list)
          datos2 <- GetReportData(ga_query,token)#, split_daywise = T)
          # Si no son nulos, sumarlos con los anteriores
          if(!is.null(datos2)){
            # Puede que falten algunos, completar para poder sumar con datos
            datos2 <- merge(datos, datos2, by = c('date','hour','minute'), all.x = TRUE)
            datos2[is.na(datos2)] <- 0
            datos[,variable_desc] <- datos[,variable_desc] + datos2[,ncol(datos2)]
            datos <- datos[order(as.Date(datos$date, format = '%Y%m%d')),]}
        }
        
        if(w=='Leads_call_me_back')
        {
          loginfo('Adding data for some metrics...')
          query_list <- Init(start.date = fecha,
                             end.date = fecha,
                             dimensions = c('ga:date', 'ga:hour', 'ga:minute'),
                             metrics = paste0('ga:',variable_desc),
                             segments = if(is.null(segmento_desc)) NULL else {segmento_desc},
                             filters = 'ga:eventCategory==conversion;ga:eventAction==conversion-call-me-back',
                             max.results = 10000, 
                             table.id = paste0('ga:',idGa))
          
          ga_query <- QueryBuilder(query_list)
          datos2 <- GetReportData(ga_query,token)#, split_daywise = T)
          if(!is.null(datos2)){
            # Puede que falten algunos, completar para poder sumar con datos
            datos2 <- merge(datos, datos2, by = c('date','hour','minute'), all.x = TRUE)
            datos2[is.na(datos2)] <- 0
            datos[,variable_desc] <- datos[,variable_desc] + datos2[,ncol(datos2)]
            datos <- datos[order(as.Date(datos$date, format = '%Y%m%d')),]}
        }
        # Cambiar nombre de columna para que no se confunda con el resto de periodos
        colnames(datos)[which(colnames(datos)==variable_desc)] <- paste0(variable_desc, '_', fecha)
        datos$date <- rep(datos_calcular_media$date[1], nrow(datos))
        datos$hour <- as.numeric(datos$hour)
        datos$minute <- as.numeric(datos$minute)
        # Unimos los valores de este periodo con el resto
        datos_calcular_media <- merge(datos_calcular_media, datos, by = c('date', 'hour', 'minute'), all.x = TRUE)
        datos_calcular_media[paste0(variable_desc, '_', fecha)] <- na.fill(datos_calcular_media[paste0(variable_desc, '_', fecha)], 0)
      }
      # Eliminamos columnas de fechas duplicadas
      #datos_calcular_media <- datos_calcular_media[, !duplicated(colnames(datos_calcular_media))]
      # Calculamos la media y la desviación típica de la metrica
      datos_calcular_media <- cbind(datos_calcular_media[,1:3],
                                    rowMeans(datos_calcular_media[,4:ncol(datos_calcular_media)]),
                                    apply(datos_calcular_media[,4:ncol(datos_calcular_media)], 1, sd))
      colnames(datos_calcular_media) <- c('date','hour', 'minute', paste0('avg_', w), paste0('sd_', w))
      tabla_periodos_antes <- merge(tabla_periodos_antes, datos_calcular_media, by = c('date', 'hour', 'minute'))
    }
    tabla_periodos_antes <- tabla_periodos_antes[order(tabla_periodos_antes$date, tabla_periodos_antes$hour, tabla_periodos_antes$minute),]
    #ya_descargado <- TRUE
  }
  for(k in 1:nrow(vistas))
  {
    loginfo('Loop %d of %d :',k,nrow(vistas))
    # El segundo bucle recorre las metricas
    loginfo('Getting into for loop to calculate alerts...')
    # Guardar el ID de vista, el nombre y la fecha de inicio del analisis
    idGa <- vistas$Vista[k]
    nombre_vista <- vistas$Nombre[which(vistas$Vista==idGa)]
    for(i in names(metrics))
    {
      trad <- i
      variable <- metrics[[i]]$variable
      segmento <- metrics[[i]]$segmento
      filtro <- metrics[[i]]$filtro
      
      loginfo(paste0('Getting and analyzing ',variable,'...'))
      print(trad)
      
      # Se descargan los datos de hoy para la detección de alertas
      query_list <- Init(start.date = as.character(fecha_hoy),
                         end.date = as.character(fecha_hoy),
                         dimensions = c('ga:date', 'ga:hour', 'ga:minute'),
                         metrics = paste0('ga:',variable),
                         segments = if(is.null(segmento)) NULL else {segmento},
                         filters = if(is.null(filtro)) NULL else {filtro},
                         max.results = 10000,
                         table.id = paste0('ga:',idGa))
      
      ga_query <- QueryBuilder(query_list)
      datos <- GetReportData(ga_query,token)#, split_daywise = T)
      datos <- datos[order(as.Date(datos$date, format = '%Y%m%d')),]
      colnames(datos)[ncol(datos)] <- trad
      datos$date <- as.Date(datos$date, format = '%Y%m%d')
      datos$hour <- as.numeric(datos$hour)
      datos$minute <- as.numeric(datos$minute)
      # Guardar último minuto descargado
      last_hour <- datos$hour[nrow(datos)]
      last_minute <- datos$minute[nrow(datos)]
      # Unir tabla con la variable seleccionada para medir alertas
      times_sigma <- 15
      aux <- cbind(tabla_periodos_antes[,c('date', 'hour', 'minute')], tabla_periodos_antes[,grepl(trad, colnames(tabla_periodos_antes))])
      tabla_alertas <- merge(aux, datos, by = c('date', 'hour', 'minute'), all.x = TRUE)
      tabla_alertas[is.na(tabla_alertas)] <- 0
      tabla_alertas <- tabla_alertas[1:(which(tabla_alertas$hour==last_hour&tabla_alertas$minute==last_minute)),]
      # Si la desviación típica es cero, cambiarla a otro valor arbitrario
      tabla_alertas[,paste0('sd_',trad)] <- ifelse(tabla_alertas[,paste0('sd_',trad)]==0, 2, tabla_alertas[,paste0('sd_',trad)])
      tabla_alertas$alerta <- ifelse(tabla_alertas[,trad]<(tabla_alertas[,paste0('avg_',trad)]-times_sigma*tabla_alertas[,paste0('sd_',trad)]),
                                     'ALERTA: INFERIOR A LO ESPERADO',
                                     ifelse(tabla_alertas[,trad]>(tabla_alertas[,paste0('avg_',trad)]+times_sigma*tabla_alertas[,paste0('sd_',trad)]),
                                            'ALERTA: SUPERIOR A LO ESPERADO',
                                     'NO HAY ALERTAS'))
      tabla_alertas$porc <- (tabla_alertas[,trad]/tabla_alertas[,paste0('avg_',trad)] - 1)*100
      tabla_alertas$porc[is.infinite(tabla_alertas$porc)] <- 100
      tabla_alertas$porc[is.na(tabla_alertas$porc)] <- 0
      solo_alertas <- tabla_alertas[tabla_alertas$alerta!='NO HAY ALERTAS',]
      # Solo queremos comunicar una alerta si no se ha comunicado previamente, guardamos un codigo de la alerta
      # basado en la fecha hora y minuto
      if(nrow(solo_alertas)>0){
        solo_alertas$time <- paste0(as.character(solo_alertas$date), ' ', solo_alertas$hour, ':', ifelse(nchar(solo_alertas$minute)==1, paste0('0', solo_alertas$minute), solo_alertas$minute))
        solo_alertas <- solo_alertas[!(solo_alertas$time %in% historico_alertas[[trad]]),]
        if(nrow(solo_alertas)>0){
          historico_alertas[[trad]] <- c(historico_alertas[[trad]], solo_alertas$time)
          # Guardar el mensaje de enviar alertas
          alertas_enviar <- paste0(alertas_enviar, trad, ' en el/los minutos ')
          alert_fin <- TRUE
          for(m in 1:nrow(solo_alertas))
          {
            # Dependiendo de si ha llegado al final de los minutos, poner coma o punto
            if(m<nrow(solo_alertas))
            {
              alertas_enviar <- paste0(alertas_enviar, solo_alertas$time[m], ', ')
            }else{
              alertas_enviar <- paste0(alertas_enviar, solo_alertas$time[m], '. ')
            }
          }
        }
      }
    }# fin de for i
  } # fin de for k
  # Enviar mensaje por slack
  if(alert_fin == TRUE){
  POST(alertas_web, encode = "json", body = list(text = alertas_enviar))
  }
  print(alertas_enviar)
  Sys.sleep(60*10)
}# fin de while