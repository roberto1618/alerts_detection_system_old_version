# Carga de librerias
source('./common/packages.R')
source('./common/send_mail.R')
source('./common/connection.R')

# Set Time Zone in order to avoid errors
Sys.setenv(TZ="UTC")

option_list = list(
	make_option(c("-l", "--logfile"),type="character",default='loggings.log',
				help="Fichero de log", metavar="character"),
	make_option(c("-c", "--config"),type="character",default='pullbear_dev.json',
				help="Fichero de configuracion", metavar="character"),
	make_option(c("-d", "--hoy"), type = "integer", default = strftime(Sys.Date(),'%w'),
	      help = "dia de la semana para ejecucion", metavar = "character")
	);
	
# Recogegemos los parametros de ejecucion
opt_parser=OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

# Fijamos fichero de log
fileLog <- opt$logfile

# Cargamos fichero de configuracion
configFile <- fromJSON(file = paste0(getwd(),'/config/',tolower(opt$config)))

cliente <- configFile$cliente
show_umbrales <- if (tolower(configFile$showUmbral) != 'true'){FALSE} else TRUE

proyecto_google <- configFile$googleProject$project
conjunto_datos <- configFile$googleProject$dataset
tokenFile <- configFile$googleProject$tokenFile
credFile <- configFile$googleProject$credFile

flag_envia_email <- if (tolower(configFile$sendEmail) != 'true'){FALSE} else TRUE 

# Configuración inicial
tabla_datos <- list('alertas', 'umbrales', 'historico')
#####web_sheet <- credential_names$GS_credential_name[[cliente]]$web

# Cargamos configuración de logging
logReset()
addHandler(writeToFile, file = fileLog)
loginfo('Execution initialized....................................................................')

# Dia semana actual
hoy <- opt$hoy

### Validar tokens
# Cargar token para leer datos de analytics
######################################################################
token <- connect2BQ(credFile=credFile,
                    tokenFile=tokenFile)
######################################################################
# loginfo("Loading credentials file ...")
# load(tokenFile)
# ValidateToken(token)
# loginfo('Credentials validated...')

# # Credenciales bigquery
# BQ_credential_name <- credFile
# bq_auth(path = BQ_credential_name)
# bqr_auth(json_file = BQ_credential_name)
# loginfo('Connected to BQ...')

### Seleccion de cliente
loginfo('Cliente : [%s]',cliente)

# Descargar las vistas y metricas del cliente seleccionado para las cuales se van a comprobar alertas
all_info <- fromJSON(file = paste0("clientes/", cliente, '/metrics.json'))
metrics <- all_info$metrics
vistas <- all_info$ids
loginfo('Views loaded...')

# Sustituir caracteres que no son correctamente interpretados en BigQuery
names(vistas) <- str_replace_all(names(vistas),' ','_')
names(vistas) <- str_replace_all(names(vistas),'[+]','and')
names(vistas) <- str_replace_all(names(vistas),'[-]','_')

# Recogemos el numero de vistas a tratar
vistas <-vistas[1:length(vistas)]
loginfo('Number of views : [%d]',length(vistas))

################ Creacion de modelos para cada vista y metrica
### Parametros iniciales
if(hoy==1)
{
  loginfo('Today == 1 (Monday)...')
  # Se define el nivel de confianza de los intervalos
  confianza <- if (is.null(configFile$modelo$nivelConfianza)) {95} else {configFile$modelo$nivelConfianza}
  n_historical <- if (is.null(configFile$modelo$n_historical)) {60} else {configFile$modelo$n_historical}
  
  # Definir horizonte temporal
  h <- if (is.null(configFile$modelo$numPeriodos==NULL)) {10} else {configFile$modelo$numPeriodos}

  loginfo('Model parameters: confianza=%d, h=%d', confianza,h)
  
  # Crear las tablas que recogeran las alertas y los umbrales a partir del cual clasificar dichas alertas
  tablaFin <- data.frame(quitar = 1:h)
  tablaDef <- tablaFin
  umbrales <- tablaFin
}else{
  loginfo('Today != 1 (other day)...')
  # Extraer datos de las predicciones de BigQuery
  #tablaDefAux <- paste("SELECT *",
  #                     "FROM",
  #                     paste0("`eam-gmail-alertas.alertas_",cliente,".alertas`"))
  #
  #loginfo('Querying ... [%s]', tablaDefAux)
  #tablaDef <- query_exec(tablaDefAux, project = "eam-gmail-alertas", use_legacy_sql = FALSE)
  tablaDef <- as.data.frame(bq_table_download(paste0(proyecto_google, '.', conjunto_datos, '.', tabla_datos[[1]])))#(paste0('eam-gmail-alertas.alertas_',cliente,'.alertas')))
  tablaDef <- tablaDef[order(as.Date(tablaDef$Fecha)),]
  
  #umbralesAux <- paste("SELECT *",
  #                     "FROM",
  #                     paste0("`eam-gmail-alertas.alertas_",cliente,".umbrales`"))
  #
  #loginfo('Querying ... [%s]', umbralesAux)
  #umbrales <- query_exec(umbralesAux, project = "eam-gmail-alertas", use_legacy_sql = FALSE)
  umbrales <- as.data.frame(bq_table_download(paste0(proyecto_google, '.', conjunto_datos, '.', tabla_datos[[2]])))
  umbrales <- umbrales[order(as.Date(umbrales[,1])),]
  loginfo('Downloaded tables from BQ...')
}

# Recoger el resumen por metrica, y el resumen general de todas las metricas que se enviara por correo
alerta_metricas <- ''

# Variable auxiliar para especificar si alguna alerta ha sido encontrada
alert <- FALSE
alert_fin <- FALSE

# Bucles anidados para la identificacion de alertas. El primero recorre las vistas
start_time <- Sys.time()
loginfo('On nested loop...')
for(k in names(metrics))
{
	loginfo('View %s:',k)
  # Es necesario reiniciar una de las tablas en cada vueltas para recoger alertas de nuevas vistas. Lo mismo con resumen
  # y la variable 'alert'
  # Guardar el ID de vista, el nombre y la fecha de inicio del analisis
  idGa <- vistas[[k]]#vistas$Vista[k]
  nombre_vista <- k#vistas$Nombre[which(vistas$Vista==idGa)]
  metrics_escogida <- metrics[[k]]
  if(hoy==1)
  {
    fecha_inicio <- Sys.Date() - n_historical
    loginfo('Today is Monday, so creating table to introduce alerts...')
    tablaFin <- data.frame(quitar = 1:h)
  }else{
    # Para que te cambie los puntos por guion bajo
    nombre_vista <- str_replace_all(nombre_vista,'[.]','_')
    fecha_inicio <- Sys.Date()-1
  }
  resumen <- 'Existen alertas en: '
  alert <- FALSE
  print(k)
  # Contador para no guardar las fechas en multiples columnas
  count <- 0
  # El segundo bucle recorre las metricas
  loginfo('Getting into for loop to calculate alerts...')
  for(i in names(metrics_escogida))
  {
    # Es necesario controlar el tiempo que se lleva ejecutando el codigo. Si es demasiado, se renueva el token
    end_time <- Sys.time()
    diff_time <- difftime(end_time, start_time, units = 'mins')
    if(diff_time>10)
    {
	    #########################################################################
      token <- connect2BQ(credFile=credFile,
                          tokenFile=tokenFile,
                          refreshToken=TRUE)
      #########################################################################
      # token<-token$refresh()
      # ValidateToken(token)
      # loginfo('Credentials validated...')
      # # Credenciales bigquery
      # BQ_credential_name <- credFile
      # bq_auth(path = BQ_credential_name)
      # bqr_auth(json_file = BQ_credential_name)
      # loginfo('Connected to BQ...')
      start_time = Sys.time()
    }
    
    # Save the metric information
    trad <- i
    variable <- metrics_escogida[[i]]$variable
    segmento <- metrics_escogida[[i]]$segmento
    filtro <- metrics_escogida[[i]]$filtro

    # If the variable can have decimal values, the round is with 3 decimal numbers
    list_decimal_variables <- c('transactionRevenue', 'transactionsPerSession')
    round_decimals <- 0
    if(variable %in% list_decimal_variables){
      round_decimals <- 2
    }
    
    # Nombres de columnas
    if(length(vistas)==1){
      suffix_vista <- ''
      resumen_name_col <- paste0('Resumen')
    } else {
      suffix_vista <- paste0('_',nombre_vista)
      resumen_name_col <- paste0('Resumen_',nombre_vista)
    }
      
    liminf_name_col <- paste0('Limite_inferior_',trad, suffix_vista)
    limsup_name_col <- paste0('Limite_superior_',trad, suffix_vista)
    pred_name_col <- paste0('Prediccion_',trad, suffix_vista)
    datoreal_name_col <- paste0('Dato_Real_',trad, suffix_vista)
    alertas_name_col <- paste0('Alertas_',trad, suffix_vista)
    
    loginfo(paste0('Getting and analyzing ',variable,'...'))
    print(trad)
    
    loginfo('Downloading data from Analytics...')
    
    split_daywise <- FALSE
    # Descarga de datos
    datos <- data.frame(date = seq(fecha_inicio, Sys.Date(), by = 'day'), y = 0)
    # Si no tiene filtro, vale cero y se mete en el bucle, ahi dentro se cambia a null. Si si lo tiene se mete y recorre los filtros
    filtro <- if(is.null(filtro)) 0 else{filtro}
    # Recorremos todos los filtros para sumar los resultados de todos los filtros. Esto no separará el análisis por filtros, sino que suma las columnas de todos ellos
    for(f in filtro){
      f <- f[[1]]
      f <- if(f == 0) NULL else{f}
      query_list <- Init(start.date = as.character(fecha_inicio),
                         end.date = as.character(Sys.Date()),
                         dimensions = c('ga:date'),
                         metrics = paste0('ga:',variable),
                         segments = if(is.null(segmento)) NULL else {segmento},#ifelse(is.null(segmento),NULL,paste0('ga:',segmento)),
                         filters = if(is.null(f)) NULL else {f},
                         max.results = 10000, 
                         table.id = paste0('ga:',idGa))
      
      ga_query <- QueryBuilder(query_list)
      datos_aux <- try(GetReportData(ga_query,token, split_daywise = split_daywise))
      # If it returns an error, try it until the error run away
      while(is.error(datos_aux)){
        datos_aux <- try(GetReportData(ga_query,token, split_daywise = split_daywise))
        # Es necesario controlar el tiempo que se lleva ejecutando el codigo. Si es demasiado, se renueva el token
        end_time <- Sys.time()
        diff_time <- difftime(end_time, start_time, units = 'mins')
        if(diff_time>10)
        {
          token <- connect2BQ(credFile=credFile,
                              tokenFile=tokenFile,
                              refreshToken=TRUE)
          start_time = Sys.time()
        }
      }
      datos_aux <- datos_aux[order(as.Date(datos_aux$date, format = '%Y%m%d')),]
      datos[,2] <- datos[,2] + datos_aux[,2]
    }

    # Renonmbrar columnas y recodificar fecha para poder tratar apropiadamente los datos
    colnames(datos) <- c('ds','y')
    datos$ds <- as.Date(datos$ds, format = '%Y-%m-%d')
    
    if(hoy==1)
    {
      loginfo('Estimating model...')
      datepred <- seq(as.Date(datos$ds[nrow(datos)-2])+1, as.Date(datos$ds[nrow(datos)-2])+h, by='days')
      datos_prophet <- datos[1:(nrow(datos)-2),]
      fit <- prophet(datos_prophet,
                      interval.width = confianza/100,
                      yearly.seasonality = FALSE,
                      seasonality.mode = 'multiplicative')
      future <- make_future_dataframe(fit, periods = h)
      forecast <- predict(fit, future)
      plot(fit, forecast)
      forecast$ds <- as.Date(forecast$ds)
      forecast <- forecast[forecast$ds >= datepred[1],]
      pred <- forecast[,c('yhat', 'yhat_lower', 'yhat_upper')]
      
      predFin <- pred
      dataTrainAux <- data.frame(yhat=datos_prophet$y, yhat_lower=NA, yhat_upper=NA, tipo='Historico')
      
      # Renombrar las filas con las fechas en el formato correcto
      rownames(predFin) <- datepred
      predFin$tipo <- 'Prediccion'
      
      # Guardar los umbrales (intervalos de confianza) en una tabla
      umbralesAux <- data.frame(Fecha = datepred,liminf = predFin$yhat_lower, limsup = predFin$yhat_upper)
      colnames(umbralesAux) <- c('Fecha',liminf_name_col,limsup_name_col)
      umbrales <- cbind(umbrales,umbralesAux)
      
      # Guardar datos con el historico y con la prediccion
      rownames(dataTrainAux)=datos$ds[1:(nrow(datos)-2)]
      dataTrainAux$yhat=as.numeric(dataTrainAux$yhat)
      dataFin=rbind(dataTrainAux,predFin)
      
      dataFin$ds=as.Date(rownames(dataFin))
      rownames(dataFin)=1:nrow(dataFin)
      
      loginfo('Saving real data...')
      # Guardar dato real del dia de ayer
      test <- datos[(nrow(datos)-1),2]
      test <- data.frame(ds=as.Date(datos$ds[nrow(datos)-1]), point = test)
      
      # Guardar el dato real en la tabla de predicciones
      pred <- data.frame(ds = datepred, Prediccion = round(pred$yhat, round_decimals))
      #if(show_umbrales) {
      pred <- cbind(pred, round(umbralesAux[,c(liminf_name_col, limsup_name_col)],round_decimals))
      #}
      pred$`Dato Real` <- ''
      pred$`Dato Real`[1] <- round(test$point, round_decimals)
      pred$Alertas <- ''
      
      es_inf <- FALSE
      es_sup <- FALSE
      
      # Establecer el mensaje de si existe alerta o no para dicha metrica
      loginfo('Cheking the type of alert...')
      if(test$point<dataFin[which(dataFin$ds==Sys.Date()-1),2]|(test$point==0&dataFin[which(dataFin$ds==Sys.Date()-1),1]>0))
      {
        pred$Alertas[1] <- 'ALERTA: INFERIOR A LO ESPERADO'
        porc <- (1 - test$point/dataFin[which(dataFin$ds==Sys.Date()-1),1])*100
        es_inf <- TRUE
      }else{
        if(test$point>dataFin[which(dataFin$ds==Sys.Date()-1),3])
        {
          pred$Alertas[1] <- 'ALERTA: SUPERIOR A LO ESPERADO'
          porc <- abs((test$point/dataFin[which(dataFin$ds==Sys.Date()-1),1] - 1)*100)
          if(porc<0){
            porc <- 100
          }
          es_sup <- TRUE
        }else{
          pred$Alertas[1] <- 'NO HAY ALERTAS'
        }
      }
	    loginfo('Type of alert: %s', pred$Alertas[1])
      # Renonmbrar columnas para especificar la vista y metrica a la que nos referimos
	    #if(show_umbrales) {
      colnames(pred) <- c('Fecha',pred_name_col,liminf_name_col,limsup_name_col,datoreal_name_col,alertas_name_col)#,'_',nombre_vista
	    #}else{
	    #  colnames(pred) <- c('Fecha',pred_name_col,datoreal_name_col,alertas_name_col)#,'_',nombre_vista
	    #}
      
      # Convertir la columna con el dato real
      pred[,datoreal_name_col] <- as.numeric(pred[,datoreal_name_col])
      
      # Si detecta alguna alerta, se guarda el nombre de la variable o variables en las que se ha detectado para cada vista,
      # y se actualiza tambien para todas las vistas
      if(pred[1,alertas_name_col]!='NO HAY ALERTAS')
      {
        resumen <- paste0(resumen,trad,'; ')
        alert <- TRUE
        alert_fin <- TRUE
        alerta_metricas <- paste0('<br/><br/><B><span style="margin-left:2em">',trad,'</B> --> Esperado: ', round(dataFin[which(dataFin$ds==Sys.Date()-1),1],round_decimals),'. Observado: ',round(test$point,round_decimals),' (',ifelse(es_inf==TRUE,paste0(round(porc, digits = 2),'% inferior a lo esperado'),paste0(round(porc, digits = 2),'% superior a lo esperado')),').</span>',alerta_metricas)
      }
      count <- count + 1
      # A partir de la segunda variable, que no se guarden las fechas
      if(count>1)
      {
        pred <- pred[,-1]
      }
      # Se van uniendo los resultados de las distintas metricas
      tablaFin <- cbind(tablaFin,pred)
    }else{
      loginfo('Checking the type of alert...')
      # Guardar el dato de ayer y sustituirlo en la casilla correspondiente de la tabla de predicciones
      test <- datos[1,2]
      tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), datoreal_name_col] <- round(test, round_decimals)#,'_',nombre_vista
      
      # Guardar los umbrales para corroborar si existe alerta
      liminf <- umbrales[which(umbrales[,1]==as.character(Sys.Date()-1)),liminf_name_col]
      limsup <- umbrales[which(umbrales[,1]==as.character(Sys.Date()-1)),limsup_name_col]
      es_inf <- FALSE
      es_sup <- FALSE
      if(test<liminf|(test==0&tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), pred_name_col]>0))#,'_',nombre_vista
      {
        tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), alertas_name_col] <- 'ALERTA: INFERIOR A LO ESPERADO'#,'_',nombre_vista
        porc <- (1 - test/tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), pred_name_col])*100#,'_',nombre_vista
        es_inf <- TRUE
      }else{
        if(test>limsup)
        {
          tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), alertas_name_col] <- 'ALERTA: SUPERIOR A LO ESPERADO'#,'_',nombre_vista
          porc <- abs((test/tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), pred_name_col]-1)*100)#,'_',nombre_vista
          if(porc<0){
            porc <- 100
          }
          es_sup <- TRUE
        }else{
          tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), alertas_name_col] <- 'NO HAY ALERTAS'#,'_',nombre_vista
        }
      }
      
      # Si se ha detectado alguna alerta, guardar en el resumen por vista y resumen global para el correo
      if(tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), alertas_name_col]!='NO HAY ALERTAS')#,'_',nombre_vista
      {
        resumen <- paste0(resumen,trad,'; ')
        alert <- TRUE
        alert_fin <- TRUE
        alerta_metricas <- paste0('<br/><br/><B><span style="margin-left:2em">',trad,'</B> --> Esperado: ', round(tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), pred_name_col],round_decimals),'. Observado: ',round(test,round_decimals),' (',ifelse(es_inf==TRUE,paste0(round(porc, digits = 2),'% inferior a lo esperado'),paste0(round(porc, digits = 2),'% superior a lo esperado')),').</span>',alerta_metricas)#,'_',nombre_vista
      }
    }
    Sys.sleep(3)
    } # fin de for i
    
    # Si no hay alertas, aparece que no hay alertas. Si las hubiera, se guardan en el resumen general de alertas
    if(alert==FALSE)
    {
      resumen <- 'NO HAY ALERTAS'
    }else{
      alerta_metricas <- paste0('<br /><br /><font face="aileron" size="2.7" color="black">Para la vista <B><i>',nombre_vista,'</i></B>:</font>',alerta_metricas)
    }
    
  loginfo('Saving the final table...')
  if(hoy==1)
  {
    # Se guarda el resumen de alertas en una tabla definitiva
    tablaFin$Resumen <- ''
    tablaFin$Resumen[1] <- resumen
    colnames(tablaFin)[ncol(tablaFin)] <- resumen_name_col#_,nombre_vista
    tablaDef <- cbind(tablaDef,tablaFin)
  }else{
  # Guardar el resumen en la tabla
  tablaDef[which(tablaDef[,1] == as.character(Sys.Date()-1)), resumen_name_col] <- resumen#,'_',nombre_vista
  }
} # fin de for k

# Negativos a cero, NA a vacío y ceros de alertas a vacío
tablaDef[tablaDef<0] <- 0
tablaDef[is.na(tablaDef)] <- 0#''
rm_zero_alertas <- grepl('Alertas',colnames(tablaDef))
tablaDef[tablaDef[,alertas_name_col]==0, rm_zero_alertas] <- ''#,'_',nombre_vista
rm_zero_resumen <- grepl('Resumen',colnames(tablaDef))
tablaDef[tablaDef[,resumen_name_col]==0, rm_zero_resumen] <- '' # Si se añade el nombre vista hay que añadirlo tambien a esta columna

# Eliminar primeras columnas
#tiene_quitar <- grepl('quitar',colnames(tablaDef))
loginfo('Saving data to BQ...')
if(hoy==1)
{
  # Cambiar los puntos por guiones bajos
  colnames(tablaDef) <- str_replace_all(colnames(tablaDef),'[.]','_')
  colnames(umbrales) <- str_replace_all(colnames(umbrales),'[.]','_')
  
  # Guardar datos en csv
  #write.csv2(umbrales[,-1],'umbrales.csv',row.names = FALSE)
  #tmpFileUmbrales <- paste0("clientes/",cliente,'/','umbrales.csv')
  umbrales <- umbrales[,-which(duplicated(colnames(umbrales)))]
  umbrales <- umbrales[,-which(grepl('quitar', colnames(umbrales)))]
  #write.csv2(umbrales,tmpFileUmbrales,row.names = FALSE)
  
  # Borrar tablas anteriores BigQuery
  bqr_delete_table(projectId = proyecto_google, datasetId = conjunto_datos, tabla_datos[[2]])
  
  # Subir los datos a bigquery
  #bq_perform_upload('umbrales', tabla_guardar)
  #insert_upload_job('eam-gmail-alertas', paste0('alertas_',cliente), 'umbrales', umbrales[,-1])
  bqr_upload_data(projectId = proyecto_google,datasetId = conjunto_datos, tabla_datos[[2]], umbrales)
  
  # Eliminar fechas duplicadas
  tabla_guardar <- tablaDef[,-which(duplicated(colnames(tablaDef)))]
  tabla_guardar <- tabla_guardar[,-which(grepl('quitar',colnames(tabla_guardar)))]
}else{
  tabla_guardar <- tablaDef
  }

# Guardar datos en Excel
#wb <- loadWorkbook(paste0(toupper(cliente), "_alertas.xlsx"))

fileAttachment <- paste0("clientes/", cliente,"/",toupper(cliente), "_alertas.xlsx")
if (!(file.exists(fileAttachment)))
{
	templateAttachment <- paste0("clientes/", cliente,"/templates/", toupper(cliente), "_alertas.xlsx")
	logerror("File not found !! Please copy template ...[%s] in the [%s] folder,  and re-run script ... ", templateAttachment, cliente)
	quit(status=1)
}
wb <- loadWorkbook(fileAttachment)


# Style for the header
hs1 <- createStyle(
  fgFill = "#9FD1F9", halign = "CENTER", textDecoration = c("italic","bold"),
  border = "TopBottomLeftRight")

# Overwrite data
# We need to create first an auxiliar sheet because if we remove the unique sheet, the excel will be corrupted
#addWorksheet(wb, 'aux')
removeWorksheet(wb, 'Alertas')
addWorksheet(wb, 'Alertas')
# Hide first sheet
sheetVisibility(wb)[1] <- FALSE
#removeWorksheet(wb, 'aux')
#deleteData(wb, sheet = 'Alertas', cols = 1:200, rows = 1:30000, gridExpand = TRUE)
#replaceStyle(wb, 3, newStyle = createStyle(fgFill = "#000000"))

# Si los umbrales son False a mitad de semana, que los borre. La tabla subida a BQ siempre va a tener umbrales
if(!show_umbrales) {
  tabla_enviar_gmail <- tabla_guardar[,-which(grepl('Limite', colnames(tabla_guardar)))]
}else{
  tabla_enviar_gmail <- tabla_guardar
}
writeData(wb, sheet = 'Alertas', tabla_enviar_gmail, borders = 'columns', headerStyle = hs1)
setColWidths(wb, sheet = 'Alertas', cols = 1:ncol(tabla_enviar_gmail), widths = "auto")
#saveWorkbook(wb, paste0(toupper(cliente), "_alertas.xlsx"), overwrite = T)
saveWorkbook(wb, fileAttachment, overwrite = T)

# Guardar los datos en un csv
#write.csv2(tabla_guardar, 'alertas.csv', row.names = FALSE)
#write.csv2(tabla_guardar, paste0(cliente,"/",'alertas.csv'), row.names = FALSE)

# Borrar tablas anteriores BigQuery
bqr_delete_table(projectId = proyecto_google,datasetId = conjunto_datos, tabla_datos[[1]])

# Subir los datos a bigquery
#bq_perform_upload('alertas', tabla_guardar)
#insert_upload_job('eam-gmail-alertas', paste0('alertas_',cliente), 'alertas', tabla_guardar)
bqr_upload_data(projectId = proyecto_google,datasetId = conjunto_datos, tabla_datos[[1]], tabla_guardar)

# Add data to historical database only if we are on Sunday
if(hoy == 7){
  loginfo('Add data to historical database...')
  bqr_upload_data(projectId = proyecto_google, datasetId = conjunto_datos, tabla_datos[[3]], tabla_guardar, writeDisposition = 'WRITE_APPEND')
  #bqr_delete_table(projectId = proyecto_google,datasetId = conjunto_datos, tabla_datos[[3]])
}

if(flag_envia_email){ 
  loginfo('Sending email...')
  # Enviar gmail
  credentials_info <- configFile$mail
  loginfo('Enviando email a ... [%s]', credentials_info$emails_to)
  send_mail(credentials=credentials_info, existen_alertas=alert_fin)
}
