# Fechas clave en los que hay que cambiar la predicción a lo que ocurrió el año pasado
      # Black Friday
      if(!is.null(ofertas))
      {
        for(a in names(ofertas))
        {
          if(Sys.Date()-1>=ofertas[[a]]$fecha_inicio_ahora&Sys.Date()-1<=ofertas[[a]]$fecha_fin_ahora)
          {
            # Calculamos dias de diferencia con la fecha inicio para seleccionar el mismo día correspondiente al año pasado
            select_aux <- as.numeric(Sys.Date()-1 - as.Date(ofertas[[a]]$fecha_inicio_ahora))
            fecha_oferta_seleccionada_pasado <- as.Date(ofertas[[a]]$fecha_inicio_pasado) + select_aux
            # Descargamos datos del año pasado en esa franja
            query_list <- Init(start.date = as.character(as.Date(ofertas[[a]]$fecha_inicio_pasado) - 60),
                               end.date = as.character(as.Date(ofertas[[a]]$fecha_fin_pasado)),
                               dimensions = c('ga:date'),
                               metrics = paste0('ga:',variable),
                               segments = if(is.null(segmento)) NULL else {segmento},#ifelse(is.null(segmento),NULL,paste0('ga:',segmento)),
                               filters = if(is.null(filtro)) NULL else {filtro},
                               max.results = 10000, 
                               table.id = paste0('ga:',idGa))
            ga_query <- QueryBuilder(query_list)
            datos_oferta <- GetReportData(ga_query,token, split_daywise = T)
            datos_oferta <- datos_oferta[order(as.Date(datos_oferta$date, format = '%Y%m%d')),]
            colnames(datos_oferta) <- c('ds','y')
            datos_oferta$ds <- as.Date(datos_oferta$ds, format = '%Y%m%d')
            
            # Calcular el factor multiplicativo para incrementar la oferta este año
            fact_mult <- mean(datos$y[datos$ds<ofertas[[a]]$fecha_inicio_ahora])/mean(datos_oferta$y[datos_oferta$ds<ofertas[[a]]$fecha_inicio_pasado])
            # Calcular nueva predicción en base a lo que ocurrio el año anterior
            nueva_pred <- datos_oferta$y[datos_oferta$ds==fecha_oferta_seleccionada_pasado]*fact_mult
            # Ver cuanto ha crecido para poder cambiar los intervalos de confianza
            inc_para_intervalos <- nueva_pred/dataFin[which(dataFin$ds==Sys.Date()-1),1]
            # Sustituir los nuevos prediccion en la tabla
            dataFin[which(dataFin$ds==Sys.Date()-1),1] <- nueva_pred
            dataFin[which(dataFin$ds==Sys.Date()-1),2] <- dataFin[which(dataFin$ds==Sys.Date()-1),2]*inc_para_intervalos
            dataFin[which(dataFin$ds==Sys.Date()-1),3] <- dataFin[which(dataFin$ds==Sys.Date()-1),3]*inc_para_intervalos
          }
        }
      }