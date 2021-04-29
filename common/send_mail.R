send_mail <- function(credentials, existen_alertas = TRUE){
  remitente <- credentials$emails_from
  destinatario <- credentials$emails_to
  host <- credentials$server
  remitente_pass <- credentials$password
  port <- credentials$port

  remitente_title <- paste0('El Arte de Medir <',remitente,'>')
  asunto <- paste0('Resumen Alertas ',Sys.Date()-1,' - ', capitalize(cliente))

  if (existen_alertas){
    email <- envelope() %>%
      from(remitente_title) %>%
      to(destinatario) %>%
      subject(asunto) %>%
      emayili::html(paste0('<font face="aileron" size="2.7" color="black">Estimado cliente,<br/><br/>Revisando los datos de sus métricas para el día ',Sys.Date()-1,',<B> se han detectado alertas</B> en algunas de ellas. A continuación, se muestra cada una en detalle.',str_replace_all(alerta_metricas,'___','_'),'<br/><br/>Recomendamos revisar el estado de sus métricas. Para cualquier consulta, contacte con <B>El Arte de Medir</B>.</font>')) %>%
      #attachment(paste0(toupper(cliente), "_alertas.xlsx"))
      attachment(fileAttachment)   
  } else {
    email <- envelope() %>%
      from(remitente_title) %>%
      to(destinatario) %>%
      subject(asunto) %>%
      emayili::html(paste0('<font face="aileron" size="2.7" color="black">Estimado cliente, <br/><br/>No se han detectado alertas en sus métricas para el día ',Sys.Date()-1,'.<br/><br/>Para cualquier consulta, contacte con <B>El Arte de Medir</B>.</font>')) %>%
      #attachment(paste0(toupper(cliente), "_alertas.xlsx"))
      attachment(fileAttachment)
  }

  # Validarse con la cuenta que enviara el email
  smtp <- server(host = host,
               port = port,
               username = remitente,
               password = remitente_pass)

  # Enviar el email
  smtp(email, verbose = TRUE)
}
