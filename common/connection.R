connect2BQ <- function(credFile, tokenFile, refreshToken=FALSE){
    loginfo("Loading credentials file ...")
    if (refreshToken){
        token<-token$refresh()
    } else {
        load(tokenFile)
    }
    
    ValidateToken(token)
    loginfo('Credentials validated...')

    # Credenciales bigquery
    BQ_credential_name <- credFile
    bq_auth(path = BQ_credential_name)
    bqr_auth(json_file = BQ_credential_name)
    loginfo('Connected to BQ...')

    return(token)
}