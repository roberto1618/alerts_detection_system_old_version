# alert_detection_system
This project has the necessary code to detect daily possible alerts in the behaviour of many metrics. It can be used for taking decisions depending on irregular behaviour of the metrics you are measuring. For example, if the system detect an unexpected drop in the *session* metric, you can decide what to do during the next day.

## How it works
The alert detection system works through four steps:
1. Download historic data (now, two months ago) from Analytics about the metrics are specified in the *.json* files in the config folder.
2. Build a model with data until two days ago and predict the yesterday's metric value.
3. Compare the real yesterday's value with the prediction and check if an alert exist.
4. Save a table with all possible alerts in a BigQuery project and send it (with *.xlsx* extension) through *Gmail* to the client with the information of all alerts.

The model is built only on Mondays and predict data 7 days in the future. The rest of the days, the predictions are downloaded from BQ so it does not have to build the model every day. Each Monday the table is erased. The historical data is stored every Sunday and it can be consulted on the BQ tables to measure the accuracy of the model.

## The model
The model user for prediction is taken from the *prophet* package in R. It takes into account the seasonality and possible stationarity of the time series and predict the output with a confidence interval. The confidence level is now set at 95%, but it can be changed. This interval is the condition we use for determining the existence of alerts. If the real value falls outside this limits, an alert is detected.

## How to execute it
Set the working directory in your Terminal equal to the path of *obtener_alertas.R* and just run
```
Rscript obtener_alertas -c config_file
```
and the code will run the process for *config_file*.

The program is automatically executed every day by using the shell script *lanza_script_alertas.sh*. The process is now configured with an Ubuntu machine.

### Configuration and credential files
All credentials and configuration files are not available in the GitHub repo due to privacy protection. They have the information and passwords you need to run the alert detection system for the customer you want. If you want an specific or a customize configuration file, please contact with [roberto.otero161@gmail.com](mailto:roberto.otero161@gmail.com)
