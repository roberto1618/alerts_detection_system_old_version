#!/bin/sh


###############
# Script de predición de deteccion alertas
# 
##############

ANYOMES=`date +%Y%m`



if [ $# -ne 1 ]
then
	echo
	echo
	echo "Script para el lanzamiento de alertas"
	echo "El script debe ejecutarse todos los días de la semana para enviar una alerta diaria"
	echo
	echo "Usage: "
	echo "           ./lanza_script_alertas.sh \"[configFile]\" "
	echo
	echo "Introduce valores validos de configuraciones. Cada configuración pertenece a un cliente y formato de ejecución (producción o pruebas). Valores admitidos :"
	for I in `ls ./config`
	do
		echo $I
	done

	echo
	echo "Ejemplos:"
	echo "            ./lanza_script_alertas.sh axa_dev.json"
	echo "            ./lanza_script_alertas.sh axa_prod.json"
	echo "            ./lanza_script_alertas.sh iberdrola_dev.json"
	echo "            ./lanza_script_alertas.sh iberdrola_prod.json"
else
	CONFIG=$1

	FICHLOG=./logs/loggings${ANYOMES}.log

	echo "Ejecutamos el script de prediccion de metricas, para el cliente "${CONFIG}

	echo `date +%Y-%m-%d`>> ${FICHLOG}
	echo "---------------------------------------------------------------" >> ${FICHLOG}
	/usr/lib64/R/bin/Rscript --verbose obtener_alertas.R --config=${CONFIG} --logfile=${FICHLOG} 
	echo "---------------------------------------------------------------" >> ${FICHLOG}
fi
