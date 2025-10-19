#!/bin/bash

# Archivo donde se guardarán los logs (o puedes enviar por socket)
LOGDIR="$(dirname "$0")/../logs"
mkdir -p "$LOGDIR"
LOGFILE="$LOGDIR/logs_$(date +%s).log"


# Niveles de log
LOG_LEVELS=("INFO" "WARN" "ERROR" "DEBUG")

# Mensajes posibles
EVENTS=(
    "User logged in"
    "User logged out"
    "Database connection established"
    "Database timeout"
    "Payment processed"
    "Payment failed"
    "File uploaded successfully"
    "File upload error"
    "Cache refreshed"
    "API request failed"
)

# Genera logs infinitamente
while true; do
    LEVEL=${LOG_LEVELS[$RANDOM % ${#LOG_LEVELS[@]}]}
    EVENT=${EVENTS[$RANDOM % ${#EVENTS[@]}]}
    TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

    LOG_LINE="$TIMESTAMP [$LEVEL] $EVENT"

    echo "$LOG_LINE" | tee -a "$LOGFILE"
    
    # Pausa aleatoria entre logs (0.5 a 2 segundos)
    sleep $(awk -v min=0.5 -v max=2 'BEGIN{srand(); print min+rand()*(max-min)}')
done
