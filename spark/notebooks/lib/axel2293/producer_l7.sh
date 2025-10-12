#!/bin/bash

INTERVAL=1 # seconds

# Function to generate a random log level
get_random_level() {
    levels=("INFO" "WARN" "ERROR" "DEBUG")
    echo "${levels[$RANDOM % ${#levels[@]}]}"
}

# Function to generate a random message
get_random_message() {
    messages=(
        "User 'admin' logged in successfully."
        "Database connection established."
        "File 'config.ini' not found, using default settings."
        "Failed to process request: invalid input."
        "System performance is optimal."
        "Memory usage exceeded threshold."
        "Starting background task 'data_cleanup'."
        "Authentication failed for user 'guest'."
        "API call to external service successful."
        "Disk space low on /var/log."
    )
    echo "${messages[$RANDOM % ${#messages[@]}]}"
}

# Delete the log files
rm -rf "/opt/spark/work-dir/data/l7/*"

echo "Generating simulated log entries to $LOG_FILE. Press Ctrl+C to stop."

while true; do
    TIMESTAMP=$(date +"%Y-%m-%d %H:%M:%S")
    LEVEL=$(get_random_level)
    MESSAGE=$(get_random_message)

    LOG_TIME=$(date +"%Y%m%d_%H%M%S")
    LOG_FILE="$OUTPUT_DIR/app_$TIMESTAMP.log"
    echo "$TIMESTAMP [$LEVEL] $MESSAGE" >> "/opt/spark/work-dir/data/l7/app_$LOG_TIME.log"
    
    sleep "$INTERVAL"
done
