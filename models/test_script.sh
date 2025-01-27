#!/bin/bash

# Check if the correct number of arguments is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <file_path>"
    exit 1
fi

# Input file path
FILE_PATH=$1

# Docker container name
CONTAINER_NAME="big-data-analytics-spark-master-1"

# Target directory in the Docker container
TARGET_DIR="/opt/bitnami/spark/models"

# Check if the file exists
if [ ! -f "$FILE_PATH" ]; then
    echo "Error: File '$FILE_PATH' does not exist."
    exit 1
fi

# Copy the file to the Docker container
echo "Copying $FILE_PATH to $CONTAINER_NAME:$TARGET_DIR..."
docker cp "$FILE_PATH" "$CONTAINER_NAME:$TARGET_DIR"
if [ $? -ne 0 ]; then
    echo "Error: Failed to copy file to container."
    exit 1
fi

# Execute the Spark job inside the Docker container
echo "Executing Spark job in $CONTAINER_NAME..."
docker exec -it $CONTAINER_NAME bash -c \
    "/opt/bitnami/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    $TARGET_DIR/$(basename "$FILE_PATH")"

if [ $? -ne 0 ]; then
    echo "Error: Spark job execution failed."
    exit 1
fi

echo "File execution completed successfully."
