# Use the bitnami Spark image as base
FROM bitnami/spark:latest

# Install Python and pip if they are not installed (just in case)
USER root
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

# Install TensorFlow and Keras
RUN pip3 install --upgrade pip && \
    pip3 install tensorflow keras scikit-learn pandas pyarrow

# Switch back to the Spark user for running the container
USER 1001

# Set up the working directory and entry point (this is optional, based on your use case)
WORKDIR /opt/bitnami/spark
