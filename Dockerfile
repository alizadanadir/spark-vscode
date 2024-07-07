# Use a base image with Java as Spark requires Java
FROM openjdk:8-jdk

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip

# Install PySpark
RUN pip3 install pyspark

# Install Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
RUN wget -q https://dlcdn.apache.org/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    tar xzf spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION /spark && \
    rm spark-$SPARK_VERSION-bin-hadoop$HADOOP_VERSION.tgz

# Set environment variables for Spark and PySpark
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYSPARK_PYTHON=python3

# Install code-server (VS Code in the browser)
RUN curl -fsSL https://code-server.dev/install.sh | sh

# Set the working directory to /workspace
WORKDIR /workspace

# Expose the necessary ports
EXPOSE 8080 8443

# Set the entry point for code-server
ENTRYPOINT ["code-server", "--bind-addr", "0.0.0.0:8443", "--auth", "none"]
