FROM apache/airflow:2.8.3-python3.11

# Instala gosu, Java (necessário para PySpark) e curl
USER root
RUN apt-get update && \
    apt-get install -y gosu openjdk-17-jdk curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Detecta a arquitetura e configura JAVA_HOME apropriadamente
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then \
      JAVA_PATH=$(find /usr/lib/jvm -name "java-17-openjdk*" -type d | head -1); \
    else \
      JAVA_PATH=/usr/lib/jvm/java-17-openjdk-amd64; \
    fi && \
    echo "JAVA_PATH=$JAVA_PATH" && \
    echo "export JAVA_HOME=$JAVA_PATH" > /etc/profile.d/java-home.sh && \
    ln -sf $JAVA_PATH /usr/lib/jvm/java-17-default || true

ENV JAVA_HOME=/usr/lib/jvm/java-17-default
ENV PATH=$PATH:$JAVA_HOME/bin

# Download and install Apache Spark
RUN mkdir -p /opt/spark && \
    curl -fsSL https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz -o /tmp/spark.tgz && \
    tar xzf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Baixa driver JDBC do PostgreSQL diretamente para o diretório de jars do Spark
RUN curl -L -o ${SPARK_HOME}/jars/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar

# Switch back to airflow user
USER airflow

COPY ./dags/etl/requirements.txt /opt/airflow/dags/etl/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/dags/etl/requirements.txt

COPY ./dags/etl /opt/airflow/dags/etl
