FROM apache/spark:3.5.0

# Switch to root (Apache image already runs as root)
USER root

# Add PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.1.jar /opt/spark/jars/

# Set working directory
WORKDIR /opt/spark/work-dir
