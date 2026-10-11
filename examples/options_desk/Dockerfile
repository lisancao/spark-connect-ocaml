# The official Spark 4.2.0 image with PyArrow and pandas, which Python data sources need.
FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu
USER root
RUN python3 -m pip install --no-cache-dir "pyarrow>=18.0.0" "pandas>=2.2.0,<3" \
 && mkdir -p /tmp/warehouse && chown spark /tmp/warehouse   # shared Delta warehouse volume
USER spark
