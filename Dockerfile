FROM bde2020/spark-submit:3.1.2-hadoop3.2

WORKDIR /app

COPY data_analysis_docker.py .
COPY start.sh .
CMD ["./start.sh"]

