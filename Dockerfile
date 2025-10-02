FROM python:3.12-slim
WORKDIR /programas/ingesta
RUN pip install --no-cache-dir boto3 pymysql
COPY . .
CMD ["python", "ingesta.py"]
