FROM python:3.13.11-slim

RUN pip install pandas pyarrow sqlalchemy tqdm click psycopg2-binary

WORKDIR /code

COPY ingest_data.py .

# ENTRYPOINT [ "python", "ingest_data.py", "12"]
ENTRYPOINT [ "python", "ingest_data.py"]