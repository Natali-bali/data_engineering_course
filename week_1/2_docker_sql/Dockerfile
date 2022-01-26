FROM python:3.9
RUN apt-get install wget
RUN pip install pandas psycopg2
RUN pip install sqlalchemy
# our working directory, we can see it in bash
WORKDIR /app
COPY ingest_data.py ingest_data.py
# ENTRYPOINT ["bash"] our first test, then we change entrypoint to run script
ENTRYPOINT ["python", "ingest_data.py"]