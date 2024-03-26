FROM apache/airflow:2.8.3

COPY requirements.txt /requirements.txt

RUN echo "from plugins.extract_load.utils import ExtractData" > __init__.py
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt