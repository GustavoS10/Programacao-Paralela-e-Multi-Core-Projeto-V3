FROM python:3.9-slim
WORKDIR /app
COPY servidor.py .
COPY ./requirements.txt .
RUN pip install -r requirements.txt
CMD ["python", "-u", "servidor.py"]
