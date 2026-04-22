FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["gunicorn","--timeout","180","--access-logfile","-","--error-logfile","-","-b","0.0.0.0:8000","wsgi:app"]