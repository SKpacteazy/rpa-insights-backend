FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --default-timeout=1000 --no-cache-dir -r requirements.txt

COPY . .

# Set PYTHONPATH to include the current directory so modules can be imported if needed
ENV PYTHONPATH=/app

CMD ["python", "client.py"]
