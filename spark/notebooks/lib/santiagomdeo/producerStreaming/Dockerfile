FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

#i think this one is already achieved by the above but, you can never be to carefull
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY producerStream.py .

ENTRYPOINT ["python3", "producerStream.py"]
