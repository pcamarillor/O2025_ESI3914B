FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Create app directory
WORKDIR /app

# Copy requirements (optional but recommended)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy crawler code
COPY producerStream.py .

# Set the default command (usage args override this)
ENTRYPOINT ["python3", "producerStream.py"]
