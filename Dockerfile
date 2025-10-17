FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install system deps required for some audio/ML packages (kept minimal).
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ffmpeg git \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest first for efficient caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project
COPY . /app

# Expose the port the app uses (default in server.py)
EXPOSE 5501

# Run the FastAPI app via uvicorn
# Use PORT env var if provided (Railway/Render), otherwise default to 5501
CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port ${PORT:-5501}"]
