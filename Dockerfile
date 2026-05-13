FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Generate protobuf inside container
RUN python -m grpc_tools.protoc -I proto --python_out=generated proto/car.proto

# Run producer
CMD ["python", "producer/producer.py"]