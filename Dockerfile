FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN python -m grpc_tools.protoc -Ipayment_service --python_out=/app --grpc_python_out=/app payment_service/payment.proto
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "api_gateway.main:app", "--host", "0.0.0.0", "--port", "8000"]
