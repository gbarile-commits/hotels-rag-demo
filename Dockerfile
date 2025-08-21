Contenido de requirements.txt fastapi0.110.0 uvicorn[standard]0.29.0 pydantic2.6.1 pandas2.2.2 requests==2.32.3

Contenido de Dockerfile FROM python:3.11-slim ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 WORKDIR /app COPY requirements.txt . RUN pip install --no-cache-dir -r requirements.txt COPY app.py . EXPOSE 8000 CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
