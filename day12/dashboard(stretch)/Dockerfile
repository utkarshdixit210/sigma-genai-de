FROM python:3.12-slim

WORKDIR /app

# Copy application files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

# Expose Streamlit port
EXPOSE 8501

# Streamlit config: disable CORS and XSRF for App Runner
# Run on 0.0.0.0 so the container is accessible externally
CMD ["streamlit", "run", "app.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true", \
     "--browser.gatherUsageStats=false"]
