FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir .
COPY config ./config

ENTRYPOINT ["unslake"]
CMD ["simulate", "--mode", "sparkplug", "--host", "mosquitto", "--duration", "7d", "--speed", "1"]
