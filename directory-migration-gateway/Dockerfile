# Pure-stdlib services: no pip install is required at build or run time.
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY gateway.py directory.py ./
COPY static ./static
COPY artifacts ./artifacts

# Gateway state (config.json / breakers.json) lives on a named volume.
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080 9000 9001

# Default command runs the gateway; docker-compose overrides it for the
# two directory instances.
CMD ["python", "-I", "gateway.py"]
