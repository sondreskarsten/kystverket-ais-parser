FROM europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/r-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --no-cache-dir pyarrow google-auth && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/parse_day.R ./parse_day.R
COPY src/entrypoint.py ./entrypoint.py

ENV PARSE_SCRIPT=/app/parse_day.R
ENV RSCRIPT_PATH=/usr/bin/Rscript

CMD ["python3", "entrypoint.py"]
