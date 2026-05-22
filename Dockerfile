FROM europe-north1-docker.pkg.dev/sondreskarsten-d7d14/r-images/r-base:latest

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    pip3 install --no-cache-dir pyarrow google-auth requests && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY src/parse_positions.R ./parse_positions.R
COPY src/derive_voyages.R ./derive_voyages.R
COPY src/entrypoint.py ./entrypoint.py
COPY src/entrypoint_daily.py ./entrypoint_daily.py

ENV PARSE_POS_SCRIPT=/app/parse_positions.R
ENV DERIVE_VOY_SCRIPT=/app/derive_voyages.R

CMD ["python3", "entrypoint.py"]
