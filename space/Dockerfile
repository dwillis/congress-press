# Datasette serving layer for the congress-press search index.
# Holds no data — pulls press.db from the HF dataset at startup (see fetch.py).
FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "datasette>=0.65,<1.0" \
    datasette-json-html \
    datasette-search-all \
    "huggingface_hub>=0.23"

COPY fetch.py start.sh /app/
RUN chmod +x /app/start.sh

# HF Spaces route traffic to this port.
EXPOSE 7860

CMD ["/app/start.sh"]
