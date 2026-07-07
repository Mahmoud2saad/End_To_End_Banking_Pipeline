# kafka/consumer/Dockerfile
#
# Containerizes the consumer as an always-on service (restart:unless-stopped
# in docker-compose), matching how the broker itself already runs -- rather
# than depending on someone leaving a terminal window open with
# `python kafka/consumer/consume_to_bronze.py` running.

FROM python:3.12-slim

WORKDIR /app

COPY kafka/consumer/requirements.txt kafka/consumer/requirements.txt
RUN pip install --no-cache-dir -r kafka/consumer/requirements.txt

# IMPORTANT: preserved at kafka/consumer/... (not flattened to /app/) --
# consume_to_bronze.py computes its own project root as
# Path(__file__).resolve().parents[2], which assumes it lives 2 directories
# below the project root, exactly like it does on your host machine
# (Banking_pipeline/kafka/consumer/consume_to_bronze.py). Flattening this
# into /app/consume_to_bronze.py directly broke that assumption -- parents[2]
# ran off the end of a 1-level-deep path with only 2 parents total,
# producing "IndexError: 2". Keeping the same nesting here means
# PROJECT_ROOT correctly resolves to /app inside the container, with zero
# changes needed to the actual Python source.
COPY kafka/consumer/consume_to_bronze.py kafka/consumer/consume_to_bronze.py
COPY data_simulation/config.py data_simulation/config.py
RUN touch data_simulation/__init__.py

# .env is deliberately NOT copied into the image -- baking secrets into an
# image layer means anyone with the image has them forever, even after
# rotation. Passed in at runtime via docker-compose's `env_file:` instead
# (see kafka/docker-compose.yml). consume_to_bronze.py's own
# load_dotenv(PROJECT_ROOT / ".env") call silently no-ops if that path
# doesn't exist in the container -- python-dotenv doesn't raise on a
# missing file, so this is harmless; real env vars still arrive via
# docker-compose's env_file mechanism.
#
# local_warehouse/ is deliberately NOT baked in either -- docker-compose
# mounts your actual local_warehouse/ as a volume at /app/local_warehouse,
# which now correctly matches what config.py's BASE_DIR resolves to
# (parent of data_simulation/, i.e. /app) now that the nesting is fixed.

CMD ["python", "kafka/consumer/consume_to_bronze.py"]
