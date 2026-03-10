ARG QIIME_BASE_IMAGE=quay.io/qiime2/amplicon:2026.1
FROM ${QIIME_BASE_IMAGE} AS base

ENV PYTHONUNBUFFERED=1
WORKDIR /app
FROM base AS dev

COPY ./pyproject.toml ./uv.lock /app/
COPY ./README.md /app/
COPY ./src /app/src

# Skip UV and use QIIME conda env
RUN pip install .

RUN pip uninstall pyOpenSSL -y || true


FROM dev AS production
WORKDIR /app

CMD ["adagio", "--help"]

