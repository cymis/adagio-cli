ARG QIIME_BASE_IMAGE=quay.io/qiime2/amplicon:2024.5
FROM ${QIIME_BASE_IMAGE} as base

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy

# Development stage
FROM base as dev

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project

ENV PYTHONPATH=/app/src

COPY ./pyproject.toml ./uv.lock /app/
COPY ./README.md /app/
COPY ./src /app/src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen

# is this needed still?
RUN pip3 uninstall pyOpenSSL -y || true

# We can extend this if needed
FROM dev as production

# Set default command
CMD ["uv", "run", "adagio", "--help"]
