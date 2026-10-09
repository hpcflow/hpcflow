# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.13
FROM python:${PYTHON_VERSION}-slim AS build

WORKDIR /build
COPY pyproject.toml README.md LICENSE ./
COPY hpcflow ./hpcflow
RUN python -m venv /opt/venv \
    && /opt/venv/bin/python -m pip install --no-cache-dir --no-compile .

FROM python:${PYTHON_VERSION}-slim AS runtime

ARG HPCFLOW_IMAGE=hpcflow:dev
ENV HPCFLOW_CONTAINER=${HPCFLOW_IMAGE} \
    PATH="/opt/venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY --from=build /opt/venv /opt/venv
WORKDIR /work
ENTRYPOINT ["hpcflow"]
CMD ["--help"]
