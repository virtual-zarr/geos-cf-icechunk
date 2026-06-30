# Build stage
FROM public.ecr.aws/lambda/python:3.14 AS builder

RUN dnf install -y git && dnf clean all

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Set uv environment variables
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Copy workspace packages — build context is the repo root
WORKDIR /build
COPY lambda/virtualizarr-processor ./lambda/virtualizarr-processor
COPY lambda/search_latest ./lambda/search_latest
COPY query-thredds ./query-thredds

# Install using uv with workspace support
WORKDIR /build/lambda/search_latest
RUN uv pip install --python /var/lang/bin/python3.14 --target /var/task --no-cache .

# Runtime stage
FROM public.ecr.aws/lambda/python:3.14

COPY --from=builder /var/task /var/task

### Set the CMD to your handler
CMD ["handler.handler"]
