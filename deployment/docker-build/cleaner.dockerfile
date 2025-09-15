FROM ubuntu:24.04 as base

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update && apt-get -y upgrade && apt-get install -y software-properties-common
RUN add-apt-repository -y ppa:deadsnakes/ppa

# Runtime + build packages
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     libpq5 \
     python3.12

FROM base as builder

# Build-only packages
RUN apt-get update && apt-get install -y \
    build-essential \
    python3.12-dev \
    python3.12-venv \
    libpq-dev

# Create virtualenv
RUN python3.12 -m venv /opt/venv

# Install pip
ENV PIP_NO_CACHE_DIR yes
RUN /opt/venv/bin/python3.12 -m pip install --upgrade pip wheel
ENV PATH="/opt/venv/bin:${PATH}"

# Install only the minimal dependencies needed for ipfs_pin_cleaner.py
RUN pip install \
    aioipfs~=0.7.1 \
    asyncpg==0.30

FROM base

RUN groupadd -g 1000 -o aleph
RUN useradd -s /bin/bash -u 1000 -g 1000 -o aleph

COPY --from=builder --chown=aleph /opt/venv /opt/venv

# Copy only the ipfs_pin_cleaner.py script
RUN mkdir -p /opt/cleaner/deployment/scripts
COPY --chown=aleph ./ipfs_pin_cleaner.py /opt/cleaner/deployment/scripts/

ENV PATH="/opt/venv/bin:${PATH}"
WORKDIR /opt/cleaner
USER aleph

# Default entrypoint to run the cleaner script
ENTRYPOINT ["python3.12", "deployment/scripts/ipfs_pin_cleaner.py"]
