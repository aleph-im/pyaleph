# Monolithic Docker image for easy setup of an Aleph.im node in demo scenarios.

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND noninteractive

# Install Python dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     python3 \
     python3-dev \
     python3-pip \
     python3-venv \
     build-essential \
     git && \
     rm -rf /var/lib/apt/lists/*

# Install system dependencies
RUN apt-get update && apt-get -y upgrade && apt-get install -y \
     libsnappy-dev \
     zlib1g-dev \
     libbz2-dev \
     libgflags-dev \
     liblz4-dev \
     librocksdb-dev \
     libgmp-dev \
     libsecp256k1-dev \
     pkg-config \
     libssl-dev \
     libleveldb-dev \
     libyaml-dev && \
     rm -rf /var/lib/apt/lists/*

# ===  Create unprivileged users ===

# - User 'source' to install code and dependencies -
RUN useradd -s /bin/bash source
RUN mkdir /opt/venv
RUN chown source:source /opt/venv
# - Installed Python libraries will be save in this file
RUN touch /opt/build-frozen-requirements.txt
RUN chown source:source /opt/build-frozen-requirements.txt

# - User 'aleph' to run the code itself
RUN useradd -s /bin/bash aleph
RUN mkdir /opt/pyaleph
RUN chown aleph:aleph /opt/pyaleph

# === Install Python environment and dependencies ===
USER source

# Create virtualenv
RUN python3 -m venv /opt/venv

# Install pip
ENV PIP_NO_CACHE_DIR yes
RUN /opt/venv/bin/python3 -m pip install --upgrade pip wheel
ENV PATH="/opt/venv/bin:${PATH}"

RUN /opt/venv/bin/pip install --upgrade --use-deprecated=legacy-resolver git+https://github.com/aleph-im/aioipfs.git@hoh-more-exceptions
RUN /opt/venv/bin/pip install --upgrade --use-deprecated=legacy-resolver \
    requests "pymongo~=3.12.2" secp256k1 coincurve "aiohttp>=3.7.4"

# === Copy source code ===
COPY setup.py /opt/pyaleph/
COPY setup.cfg /opt/pyaleph/
COPY src /opt/pyaleph/src

COPY tests /opt/pyaleph/tests

# Git data is used to determine PyAleph's version
COPY .git /opt/pyaleph/.git


# === Install the application and dependencies ===

# Setup directories for `python setup.py develop`
USER root
RUN mkdir /opt/pyaleph/src/pyaleph.egg-info
RUN mkdir /opt/pyaleph/.eggs
RUN chown -R source:source /opt/pyaleph/src /opt/pyaleph/.eggs /opt/pyaleph/.git

# Install PyAleph source
USER source
WORKDIR /opt/pyaleph
RUN pip install -U --use-deprecated=legacy-resolver git+https://github.com/aleph-im/nuls2-python.git
RUN pip install -U --use-deprecated=legacy-resolver cosmospy
RUN pip install -U --use-deprecated=legacy-resolver substrate-interface
RUN python setup.py develop
RUN /opt/venv/bin/pip install --use-deprecated=legacy-resolver -e ".[testing]"

# Fix an issue with Aiohttp not working
# RUN /opt/venv/bin/pip install aiohttp==3.7.2

# Save installed Python requirements for debugging
RUN /opt/venv/bin/pip freeze > /opt/build-frozen-requirements.txt

USER aleph
CMD ["pyaleph"]

# PyAleph API
EXPOSE 8000
# PyAleph p2p network
EXPOSE 4024 4025
