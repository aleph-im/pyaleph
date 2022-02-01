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
RUN mkdir -p /opt/pyaleph/src/pyaleph.egg-info
RUN mkdir /opt/pyaleph/.eggs
RUN chown -R source:source /opt/pyaleph/src /opt/pyaleph/.eggs /opt/pyaleph/.git

# Install PyAleph source
USER source
WORKDIR /opt/pyaleph

RUN /opt/venv/bin/pip install --no-cache-dir ".[testing]"

# Save installed Python requirements for debugging
RUN /opt/venv/bin/pip freeze > /opt/build-frozen-requirements.txt

USER aleph
CMD ["pyaleph"]

# PyAleph API
EXPOSE 8000
