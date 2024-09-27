FROM python:3.9-slim

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-utils \
    python3-tk \
    curl \
    git \
    gosu \
    graphviz \
    openssh-client \
    sudo \
    vim \ 
    && rm -rf /var/lib/apt/lists/*

# Create the tdp directory
RUN mkdir -p /home/tdp/tdp-lib && chmod 777 /home/tdp/tdp-lib

# Set tdp as working directory
WORKDIR /home/tdp/tdp-lib

# Copy all files
COPY . .

# Install tdp-lib with extra dependencies
RUN pip install ".[visualization, postgresql-binary, mysql]"

# Install dependencies for tdp_extra and tdp_observability
RUN pip install passlib==1.7.4 jmespath==1.0.1

# Use docker-entrypoint;sh as entrypoint
RUN cp dev/docker-entrypoint.sh /usr/local/bin
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
