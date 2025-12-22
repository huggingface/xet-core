FROM rust:1.82-slim

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies (network tools for simulation)
# Work around GPG key issues by using --allow-unauthenticated for update
RUN (apt-get update -o Acquire::AllowInsecureRepositories=true -o Acquire::AllowDowngradeToInsecureRepositories=true 2>&1 | grep -v "^W:" || \
    (echo 'Acquire::Check-Valid-Until "false";' > /etc/apt/apt.conf.d/99no-check && \
     echo 'Acquire::AllowInsecureRepositories "true";' >> /etc/apt/apt.conf.d/99no-check && \
     apt-get update)) && \
    apt-get install -y --no-install-recommends --allow-unauthenticated \
    pkg-config \
    libssl-dev \
    iproute2 \
    iputils-ping \
    net-tools \
    ca-certificates \
    git \
    && rm -rf /var/lib/apt/lists/* /etc/apt/apt.conf.d/99no-check 2>/dev/null || true && \
    (which ip || test -x /sbin/ip || test -x /usr/sbin/ip || (echo "ERROR: ip command not found after installing iproute2" && exit 1))

# Rust is already installed in the base image, but ensure we have the latest stable
RUN rustup update stable && rustup default stable

# Set working directory
WORKDIR /workspace

# Note: The project will be mounted as a volume at runtime
# We'll build the test binaries during container startup

# Note: Network utilities will be loaded from the mounted volume

# Create entrypoint script
RUN echo '#!/bin/bash\n\
    # Source network utilities from copied script\n\
    if [ -f /usr/local/bin/network_utils.sh ]; then\n\
    source /usr/local/bin/network_utils.sh\n\
    else\n\
    echo "Warning: network_utils.sh not found, network simulation functions unavailable"\n\
    fi\n\
    \n\
    # Default to no network simulation\n\
    clear_network 2>/dev/null || true\n\
    \n\
    # Start bash with network utilities available\n\
    exec "$@"\n\
    ' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["/bin/bash"]
