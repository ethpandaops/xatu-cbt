# Build argument for the CBT image tag
ARG CBT_TAG=latest

# Use the ethpandaops/cbt base image
FROM ethpandaops/cbt:${CBT_TAG}

# Copy the model files to the container
COPY models /models

# The entrypoint is already set in the base image (/cbt)