FROM continuumio/miniconda3

WORKDIR /app

# Create the environment:
COPY apps.yaml .
RUN conda env create -f apps.yaml

# Make RUN commands use the new environment:
SHELL ["conda", "run", "-n", " apps", "/bin/bash", "-c"]

# The code to run when container is started:
#COPY run.py .
ENTRYPOINT ["conda", "run", "--no-capture-output", "-n", "apps"]

