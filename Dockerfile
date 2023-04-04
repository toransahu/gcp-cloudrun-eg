FROM python:3.8.12-slim-bullseye AS debian-python-base
# -e: Exit immediately if any command has non-zero exit status
# -u: Exit immediately if any undefined variable is referenced
# -x: stdout the executed commands
# -o pipefail: set the bash option `pipefail`, i.e. If  pipefail is enabled,
#   the pipeline's return status is the value of the last (rightmost) command
#   to exit with a non-zero status, or zero if all commands exit successfully. (ref `man bash`)
# -c: commands  are  read  from the first non-option argument command_string. (ref `man bash`)
SHELL ["/bin/bash", "-euxo", "pipefail", "-c"]
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        ca-certificates=20210119 \
    && rm -rf /var/lib/apt/lists/*

FROM debian-python-base AS job
WORKDIR /home/job
COPY ./requirements.txt ./
RUN pip install -r requirements.txt
COPY ./hello_world.py ./
COPY ./gcs_api.py ./
# CMD ["python", "hello_world.py"]
CMD ["python", "gcs_api.py"]


