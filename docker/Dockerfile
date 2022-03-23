FROM condaforge/mambaforge

LABEL maintainer="heber.augusto@gmail.com"

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get -qq update --yes \
  && apt-get -qq install --yes --no-install-recommends \
  build-essential firefox\
  ca-certificates sudo \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -ms /bin/bash developer \
  && echo "developer ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/developer \
  && chmod 0440 /etc/sudoers.d/

# Set home to developer
ENV HOME /home/developer
# Allow the application to execute as any UID 
# and still write temporary data to /tmp
ENV APP_TMP_DATA=/tmp

# Copy environment file to tmp/
COPY environment.yaml ${APP_TMP_DATA}/environment.yaml

# Use environment to update the env base
RUN mamba update --all --yes --quiet \
  && mamba env update --file ${APP_TMP_DATA}/environment.yaml --name base \
  && mamba clean -afy

#install wget
RUN apt-get install wget

#create folder to receive dbc and dbf files
RUN mkdir ${HOME}/dbc-files
RUN mkdir ${HOME}/dbf-files

# Copy sources and shell scripts
RUN mkdir ${HOME}/auto-get-files
WORKDIR ${HOME}/auto-get-files
COPY pysus/auto-get-files .

RUN chmod +x collect_from_ftp.sh

WORKDIR ${HOME}/auto-get-files/blast
RUN gcc blast.c blast-dbf.c -o ../dbc-2-dbf
WORKDIR ${HOME}/auto-get-files
RUN chmod +x dbc-2-dbf

USER developer

CMD [ "python","-u", "./sia/download_sia.py"]
