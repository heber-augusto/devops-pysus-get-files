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

#create folder to receive dbc, dbf, csv and parquet files
RUN mkdir ${HOME}/dbc-files
RUN chmod o+w ${HOME}/dbc-files/.
RUN mkdir ${HOME}/dbf-files
RUN chmod o+w ${HOME}/dbf-files/.
RUN mkdir ${HOME}/csv-files
RUN chmod o+w ${HOME}/csv-files/.
RUN mkdir ${HOME}/output-files
RUN chmod o+w ${HOME}/output-files/.




# Install gcsfuse.
# RUN echo "deb http://packages.cloud.google.com/apt gcsfuse-bionic main" | tee /etc/apt/sources.list.d/gcsfuse.list
# RUN apt-get install apt-transport-https ca-certificates gnupg
# RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
# RUN apt-get update
# RUN apt-get install -y gcsfuse

# Install gcloud.
# RUN apt-get install -y apt-transport-https
# RUN apt-get install -y ca-certificates
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
# RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
# RUN apt-get update
# RUN apt-get install -y google-cloud-sdk

# RUN gcloud auth activate-service-account --project=teak-ellipse-317015 --key-file=${HOME}/gcp.json
# RUN gcloud config set project teak-ellipse-317015
# RUN mkdir ${HOME}/output-files2
# RUN gcsfuse --implicit-dirs observatorio-oncologia ${HOME}/output-files2
# RUN mkdir ${HOME}/output-files2/monitor/heber


# Copy sources and shell scripts
RUN mkdir ${HOME}/auto-get-files
WORKDIR ${HOME}/auto-get-files
COPY auto-get-files .

RUN chmod +x collect_from_ftp.sh

WORKDIR ${HOME}/auto-get-files/blast
RUN gcc blast.c blast-dbf.c -o ../dbc-2-dbf
WORKDIR ${HOME}/auto-get-files
RUN chmod +x dbc-2-dbf

USER developer

CMD [ "python","-u", "./main.py"]
