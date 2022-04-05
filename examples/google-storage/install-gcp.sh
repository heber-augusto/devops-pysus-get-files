NOMESHELL=$(basename $0)
echo ">>>"
echo ">>> Iniciando $NOMESHELL..."
echo ">>> Checks iniciais de ambiente..."
echo ">>> Parametros de processamento..."
vrc=0
if [ ! "$1" ]
then
  echo ">>> Parametro 1: Nome do projeto nao informado!"
  vrc=20
fi

if [ ! "$2" ]
then
  echo ">>> Parametro 2: arquivo de service account invalido!"
  vrc=21
fi

if [ ! "$3" ]
then
  echo ">>> Parametro 3: nome do bucket nao fornecido!"
  vrc=22
fi

if [ ! "$4" ]
then
  echo ">>> Parametro 4: pasta local invalida!"
  vrc=23
fi

GOOGLE_PRJ_NAME=$1
GOOGLE_ACC_JSON=$2
GOOGLE_BUCKET_NAME=$3
LOCAL_FOLDER=$4

echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
sudo apt-get install apt-transport-https ca-certificates gnupg
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
sudo apt-get update && sudo apt-get install google-cloud-sdk

export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s`
echo "deb http://packages.cloud.google.com/apt $GCSFUSE_REPO main" | sudo tee /etc/apt/sources.list.d/gcsfuse.list
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add -
sudo apt-get update
sudo apt-get install gcsfuse

export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_ACC_JSON}"

sudo groupadd fuse
sudo usermod -a -G fuse $USER

sudo gcloud auth activate-service-account --project=${GOOGLE_PRJ_NAME} --key-file=${GOOGLE_ACC_JSON}
sudo gcloud config set project ${GOOGLE_PRJ_NAME}
sudo mkdir ${LOCAL_FOLDER}
gcsfuse -o allow_other --dir-mode=777 --file-mode=777 --key-file=${GOOGLE_ACC_JSON} --implicit-dirs ${GOOGLE_BUCKET_NAME} ${LOCAL_FOLDER}