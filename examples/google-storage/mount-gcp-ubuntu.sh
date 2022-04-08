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

sudo groupadd fuse
sudo usermod -a -G fuse $USER

export GOOGLE_APPLICATION_CREDENTIALS="${GOOGLE_ACC_JSON}"
sudo umount -l ${LOCAL_FOLDER}
sudo gcloud auth activate-service-account --project=${GOOGLE_PRJ_NAME} --key-file=${GOOGLE_ACC_JSON}
sudo gcloud config set project ${GOOGLE_PRJ_NAME}
gcsfuse -o allow_other --dir-mode=777 --file-mode=777 --key-file=${GOOGLE_ACC_JSON} --implicit-dirs ${GOOGLE_BUCKET_NAME} ${LOCAL_FOLDER}