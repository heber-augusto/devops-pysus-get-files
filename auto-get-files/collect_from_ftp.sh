#!/bin/sh
#Primeiro parametro é o caminho até o arquivo no ftp do sus
#Segundo é o local onde o arquivo deve ser armazenado
NOMESHELL=$(basename $0)
echo ">>>"
echo ">>> Iniciando $NOMESHELL..."
echo ">>> Checks iniciais de ambiente..."
echo ">>> Parametros de processamento..."
vrc=0
if [ ! "$1" ]
then
  echo ">>> Parametro 1: Caminho nao informado!"
  vrc=20
fi

if [ ! "$2" ]
then
  echo ">>> Parametro 2: destino do arquivo inválido!"
  vrc=24
fi


if [ ${vrc} -eq "0" ]
then
   echo ">>> Parametros de processamento OK!"
else
   echo ">>>"
   echo ">>>---------------- Parametros Obrigatorios -----------------------"
   echo ">>> \$1 = Arquivo para pegar                                       "
   echo ">>> \$2 = Destino do arquivo                                       "   
   echo ">>> Exit Code=$vrc"
   exit $vrc
fi
FILE_PATH=$1
DIR_FILES_DEST=$2
wget -c -P $DIR_FILES_DEST -t 1 --user=Anonymous password= ftp://ftp.datasus.gov.br${FILE_PATH}
if [ $? -ne 0 ]; then
   vrc=25
else
   echo "Arquivo \$1 coletado com sucesso!"
fi
exit $vrc


