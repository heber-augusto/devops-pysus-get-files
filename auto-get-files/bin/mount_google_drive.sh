sudo add-apt-repository --yes ppa:alessandro-strada/ppa
sudo apt-get update
sudo apt-get install google-drive-ocamlfuse

mkdir "${1}/datalake"
chmod o+w "${1}/datalake"/.
mkdir "${1}/gdfuse"
chmod o+w "${1}/gdfuse"/.

google-drive-ocamlfuse -xdgbd -label monitor -serviceaccountpath "${4}" -serviceaccountuser "${2}"

python3 ${1}/auto-get-files/utils/gdfuse_config.py -c "${1}/gdfuse/monitor/config" -t "${3}"

google-drive-ocamlfuse -cc -label monitor ${1}/datalake
