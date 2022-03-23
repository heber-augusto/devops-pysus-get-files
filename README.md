# devops-pysus-get-files
A docker-compose and docker files to collect SIA PA files using [PySUS library](https://github.com/AlertaDengue/PySUS/)

# Some useful information:

 - To create the image "docker-compose build" inside docker folder
 - Create a folder named sus-files or change source from the folder
 - The docker_auto-get-files docker image can run using the following syntax: 
   ```
   sudo docker run -e "STATE=ES" -e "YEAR=2020" -e "MONTH=11" docker_auto-get-files
   ```
 - The image can be found at https://hub.docker.com/repository/docker/heberaugusto/pysus-get-files

 - Example on how to call docker image and set parameters:
   ```docker run -it --mount type=bind,source="$(pwd)"/../dbf-files,target=/home/developer/dbf-files --mount type=bind,source="$(pwd)"/../dbc-files,target=/home/developer/dbc-files -e "STATE=SP" -e "YEAR=2021" -e "MONTH=4" -e "DBC_DIR=/home/developer/dbc-files" -e "DBF_DIR=/home/developer/dbf-files" heberaugusto/pysus-get-files:latest```

 The target '/home/developer/dbf-files' is the default path, inside docker container, where dbc and dbf files are created.