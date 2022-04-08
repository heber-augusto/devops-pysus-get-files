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
   ```docker run --mount type=bind,source=/home/ubuntu/output-files,target=/home/developer/output-files -e "STATE=SP" -e "YEAR=2014" -e "MONTH=-1" -e "OUTPUT_DIR=/home/developer/output-files" heberaugusto/pysus-get-files:latest```

 
 The target '/home/developer/output-files' is the default path, inside docker container, where parquet files are created.