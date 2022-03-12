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
   ```
  docker run --mount type=bind,source="$(pwd)"/sus-files,target=/home/developer/pysus -e "STATE=ES" -e "YEAR=2021" -e "MONTH=4" heberaugusto/pysus-get-files:latest
   ```

 The target '/home/developer/pysus' is the default path, inside docker container, where files are created.