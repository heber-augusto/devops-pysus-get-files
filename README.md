# devops-pysus-get-files
A docker-compose and docker files to collect SIA PA files using [PySUS library](https://github.com/AlertaDengue/PySUS/)

# Some useful information:

 - To create the image "docker-compose build"
 - The docker_auto-get-files docker image can run using the following syntax: "sudo docker run -e "STATE=ES" -e "YEAR=2020" -e "MONTH=11" docker_auto-get-files"
 - The image can be found at https://hub.docker.com/repository/docker/heberaugusto/pysus-get-files