# /bin/bash
docker container rm $(docker container ls -aq)
docker volume rm $(docker volume ls -q)
docker image rm $(docker images -q)
docker system prune -f 