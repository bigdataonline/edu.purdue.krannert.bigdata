#!/bin/bash
if [[ `sudo systemctl status docker | egrep -c "Active: .*running.*"` -eq 0 ]] ; then
    # Docker is not running. Must start it up.
    sudo systemctl start docker
fi
# Check the list of docker images uploaded into the local docker deamon.
sudo docker image list
if [[ `sudo docker image list | grep -c 'cloudera/quickstart'` -ge 1 ]]; then
    sudo docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8888:8888 -p 80:80 -p 7180:7180 -p 10000:10000 cloudera/quickstart /usr/bin/docker-quickstart
else
    echo "Cannot find the cloudera/quickstart image in the list of available images in your docker instance. Did you load it? sudo docker pull cloudera/quickstart:latest"
fi