#!/bin/bash
# Update system libraries.
sudo yum check-update
# Download and install a local instance of Docker.
sudo curl -fsSL https://get.docker.com/ | sh
# Start your instance of Docker.
sudo systemctl start docker
sudo systemctl status docker
# Open up ports on this VM so that you can access UIs running within active images.
sudo gcloud compute firewall-rules create "hue" --description="Allow access to Hue from anywhere." --priority "1000" --direction INGRESS --action allow --source-ranges="0.0.0.0/0" --rules tcp:8888
sudo gcloud compute firewall-rules create "hive" --description="Allow access to Hive from anywhere." --priority "1000" --direction INGRESS --action allow --source-ranges="0.0.0.0/0" --rules tcp:10000
# Load the cloudera/quickstart image into your Docker.
sudo docker pull cloudera/quickstart:latest
# Run the cloudera/quickstart container. This will place you within a shell within the container. When you exit the container, you will return to your shell within the VM.
sudo docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8888:8888 -p 80:80 -p 7180:7180 -p 10000:10000 cloudera/quickstart /usr/bin/docker-quickstart