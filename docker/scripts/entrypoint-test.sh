#!/bin/bash
source /add-user-docker.sh

# this doens't protect from docker but it's a little more secure
sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers
echo "start script"
exec sg $DOCKER_GROUP "$*"
