#!/bin/bash
DOCKER_SOCKET=/var/run/docker.sock
DOCKER_GROUP=docker
PUCKEL_ENTRYPOINT=/entrypoint.sh

# In order run DooD (Docker outside of Docker) we need to make sure the
# container's docker group id matches the host's group id. If it doens't match,
# update the group id and then restart the script. (also remove sudoer privs)
if [ ! -S ${DOCKER_SOCKET} ]; then
    echo 'Docker socket not found!'
else
    DOCKER_GID=$(stat -c '%g' $DOCKER_SOCKET)
    USER_GID=$(id -G ${AIRFLOW_USER})
    echo "User ${AIRFLOW_USER} belongs to the groups $USER_GID"
    if $(echo $USER_GID | grep -qw $DOCKER_GID); then
        echo "Host Docker Group ID $DOCKER_GID found on user"
    else
        echo "Host Docker Group ID $DOCKER_GID not found on user"
        echo "Updating docker group to host docker group"
        sudo groupmod -g ${DOCKER_GID} ${DOCKER_GROUP}
		# add this for boot2docker
		sudo usermod -aG 100,50 $AIRFLOW_USER
    fi
fi

# this doens't protect from docker but it's a little more secure
sudo sed -i "/$AIRFLOW_USER/d" /etc/sudoers
echo "start script"
exec sg $DOCKER_GROUP "$PUCKEL_ENTRYPOINT $*"
