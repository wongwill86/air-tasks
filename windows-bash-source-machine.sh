#!/bin/bash
echo "Setting docker environment to machine $1..."
docker-machine.exe start $1
CMD=$(docker-machine.exe env $1 | \
	grep DOCKER | \
	sed 's/\r/;\n/g' | \
	sed 's/SET/export/' | \
	sed 's/C:\\/\/mnt\/c\//' | \
	sed 's/\\/\//g'
	)
echo "Setting environment variables..."
echo "$CMD"
eval "$CMD"
