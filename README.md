# air-tasks

DooD support and AWS ECR Credential Helper


NOTES:
Chunkflow: make sure AWS_ACCESS_KEY_ID, etc... are set in environment variables!
export PYTHONDONTWRITEBYTECODE=1 
docker-compose -f docker/docker-compose.test.yml -p ci build
docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut ptw -- --pylama

export 

When deploying docker/docker-compose-CeleryExecutor.yml remember to deploy secrets!
( or put in blank for no web auth )
* echo '<Put some username here>' | docker secret create basic_auth_username -
* echo '<Put some password here>' | docker secret create basic_auth_password -

To deploy on swarm:

export STACK_NAME=mystack
docker stack deploy -c docker/docker-compose-CeleryExecutor.yml ${STACK_NAME}
(Swarm always pulls from registry! make sure version in docker registry is up to date!)

To deploy locally:
docker-compose -f docker/demo-docker-compose-CeleryExecutor.yml up -d
