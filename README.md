# air-tasks

DooD support and AWS ECR Credential Helper


NOTES:
Chunkflow: make sure AWS_ACCESS_KEY_ID, etc... are set in environment variables!
docker-compose -f docker/docker-compose.test.yml -p ci build
docker-compose -f docker/docker-compose.test.yml -p ci run --rm sut ptw


When deploying docker/docker-compose-CeleryExecutor.yml remember to deploy secrets!
( or put in blank for no web auth )
* echo '<Put some username here>' | docker secret create basic_auth_username -
* echo '<Put some password here>' | docker secret create basic_auth_password -
