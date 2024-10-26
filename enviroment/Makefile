MAKEFLAGS += --always-make


# ---------------------------------------------- NETWORK ----------------------------------------------
create_network:
	docker network create --subnet=172.11.10.0/24 --gateway=172.11.10.1 --driver=bridge demo


start:
	export DOCKER_HOST_IP=$(hostname -I | awk '{print $1}')
	docker-compose up -d


stop:
	docker-compose down
