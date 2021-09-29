.DEFAULT_GOAL := help
SHELL := /bin/bash

PYTHON_DEPS := docopt

.PHONY: up
up:  ## Bring up all docker containers
	source .envrc && docker-compose up -d
	source .envrc && docker-compose exec spark-client pip3 install $(PYTHON_DEPS)

.PHONY: down
down:  ## Shutdown all docker containers
	source .envrc && docker-compose down --volumes --remove-orphans

.PHONY: logs
logs:  ## Tail the docker-compose logs.
	source .envrc && docker-compose logs -f

.PHONY: clean
clean: down  ## Clean all temporary data.
	rm -rf dfs/*

.PHONY: restart
restart: down up  ## Restart all docker containers.

.PHONY: reset
reset: down clean up  ## Restart all docker containers and clean all temporary data.

.PHONY: client
client:  ## Open a shell in the spark-client container.
	source .envrc && docker-compose exec spark-client bash

help:  ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f1- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
