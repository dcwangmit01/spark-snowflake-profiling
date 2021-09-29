SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON_DEPS := docopt pyspark pyyaml yamllint

install:
	pipenv run pip install c7n c7n-org $(PYTHON_DEPS)

aws-regions: aws-regions.txt  ## Create a list of aws-regions
aws-regions.txt:
	mkdir -p .tmp
	aws --region us-west-2 ec2 describe-regions \
	  | jq -r .Regions[].RegionName > aws-regions.txt

config: aws-regions  ## Generate or Update configurations
	@# Update the config.yaml with the aws regions
	@#   First clear out the yaml node (array)
	yq e -i '.config.aws.regions = []' config.yaml
	@#   Then add elements to the node (array)
	cat aws-regions.txt | xargs -n 1 -I "{}" yq e -i '.config.aws.regions |= . + ["{}"]' config.yaml

spark-start:  ## Start Spark Cluster (in Docker)
	make -C spark up

spark-stop:  ## Stop Spark Cluster (in Docker)
	make -C spark stop

es-start:  ## Start Elasticsearch (in Docker)
	docker-compose -f docker-compose-es.yaml up -d

es-stop:  ## Stop Elasticsearch (in Docker)
	docker-compose -f docker-compose-es.yaml stop -d

.PHONY: cc-data
cc-data: config ## Run Cloud Custodian to Fetch all data/all profiles/all regions
	@# Create the all-resources.yaml config file
	@#   Configures Cloud Custodian to probe all resources
	cat config.yaml | yq e '.config.resources[]' - | \
	  ./cc-data/gen-resources.py > ./cc-data/all-resources.yaml

	@AWS_PROFILES=$$(cat config.yaml | yq e '.config.aws.profiles[]' -) && \
	   AWS_REGIONS=$$(cat config.yaml | yq e '.config.aws.regions[]' - | awk '{print "--region " $$1}') && \
	   for profile in $$AWS_PROFILES; do \
	     custodian run  --profile $$profile --output-dir ./cc-data/out/$$profile \
	       $$AWS_REGIONS ./cc-data/all-resources.yaml; \
	   done

cc-spark:  ## Run Spark to process the program outputs
	MODULE=cc-data && \
	  rm -rf ./spark/dfs/$$MODULE && cp -r $$MODULE ./spark/dfs && \
	  cp config.yaml ./spark/dfs/$$MODULE && \
	(cd spark && source .envrc && \
	  docker-compose exec spark-client pip3 install $(PYTHON_DEPS) && \
	  time ./docker-spark-submit \
	    --packages org.elasticsearch:elasticsearch-spark-30_2.12:7.15.0 \
	    $${SPARK_HOME}/dfs/$$MODULE/run.py \
	    $${SPARK_HOME}/dfs/$$MODULE/config.yaml \
            $${SPARK_HOME}/dfs/$$MODULE/out)
	rm -rf ./spark/dfs/$$MODULE

clean:  ## Remove all data
	rm -rf .tmp ./cc-data/out

help: ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep --with-filename -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f2- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | sort
