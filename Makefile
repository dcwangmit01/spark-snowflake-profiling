SHELL := /bin/bash
.DEFAULT_GOAL := help

install:
	pipenv run pip install c7n
	pipenv run pip install c7n-org
	pipenv run pip install pyspark
	pipenv run pip install pyyaml yamllint

aws-regions: .tmp/aws-regions.txt  ## Create a list of aws-regions
.tmp/aws-regions.txt:
	mkdir -p .tmp
	aws --region us-west-2 ec2 describe-regions \
	  | jq -r .Regions[].RegionName > .tmp/aws-regions.txt

config: aws-regions  ## Generate or Update configurations
	@# Update the config.yaml with the aws regions
	yq e -i '.config.aws.regions = []' config.yaml
	cat .tmp/aws-regions.txt | xargs -n 1 -I "{}" yq e -i '.config.aws.regions |= . + ["{}"]' config.yaml

	@# Create the all-resources.yaml config file
	@#   Used for probing all resources
	mkdir -p .tmp
	cat config.yaml | yq e '.config.resources[]' - | ./bin/gen-resources.py > .tmp/all-resources.yaml

fetch-data: config  ## Fetch all data for all profiles in all regions
	@export AWS_PROFILES=$$(cat config.yaml | yq e '.config.aws.profiles[]' -) && \
	  export AWS_REGIONS=$$(cat config.yaml | yq e '.config.aws.regions[]' - | awk '{print "--region " $$1}') && \
	  for profile in $$AWS_PROFILES; do \
	    custodian run  --profile $$profile --output-dir out/$$profile $$AWS_REGIONS .tmp/all-resources.yaml; \
	  done

process-data: config  ## Process all data
	./bin/process-data.py

clean:  ## Remove all data
	rm -rf .tmp out

help: ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep --with-filename -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f2- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | sort
