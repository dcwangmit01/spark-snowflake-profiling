SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON_DEPS := docopt pyspark pyyaml yamllint yapf flake8 ipdb

# Only set SLEEP_SECONDS if you want the job to hang around so the Spark UI is accessible
SLEEP_SECONDS := 0

install:
	pipenv run pip install $(PYTHON_DEPS)
	(cd spark && source .envrc && docker-compose exec spark-client pip3 install $(PYTHON_DEPS))

format:  ## Auto-format and check pep8
	pipenv run yapf -i $$(find * -type f -name '*.py')
	pipenv run flake8 $$(find * -type f -name '*.py)')

spark-start:  ## Start Spark Cluster (in Docker)
	make -C spark up

spark-stop:  ## Stop Spark Cluster (in Docker)
	make -C spark stop

snowflake-all: clean  ## Run all snowflake jobs
	make -C . snowflake-spark-sequential 2>&1 | tee logs/snowflake-spark-sequential.txt
	make -C . snowflake-spark-parallel 2>&1   | tee logs/snowflake-spark-parallel.txt
	make -C . snowflake-jdbc-sequential 2>&1  | tee logs/snowflake-jdbc-sequential.txt
	make -C . snowflake-jdbc-parallel 2>&1    | tee logs/snowflake-jdbc-parallel.txt


# For the commands below, some of the variables come from sourcing .envrc's
#   Anything starting with a single $ is a Makefile variable
#   Anything starting with a double $$ is a shell variable
SQL_STATEMENT := select * from $${SNOWFLAKE_DATABASE}.$${SNOWFLAKE_SCHEMA}.$${SNOWFLAKE_TABLE} limit 5000000
RUN_COMMAND := \
	MODULE=snowflake-job && \
	  rm -rf ./spark/dfs/$${MODULE} && cp -r $${MODULE} ./spark/dfs && \
	cd $${MODULE} && source .envrc && cd .. && cd spark && source .envrc \
	  docker-compose exec spark-client pip3 install $(PYTHON_DEPS) && \
	  time ./docker-spark-submit \
	    --packages net.snowflake:snowflake-jdbc:3.12.12,net.snowflake:spark-snowflake_2.12:2.8.2-spark_3.0 \
	    $${SPARK_HOME}/dfs/$${MODULE}/run.py \
	      --debug --sleep=$(SLEEP_SECONDS)


snowflake-spark-sequential:  ## Run snowflake-spark-sequential
	$(RUN_COMMAND) --$@ \
	   $${SPARK_HOME}/dfs/$${MODULE}/parquet \
	   "$(SQL_STATEMENT)"

snowflake-spark-parallel:  ## Run snowflake-spark-parallel
	$(RUN_COMMAND) --$@ \
	   $${SPARK_HOME}/dfs/$${MODULE}/parquet \
	   "$(SQL_STATEMENT)"

snowflake-jdbc-sequential:  ## Run snowflake-jdbc-sequential
	$(RUN_COMMAND) --$@ \
	   $${SPARK_HOME}/dfs/$${MODULE}/parquet \
	   "$(SQL_STATEMENT)"

snowflake-jdbc-parallel:  ## Run snowflake-jdbc-parallel
	$(RUN_COMMAND) --$@ \
	   $${SPARK_HOME}/dfs/$${MODULE}/parquet \
	   "$(SQL_STATEMENT)"

clean:  ## Remove all data
	rm -rf ./spark/dfs/* ./logs/*

help: ## Print list of Makefile targets
	@# Taken from https://github.com/spf13/hugo/blob/master/Makefile
	@grep --with-filename -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  cut -d ":" -f2- | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' | sort
