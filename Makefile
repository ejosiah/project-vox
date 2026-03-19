VENV?=env
PYTHON := env/bin/python
PIP := env/bin/pip
export PYTHONPATH=.

VIDEO_FILE?=./monologe.MOV
OUTPUT_TYPES?=txt json
BOOTSTRAP_SERVERS?=localhost:9092
TOPIC?=vox.jobs.request

PROTO_DIR=proto
PROTO_OUT=.

PROTO_FILES := $(PROTO_DIR)/vox/common.proto \
               $(PROTO_DIR)/vox/job_request.proto \
               $(PROTO_DIR)/vox/job_status.proto \
               $(PROTO_DIR)/vox/job_result.proto

.PHONY: kafka-up kafka-down kafka-logs \
        topic-create topic-list topic-delete \
		proto submit-job format lint test  \
		topic-consume topic-consume-beginning run

proto:
	python -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=$(PROTO_OUT) \
		$(PROTO_FILES)


.PHONY: install

format:
	black .

lint:
	ruff check .

test:
	pytest

install:
	$(PIP) install -r requirements.txt

submit-job:
	$(PYTHON) scripts/submit_job.py \
		--video-file $(VIDEO_FILE) \
		--output-types $(OUTPUT_TYPES) \
		--bootstrap-servers $(BOOTSTRAP_SERVERS) \
		--topic $(TOPIC)

KAFKA_CONTAINER := kafka
KAFKA_BIN := /opt/kafka/bin
BOOTSTRAP := localhost:9092

kafka-up:
	docker-compose up -d

kafka-down:
	docker-compose down

kafka-logs:
	docker logs -f $(KAFKA_CONTAINER)

# Create a topic: make topic-create TOPIC=test
topic-create:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--create \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP) \
		--partitions 1 \
		--replication-factor 1

# List topics
topic-list:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--list \
		--bootstrap-server $(BOOTSTRAP)

# Delete a topic: make topic-delete TOPIC=test
topic-delete:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-topics.sh \
		--delete \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP)

topic-consume:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-consumer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP)

topic-consume-beginning:
	docker exec -it $(KAFKA_CONTAINER) $(KAFKA_BIN)/kafka-console-consumer.sh \
		--topic $(TOPIC) \
		--bootstrap-server $(BOOTSTRAP) \
		--from-beginning

run:
	$(PYTHON) -m app.main