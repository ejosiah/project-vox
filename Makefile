PROTO_DIR=proto
PROTO_OUT=.

PROTO_FILES := $(PROTO_DIR)/vox/common.proto \
               $(PROTO_DIR)/vox/job_request.proto \
               $(PROTO_DIR)/vox/job_status.proto \
               $(PROTO_DIR)/vox/job_result.proto

.PHONY: proto

proto:
	python -m grpc_tools.protoc \
		-I$(PROTO_DIR) \
		--python_out=$(PROTO_OUT) \
		$(PROTO_FILES)

VENV?=env
.PHONY: install

install:
	$(VENV)/bin/pip install -r requirements.txt

format:
	black .

lint:
	ruff check .

test:
	pytest