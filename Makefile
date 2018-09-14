# Copyright 2015 The Prometheus Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

GO           := go
FIRST_GOPATH := $(firstword $(subst :, ,$(shell $(GO) env GOPATH)))
PROMU        := $(FIRST_GOPATH)/bin/promu
pkgs          = $(shell $(GO) list ./... | grep -v /vendor/)

PREFIX              ?= $(shell pwd)
BIN_DIR             ?= $(shell pwd)
DOCKER_IMAGE_NAME   ?= mysqld-exporter
DOCKER_IMAGE_TAG    ?= $(subst /,-,$(shell git rev-parse --abbrev-ref HEAD))


all: verify-vendor format build test-short

style:
	@echo ">> checking code style"
	@! gofmt -d $(shell find . -path ./vendor -prune -o -name '*.go' -print) | grep '^'

test-short:
	@echo ">> running short tests"
	@$(GO) test -short -race $(pkgs)

test:
	@echo ">> running tests"
	@$(GO) test -race $(pkgs)

verify-vendor:
	@echo ">> verify that vendor/ is in sync with code and Gopkg.*"
	curl https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64 -L -o ~/dep && chmod +x ~/dep
	rm -fr vendor/
	~/dep ensure -v -vendor-only
	git diff --exit-code

format:
	@echo ">> formatting code"
	@$(GO) fmt $(pkgs)

vet:
	@echo ">> vetting code"
	@$(GO) vet $(pkgs)

build: promu
	@echo ">> building binaries"
	@$(PROMU) build --prefix $(PREFIX)

tarball: promu
	@echo ">> building release tarball"
	@$(PROMU) tarball --prefix $(PREFIX) $(BIN_DIR)

docker:
	@echo ">> building docker image"
	@docker build -t "$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG)" .

promu:
	@GOOS=$(shell uname -s | tr A-Z a-z) \
		GOARCH=$(subst x86_64,amd64,$(patsubst i%86,386,$(shell uname -m))) \
		$(GO) get -u github.com/prometheus/promu


do:
	GOOS=linux go build mysqld_exporter.go && \
	     docker exec pmm-client-mysql pmm-admin stop --all && \
	     docker cp mysqld_exporter pmm-client-mysql:/usr/local/percona/pmm-client/mysqld_exporter && \
	     docker exec -e DATA_SOURCE_NAME="root:@/" pmm-client-mysql pmm-admin start --all && \
		 docker exec pmm-client-mysql pmm-admin list


# pmm-admin add mysql:metrics MySQL57 -- -collect.custom_query=true -queries-file-name=/usr/local/percona/pmm-client/queries-mysqld.yml

exec:
	GOOS=linux go build mysqld_exporter.go && \
	docker cp mysqld_exporter pmm-client-mysql:/usr/local/percona/pmm-client/mysqld_exporter && \
	docker exec -e DATA_SOURCE_NAME="root:@/" pmm-client-mysql /usr/local/percona/pmm-client/mysqld_exporter -web.listen-address=172.19.0.4:42022 -web.auth-file=/usr/local/percona/pmm-client/pmm.yml -web.ssl-key-file=/usr/local/percona/pmm-client/server.key -web.ssl-cert-file=/usr/local/percona/pmm-client/server.crt -collect.auto_increment.columns=true -collect.binlog_size=true -collect.global_status=true -collect.global_variables=true -collect.info_schema.innodb_metrics=true -collect.info_schema.innodb_cmp=true -collect.info_schema.innodb_cmpmem=true -collect.info_schema.processlist=true -collect.info_schema.query_response_time=true -collect.info_schema.tables=true -collect.info_schema.tablestats=true -collect.info_schema.userstats=true -collect.perf_schema.eventswaits=true -collect.perf_schema.file_events=true -collect.perf_schema.indexiowaits=true -collect.perf_schema.tableiowaits=true -collect.perf_schema.tablelocks=true -collect.slave_status=true -collect.custom_query=true -queries-file-name=/usr/local/percona/pmm-client/queries-mysqld.yml


.PHONY: all style format build test vet tarball docker promu
