IMAGE   := asterisk-dev
SRC_DIR := $(realpath ../..)
WORKDIR := /usr/src/asterisk
JOBS    := $(shell nproc)

DOCKER_RUN := docker run --rm -it \
	-v $(SRC_DIR):$(WORKDIR) \
	--workdir $(WORKDIR) \
	--network host \
	--cap-add NET_ADMIN \
	--cap-add NET_RAW \
	$(IMAGE)

CONFIGURE_FLAGS := \
	--with-pjproject-bundled \
	--enable-dev-mode

.PHONY: image shell autoconf configure build install run clean

image:
	docker build -f Dockerfile.dev -t $(IMAGE) .

shell:
	$(DOCKER_RUN) bash

autoconf:
	$(DOCKER_RUN) bash -c "autoreconf -fi -I autoconf"

configure:
	$(DOCKER_RUN) bash -c "autoreconf -fi -I autoconf && ./configure $(CONFIGURE_FLAGS)"

build:
	$(DOCKER_RUN) bash -c "make -j$(JOBS)"

install:
	$(DOCKER_RUN) bash -c "make install && make samples && ldconfig"

samples:
	$(DOCKER_RUN) bash -c "make samples"

run:
	$(DOCKER_RUN) bash -c "asterisk -cvvvv"

clean:
	$(DOCKER_RUN) bash -c "make clean"
