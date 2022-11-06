# Copyright 2020 Iguazio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# We only want to format and lint checked in python files
CHECKED_IN_PYTHING_FILES := $(shell git ls-files | grep '\.py$$')

FLAKE8_OPTIONS := --max-line-length 120
BLACK_OPTIONS := --line-length 120
ISORT_OPTIONS := --profile black

.PHONY: all
all:
	$(error please pick a target)

.PHONY: fmt
fmt:
	@echo "Running black fmt..."
	python -m black $(BLACK_OPTIONS) $(CHECKED_IN_PYTHING_FILES)
	python -m isort $(ISORT_OPTIONS) $(CHECKED_IN_PYTHING_FILES)

.PHONY: lint
lint: flake8 fmt-check

.PHONY: fmt-check
fmt-check:
	@echo "Running black+isort fmt check..."
	python -m black $(BLACK_OPTIONS) --check --diff $(CHECKED_IN_PYTHING_FILES)
	python -m isort --check --diff $(ISORT_OPTIONS) $(CHECKED_IN_PYTHING_FILES)

.PHONY: flake8
flake8:
	@echo "Running flake8 lint..."
	python -m flake8 $(FLAKE8_OPTIONS) $(CHECKED_IN_PYTHING_FILES)

.PHONY: test
test:
	python -m pytest \
	    -rf -v \
	    --disable-warnings \
	    --doctest-modules \
	    tests v3iofs

.PHONY: test-docker
test-docker:
	docker build \
	    -f tests/Dockerfile \
	    --rm \
	    --build-arg V3IO_API=$(V3IO_API) \
	    --build-arg V3IO_ACCESS_KEY=$(V3IO_ACCESS_KEY) \
	    .

.PHONY: test-docker-fsspec-6
test-docker-fsspec-6:
	docker build \
	    -f tests/Dockerfile.fsspec-6 \
	    --rm \
	    --build-arg V3IO_API=$(V3IO_API) \
	    --build-arg V3IO_ACCESS_KEY=$(V3IO_ACCESS_KEY) \
	    .

.PHONY: env
env:
	pip install -r requirements.txt

.PHONY: dev-env
dev-env: env
	pip install -r dev-requirements.txt

.PHONY: dist
dist: dev-env
	python -m build --sdist --wheel --outdir dist/ .

.PHONY: set-version
set-version:
	python set-version.py
