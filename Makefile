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

.PHONY: all
all:
	$(error please pick a target)

.PHONY: lint
lint:
	flake8 v3iofs tests

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

.PHONY: set-version
set-version:
	python set-version.py

.PHONY: dist
dist:
	rm -rf dist build
	python -m build --sdist --wheel --outdir dist/ .
