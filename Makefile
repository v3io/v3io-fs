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

all:
	$(error please pick a target)

test:
	find v3iofs -name '*.pyc' -exec rm {} \;
	find tests -name '*.pyc' -exec rm {} \;
	flake8 v3iofs tests
	python -m pytest \
	    -rf -v \
	    --disable-warnings \
	    --doctest-modules \
	    tests v3iofs

test-docker:
	docker build \
	    -f tests/Dockerfile \
	    --rm \
	    --build-arg V3IO_API=$(V3IO_API) \
	    --build-arg V3IO_ACCESS_KEY=$(V3IO_ACCESS_KEY) \
	    .

test-docker-fsspec-6:
	docker build \
	    -f tests/Dockerfile.fsspec-6 \
	    --rm \
	    --build-arg V3IO_API=$(V3IO_API) \
	    --build-arg V3IO_ACCESS_KEY=$(V3IO_ACCESS_KEY) \
	    .

publish:
	@echo Checking clean tree
	test -z "$(shell git status -su)"
	@echo "Checking for VERSION in environment (make publish VERSION=1.2.3)"
	test -n "$(VERSION)"
	sed -i "s/__version__ = '.*'/__version__ = '$(VERSION)'/" \
	    v3iofs/__init__.py
	git commit -m 'version $(VERSION)' v3iofs/__init__.py
	rm -fr dist build
	python setup.py sdist
	python -m twine upload dist/*.tar.gz
	git tag version-$(VERSION)
	git push && git push --tags
