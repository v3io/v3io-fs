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
	python -m pytest -rf -v --disable-warnings tests
