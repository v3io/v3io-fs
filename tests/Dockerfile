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
#
FROM python:3.8-slim
RUN apt-get update && apt-get install -y gcc make
WORKDIR /code
COPY requirements.txt .
COPY dev-requirements.txt .
RUN python -m pip install --upgrade pip
RUN python -m pip install -r requirements.txt -r dev-requirements.txt
COPY . .
ARG V3IO_API
ARG V3IO_ACCESS_KEY
ENV V3IO_API=$V3IO_API
ENV V3IO_ACCESS_KEY=$V3IO_ACCESS_KEY
RUN make test
