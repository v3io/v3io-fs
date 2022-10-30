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
_ignore_statuses = {200, 204, 206, 409}


def handle_v3io_errors(response, file_path):
    if response.status_code in _ignore_statuses:
        return response.body
    if response.status_code == 404:
        raise FileNotFoundError(f"{file_path!r} not found")
    raise Exception(f"{response.status_code} received while accessing {file_path!r}")
