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

from collections import namedtuple
from datetime import datetime
from getpass import getuser
from os import environ

import pytest

from v3iofs import V3ioFS

host = environ.get('V3IO_API')
access_key = environ.get('V3IO_ACCESS_KEY')
container = 'bigdata'  # TODO: configure


Obj = namedtuple('Obj', 'path data')


@pytest.fixture
def fs():
    fs = V3ioFS(v3io_api=host, v3io_access_key=access_key)
    yield fs
    fs._client.close()


@pytest.fixture
def tmp_obj():
    user, ts = getuser(), datetime.now().isoformat()
    client = V3ioFS(v3io_api=host, v3io_access_key=access_key)._client

    path = f'{user}-test-{ts}'
    body = f'test data for {user} at {ts}'
    client.put_object(container, path, body=body.encode())

    yield Obj(f'/{container}/{path}', body)

    client.delete_object(container, path)
