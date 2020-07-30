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
from http import HTTPStatus
from getpass import getuser
from os import environ

import pytest

from v3iofs import V3ioFS

host = environ.get('V3IO_API')
access_key = environ.get('V3IO_ACCESS_KEY')
test_container = 'bigdata'
test_dir = 'v3io-fs-test'


Obj = namedtuple('Obj', 'path data')


@pytest.fixture
def fs():
    fs = V3ioFS(v3io_api=host, v3io_access_key=access_key)
    yield fs
    fs._client.close()


@pytest.fixture(scope="session")
def tmp_obj():
    user, ts = getuser(), datetime.now().isoformat()
    client = V3ioFS(v3io_api=host, v3io_access_key=access_key)._client

    path = f'{test_dir}/{user}-test-{ts}'
    body = f'test data for {user} at {ts}'.encode()
    resp = client.put_object(test_container, path, body=body)
    assert resp.status_code == HTTPStatus.OK, 'create failed'

    yield Obj(f'/{test_container}/{path}', body)

    client.delete_object(test_container, path)


@pytest.fixture()
def new_file():
    _client, _path = None, ''

    def create_file(client, path):
        nonlocal _client, _path

        _client, _path = client, path
        body = datetime.now().isoformat().encode('utf-8')
        _client.put_object(test_container, path, body=body)

    yield create_file

    _client.delete_object(
        container=test_container,
        path=_path,
        raise_for_status=[HTTPStatus.NO_CONTENT],
    )
