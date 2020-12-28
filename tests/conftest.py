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

import pytest

import v3io.dataplane
from v3iofs import V3ioFS
from v3iofs.fs import _new_client

test_container = 'bigdata'  # TODO: configure
test_dir = 'v3io-fs-test'


Obj = namedtuple('Obj', 'path data')


@pytest.fixture
def fs():
    fs = V3ioFS()
    yield fs
    fs._client.close()


@pytest.fixture
def tmp_obj():
    user, ts = getuser(), datetime.now().isoformat()
    client = _new_client()

    path = f'{test_dir}/{user}-test-{ts}'
    body = f'test data for {user} at {ts}'.encode()
    resp = client.put_object(test_container, path, body=body)
    assert resp.status_code == HTTPStatus.OK, 'create failed'

    yield Obj(f'/{test_container}/{path}', body)

    client.delete_object(test_container, path)
    client.close()


@pytest.fixture
def new_file():
    _client, _path = None, ''

    def create_file(client, path, body=b''):
        nonlocal _client, _path

        _client, _path = client, path
        body = body or datetime.now().isoformat().encode('utf-8')
        out = client.put_object(test_container, path, body=body)
        out.raise_for_status()

    yield create_file

    _client.delete_object(
        container=test_container,
        path=_path,
        raise_for_status=v3io.dataplane.RaiseForStatus.never,
    )


@pytest.fixture
def client():
    client = _new_client()
    try:
        yield client
    finally:
        client.close()


tree_data = {
    'file1': b'file 1 data',
    'a': {
        'file1': b'file 1 data - a',
        'file2': b'file 2 data',
    },
    'b': {
        'file1': b'file 1 data - b',
    },
}
tree_root = f'/{test_container}/{test_dir}/tree'
Tree = namedtuple('Tree', 'root data')


@pytest.fixture(scope='session')
def tree():
    fs = V3ioFS()
    create_tree(fs, tree_root, tree_data)

    yield Tree(tree_root, tree_data)

    fs.rm(tree_root, recursive=True)
    fs._client.close()


def create_tree(fs, prefix, tree):
    fs.mkdir(prefix)
    for name, value in tree.items():
        path = f'{prefix}/{name}'
        if isinstance(value, bytes):
            with fs.open(path, 'wb') as out:
                out.write(value)
        elif isinstance(value, dict):
            fs.mkdir(path)
            create_tree(fs, path, value)
        else:
            raise TypeError(f'unknown value type: {type(value)!r}')
