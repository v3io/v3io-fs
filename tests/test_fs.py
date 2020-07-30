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

from datetime import datetime, timezone
from os.path import dirname

import pytest

from conftest import test_container, test_dir
from v3iofs import V3ioFS
from v3iofs.fs import parse_time
from v3iofs.path import split_container


def test_ls(fs: V3ioFS, new_file):
    # Also need to modify directory names to end in "/"
    new_file.create_file(fs._client, f'{test_dir}/test-file')
    new_file.create_file(fs._client, f'{test_dir}/a/file.txt')
    new_file.create_file(fs._client, f'{test_dir}/a/file2.txt')

    path = f'/{test_container}/{test_dir}/'
    out0 = fs.ls(path)
    assert len(out0) > 0, 'nothing found'

    out1 = fs.ls(path, detail=False)
    assert len(out1) > 0, 'nothing found'
    assert all(isinstance(p, str) for p in out1), 'not string'
    assert set(out1) == set(
        ['/bigdata/v3io-fs-test/test-file', '/bigdata/v3io-fs-test/a']
    )

    out2 = fs.ls('bigdata/v3io-fs-test/a', detail=False)
    assert set(out2) == set(
        ['/bigdata/v3io-fs-test/a/file.txt',
         '/bigdata/v3io-fs-test/a/file2.txt'
         ]
    )

    path = f'/{test_container}/{test_dir}/test-file'
    out3 = fs.ls(path, detail=True)
    assert len(out3) > 0, 'nothing found'
    assert out3 == [
        {'name': '/bigdata/v3io-fs-test/test-file',
         'size': 26, 'type': 'file'}
    ]

    out4 = fs.ls('bigdata/v3io-fs-test/a', detail=True)
    assert out4 == [
        {'name': '/bigdata/v3io-fs-test/a/file.txt',
         'size': 26, 'type': 'file'},
        {'name': '/bigdata/v3io-fs-test/a/file2.txt',
         'size': 26, 'type': 'file'},
    ]

    out5 = fs.ls('/bigdata/v3io-fs-test', detail=True)
    assert len(out5) > 0, 'nothing found'
    assert out5 == [
        {'name': '/bigdata/v3io-fs-test/a',
         'size': 0, 'type': 'directory'
         },
        {'name': '/bigdata/v3io-fs-test/test-file',
         'size': 26, 'type': 'file'
         },
    ]


def test_glob(fs: V3ioFS, new_file):
    new_file.create_file(fs._client, f'{test_dir}/test-file')
    new_file.create_file(fs._client, f'{test_dir}/a/file.txt')
    new_file.create_file(fs._client, f'{test_dir}/a/file2.txt')
    assert fs.glob("bigdata/v3io-fs-test") == ["/bigdata/v3io-fs-test"]
    assert fs.glob("bigdata/v3io-fs-test/") == [
        "/bigdata/v3io-fs-test/a",
        "/bigdata/v3io-fs-test/test-file",
    ]
    assert fs.glob("bigdata/v3io-fs-test/*") == [
        "/bigdata/v3io-fs-test/a",
        "/bigdata/v3io-fs-test/test-file",
    ]


def test_rm(fs: V3ioFS, tmp_obj):
    path = tmp_obj.path
    fs.rm(path)
    out = fs.ls(dirname(path), detail=False)
    assert path not in out, 'not deleted'


def test_touch(fs: V3ioFS, tmp_obj):
    path = tmp_obj.path
    fs.touch(path)
    container, path = split_container(path)
    resp = fs._client.get_object(test_container, path)
    assert resp.body == b'', 'not truncated'


now = datetime(2020, 1, 2, 3, 4, 5, 6789, tzinfo=timezone.utc)
parse_time_cases = [
    # value, expected, raises
    ('', None, True),
    (now.strftime('%Y-%m-%d'), None, True),
    (now.strftime('%Y-%m-%dT%H:%M:%S.%f%z'), now.timestamp(), False),
    (now.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), now.timestamp(), False),
]


@pytest.mark.parametrize('value, expected, raises', parse_time_cases)
def test_parse_time(value, expected, raises):
    if raises:
        with pytest.raises(ValueError):
            parse_time(value)
        return

    out = parse_time(value)
    assert expected == out
