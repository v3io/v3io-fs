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

from v3iofs import V3ioFS
from v3iofs.fs import parse_time, split_auth
from v3iofs.path import split_container

container = 'bigdata'  # FIXME


def test_ls(fs: V3ioFS):
    path = f'/{container}/miki/'
    out = fs.ls(path)
    assert len(out) > 0, 'nothing found'
    assert all(isinstance(p, dict) for p in out), 'not dict'

    out = fs.ls(path, detail=False)
    assert len(out) > 0, 'nothing found'
    assert all(isinstance(p, str) for p in out), 'not string'


def test_rm(fs: V3ioFS, tmp_obj):
    path = tmp_obj.path
    fs.rm(path)
    out = fs.ls(dirname(path), detail=False)
    assert path not in out, 'not deleted'


def test_touch(fs: V3ioFS, tmp_obj):
    path = tmp_obj.path
    fs.touch(path)
    container, path = split_container(path)
    resp = fs._client.get_object(container, path)
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


split_auth_cases = [
    (
        'v3io://api_key:s3cr3t@domain.company.com',
        ('v3io://domain.company.com', 's3cr3t'),
        False,
    ),
    (
        'v3io://domain.company.com',
        ('v3io://domain.company.com', ''),
        False,
    ),
    (
        'v3io://s3cr3t@domain.company.com',
        ('v3io://domain.company.com', ''),
        True,
    ),
]


@pytest.mark.parametrize('url, expected, raises', split_auth_cases)
def test_split_auth(url, expected, raises):
    if raises:
        with pytest.raises(ValueError):
            split_auth(url)
        return
    out = split_auth(url)
    assert expected == out
