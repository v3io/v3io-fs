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
from os.path import basename, dirname
from pathlib import Path

import fsspec
import pytest
from conftest import test_container, test_dir

from v3iofs import V3ioFS
from v3iofs.fs import parse_time
from v3iofs.path import split_container

path_types = [
    str,
    Path,
]


@pytest.mark.parametrize('path_cls', path_types)
def test_ls(fs: V3ioFS, new_file, path_cls):
    new_file(fs._client, f'{test_dir}/test-file')  # Make sure dir exists
    path = path_cls(f'/{test_container}/{test_dir}/')
    out = fs.ls(path)
    assert len(out) > 0, 'nothing found'
    assert all(isinstance(p, dict) for p in out), 'not dict'

    out = fs.ls(path, detail=False)
    assert len(out) > 0, 'nothing found'
    assert all(isinstance(p, str) for p in out), 'not string'


def test_ls_with_marker(fs: V3ioFS, new_file):
    for i in range(1200):
        new_file(fs._client, f'{test_dir}/test_ls/test-file{i}')
    path = str(f'/{test_container}/{test_dir}/test_ls')

    out = fs.ls(path, detail=True)
    assert len(out) == 1200, 'not all files returned'


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


def load_tree(fs, root):
    out = {}
    for entry in fs.ls(root, detail=True):
        name = basename(entry['name'])
        if entry['type'] == 'file':
            with fs.open(entry['name'], 'rb') as fp:
                out[name] = fp.read()
        elif entry['type'] == 'directory':
            out[name] = load_tree(fs, f'{root}/{name}')
        else:
            raise TypeError(f'unknown entry type: {entry["type"]}')
    return out


def test_directory(fs, tree):
    out = load_tree(fs, tree.root)
    assert tree.data == out


def test_fsspec():
    fs = fsspec.filesystem("v3io")
    dirpath = f'/{test_container}/{test_dir}/fss'
    file_name = datetime.now().strftime('test_%f')
    filepath = f'{dirpath}/{file_name}.txt'
    with fs.open(filepath, 'wb') as fp:
        fp.write(b'123')
    for prefix in ['', 'v3io://']:
        files = fs.ls(prefix + dirpath, detail=True)
        assert len(files) == 1, \
            f'unexpected number of files {len(files)} in {prefix + dirpath}'
        assert fs.info(prefix + filepath)['type'] == 'file', \
            f'unexpected type in {prefix + dirpath}'
        assert fs.size(prefix + filepath) == 3, \
            f"unexpected file size in {prefix + dirpath}"
    with fs.open(prefix + filepath) as fp:
        data = fp.read()
    assert data == b'123', 'unexpected data'
    print(filepath)
    fs.rm(filepath)
