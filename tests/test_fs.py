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

from v3iofs import V3ioFS
from v3iofs.fs import split_path
from os.path import dirname

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
    fs.rm(tmp_obj)
    out = fs.ls(dirname(tmp_obj), detail=False)
    assert tmp_obj not in out, 'not deleted'


def test_touch(fs: V3ioFS, tmp_obj):
    fs.touch(tmp_obj)
    container, path = split_path(tmp_obj)
    resp = fs._client.get_object(container, path)
    assert resp.body == b'', 'not truncated'
