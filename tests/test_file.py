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

from v3iofs import V3ioFile, V3ioFS


def test_fetch_range(fs: V3ioFS, tmp_obj):
    v3f = V3ioFile(fs, tmp_obj.path)
    start, end = 3, len(tmp_obj.data) - 3
    data = v3f._fetch_range(start, end)
    expected = tmp_obj.data[start:end]
    assert expected == data, 'bad data'


def test_upload_chunk(fs: V3ioFS, tmp_obj):
    v3f = V3ioFile(fs, tmp_obj.path, 'ab')
    chunk = b'::chunk of data'
    v3f.buffer.write(chunk)
    v3f._upload_chunk()

    expected = tmp_obj.data + chunk

    with fs.open(tmp_obj.path, 'rb') as fp:
        data = fp.read()

    assert expected == data, 'bad data'


def test_initiate_upload(fs: V3ioFS, tmp_obj):
    fs.touch(tmp_obj.path)
    assert fs.exists(tmp_obj.path)
    v3f = V3ioFile(fs, tmp_obj.path, 'wb')
    v3f._initiate_upload()
    assert not fs.exists(tmp_obj.path)
    # should not fail even if the file does not exist
    v3f._initiate_upload()
