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


import dask.bag as db

from conftest import access_key, host

data = 'In god we trust; all others must bring data.'


def test_dask(tmp_obj):
    uri = f'v3io://{host}/{tmp_obj.path}'
    storage_options = {
        'V3IO_ACCESS_KEY': access_key
    }
    file = db.read_text(uri, storage_options=storage_options)
    data, = file.compute()
    assert tmp_obj.data == data, 'bad data'
