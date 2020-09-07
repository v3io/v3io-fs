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


from datetime import datetime
from pathlib import Path

import dask.bag as db
import dask.dataframe as dd
import pytest
from conftest import test_container, test_dir

csv_data = b'''
name,item,price,quantity
Rick,Vodka,13.2,22
Jerry,Beer,34.2,300
Beth,Lettuce,1.2,13
Summer,M&M,2.2,7
Morty,Twix,1.7,5
'''


tests_root = Path(__file__).absolute().parent
test_pq = tests_root / 'sanchez.pq'
with test_pq.open('rb') as fp:
    parquet_data = fp.read()

data_by_type = {
    'csv': csv_data,
    'pq': parquet_data,
}

def test_dask(tmp_file):
    uri = f'v3io://{tmp_file.path}'
    storage_options = {
        'V3IO_ACCESS_KEY': access_key,
    }
    file = db.read_text(uri, storage_options=storage_options)
    data = file.compute(scheduler='single-threaded')
    data = data[0].encode('utf-8')
    assert tmp_file.data == data, 'bad data'
