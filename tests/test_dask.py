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


def test_text(tmp_obj):
    uri = f'v3io://{tmp_obj.path}'
    file = db.read_text(uri)
    data, = file.compute(scheduler='single-threaded')
    data = data.encode('utf-8')
    assert tmp_obj.data == data, 'bad data'


read_cases = [
    # read_fn, typ
    (dd.read_csv, 'csv'),
    (dd.read_parquet, 'pq'),
]


@pytest.mark.parametrize('read_fn, typ', read_cases)
def test_read(read_fn, typ, client, new_file):
    assert typ in data_by_type, f'unknown type: {typ!r}'
    data = data_by_type[typ]
    file_name = datetime.now().strftime('test-%Y%m%d%H%M.') + typ
    file_path = f'/{test_dir}/{file_name}'
    new_file(client, file_path, data)

    df = read_fn(f'v3io://{test_container}{file_path}')
    assert 4 == len(df.columns), '# of columns'
    assert 5 == len(df), '# of rows'
