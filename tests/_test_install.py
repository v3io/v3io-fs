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

from subprocess import run
from sys import executable
from tempfile import mkdtemp

import v3iofs


def test_install():
    out = run([executable, 'setup.py', 'sdist'])
    assert out.returncode == 0, 'sdist'

    tmp_dir = mkdtemp(prefix='v3iofs-test')
    out = run([executable, '-m', 'venv', tmp_dir])
    assert out.returncode == 0, 'create venv'

    dist = f'./dist/v3iofs-{v3iofs.__version__}.tar.gz'
    py = f'{tmp_dir}/bin/python'

    out = run([py, '-m', 'pip', 'install', dist])
    assert out.returncode == 0, 'install'

    out = run([py, '-c', 'import v3iofs'])
    assert out.returncode == 0, 'import'
