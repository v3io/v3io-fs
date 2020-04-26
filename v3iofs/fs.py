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
from http import HTTPStatus

from fsspec.registry import registry
from fsspec.spec import AbstractFileSystem
from v3io.dataplane import Client


class V3ioFS(AbstractFileSystem):
    """File system driver to v3io

    Parameters
    ----------
    v3io_api: str
        API host name
    v3io_access_key: str
        v3io access key (if not will use V3IO_ACCESS_KEY from environment)
    **kw:
        Passed to fsspec.AbstractFileSystem
    """
    def __init__(self, v3io_api, v3io_access_key=None, **kw):
        super().__init__(**kw)
        self._client = Client(
            endpoint=v3io_api,
            access_key=v3io_access_key,
            transport_kind='requests',
        )

    def ls(self, path, detail=True, **kwargs):
        """Lists files & directories under path"""
        container, path = split_path(path)

        if not container:
            return self._list_containers(detail)

        resp = self._client.get_container_contents(
            container=container,
            path=path,
            get_all_attributes=True,
            raise_for_status=[HTTPStatus.OK],
        )

        prefixes = resp.output.common_prefixes  # directories
        fn = prefix_info if detail else prefix_path
        dirs = [fn(container, p) for p in prefixes]

        objects = resp.output.contents  # files
        fn = object_info if detail else object_path
        files = [fn(container, o) for o in objects]

        return dirs + files

    def _list_containers(self, detail):
        resp = self._client.get_containers(
            raise_for_status=[HTTPStatus.OK],
        )
        fn = container_info if detail else container_path
        return [fn(c) for c in resp.output.containers]

    def copy(self, path1, path2, **kwargs):
        ...  # FIXME

    def _rm(self, path):
        container, path = split_path(path)
        if not container:
            raise ValueError(f'bad path: {path:r}')

        self._client.delete_object(
            container=container,
            path=path,
            raise_for_status=[HTTPStatus.NO_CONTENT],
        )

    def touch(self, path, truncate=True, **kwargs):
        if not truncate:  # TODO
            raise ValueError('only truncate touch supported')
        container, path = split_path(path)
        self._client.put_object(
            container, path,
            raise_for_status=[HTTPStatus.OK],
        )


def split_path(path):
    if not path:
        raise ValueError('empty path')

    if path == '/':
        return '', ''

    if path[0] == '/':
        path = path[1:]

    if '/' not in path:
        return path, ''  # container

    return path.split('/', maxsplit=1)


def unslash(s):
    if not s or s[-1] != '/':
        return s
    return s[:-1]


def container_path(container):
    return f'/{container.name}'


def container_info(container):
    return {
        'name': container.name,
        'size': None,
        'created': parse_time(container.creation_date),
    }


def prefix_path(container_name, prefix):
    if not isinstance(prefix, str):
        prefix = prefix.prefix
    # prefix already have a leading /
    return unslash(f'/{container_name}{prefix}')


def prefix_info(container_name, prefix):
    return info_of(container_name, prefix, 'prefix')


def object_path(container_name, object):
    # object.key already have a leading /
    return f'/{container_name}{object.key}'


def object_info(container_name, object):
    return info_of(container_name, object, 'key')


def info_of(container_name, obj, name_key):
    return {
        'name': prefix_path(container_name, getattr(obj, name_key)),
        'size': getattr(obj, 'size', None),
        'created': parse_time(obj.creating_time),
        'mtime': parse_time(obj.last_modified),
        'atime': parse_time(obj.access_time),
        'mode': int(obj.mode[1:], base=8),  # '040755'
        'gid': int(obj.gid, 16),
        'uid': int(obj.uid, 16),
    }


def parse_time(creation_date):
    # '2020-03-26T09:42:57.504000+00:00'
    # '2020-03-26T09:42:57.71Z'
    i = creation_date.rfind('+')  # If not found will be -1, good for Z
    dt = datetime.strptime(creation_date[:i], '%Y-%m-%dT%H:%M:%S.%f')
    return dt.timestamp()


# TODO: Require explicit manual regisration?
if 'v3io' not in registry:
    registry['v3io'] = V3ioFS
