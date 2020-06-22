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


import logging

from datetime import datetime, timezone
from http import HTTPStatus
from os import environ
from urllib.parse import urlparse

from fsspec.spec import AbstractFileSystem
from v3io.dataplane import Client

from .path import split_container, unslash
from .file import V3ioFile

logger = logging.getLogger(__name__)


class V3ioFS(AbstractFileSystem):
    """File system driver to v3io

    Parameters
    ----------
    v3io_api: str
        API host name (or V3IO_API environment)
    v3io_access_key: str
        v3io access key (or V3IO_ACCESS_KEY from environment)
    **kw:
        Passed to fsspec.AbstractFileSystem
    """

    protocol = "v3io"

    def __init__(self, v3io_api=None, v3io_access_key=None, **kw):
        # TODO: Support storage options for creds (in kw)
        super().__init__(**kw)
        self._v3io_api = v3io_api or environ.get("V3IO_API")
        self._v3io_access_key = v3io_access_key or environ.get(
            "V3IO_ACCESS_KEY"
            )

        self._client = Client(
            endpoint=self._v3io_api,
            access_key=self._v3io_access_key,
            transport_kind="requests",
        )

    def _details(self, contents, **kwargs):
        pathlist = []
        for c in contents:
            data = {}
            data["name"] = c["name"].lstrip("/")
            if c["size"] is not None:
                data["type"] = "file"
                data["size"] = c["size"]
            else:
                data["type"] = "directory"
                data["size"] = 0
            pathlist.append(data)
        return pathlist

    def ls(self, path, detail=True, **kwargs):
        """Lists files & directories under path"""

        container, path = split_container(path)
        if container == "":
            return self._list_containers(detail)
        else:
            containers = self._list_containers(
                detail=True
                )
            containers = [c["name"] for c in containers]
            if container not in containers:
                raise FileNotFoundError("Container not found!!")

        resp = self._client.get_container_contents(
                container=container,
                path=path,
                get_all_attributes=True,
                raise_for_status="never",#[HTTPStatus.OK],
            )

        # Try to fetch a list of directories, else return empty
        if hasattr(resp.output, 'common_prefixes'):
            prefixes = resp.output.common_prefixes
            dirs = [prefix_info(container, p) for p in prefixes]
        else:
            dirs = []

        # Try to fetch a list of files, else return empty
        if hasattr(resp.output, 'contents'):
            objects = resp.output.contents
            files = [object_info(container, o) for o in objects]
        else:
            files = []

        pathlist = dirs + files
#             if not pathlist:
#                 raise FileNotFoundError(f"Nothing found in {path}")

        if not hasattr(resp.output, 'common_prefixes') and not hasattr(resp.output, 'contents'):
            dirname, _, filename = path.rpartition("/")
            resp = self._client.get_container_contents(
                container=container, path=dirname,
            )
            objects = resp.output.contents
            fn = object_info if detail else object_path
            tempfiles = [object_info(container, o) for o in objects]
            fullpath = f"/{container}/{path}"
            logger.debug(f"fullpath:  {fullpath}")
            for f in tempfiles:
                if f["name"] == fullpath:
                    pathlist.append(f)
            if not pathlist:
                raise FileNotFoundError("File or directory not found!!")
        pathlist = self._details(pathlist)
        if detail:
            return pathlist
        else:
            pathlist = [f["name"] for f in files]
        return pathlist

    def _list_containers(self, detail):
        resp = self._client.get_containers(raise_for_status=[HTTPStatus.OK],)
        fn = container_info if detail else container_path
        return [fn(c) for c in resp.output.containers]

    def copy(self, path1, path2, **kwargs):
        ...  # FIXME

    def _rm(self, path):
        container, path = split_container(path)
        if not container:
            raise ValueError(f"bad path: {path:r}")

        self._client.delete_object(
            container=container, path=path, raise_for_status=[
                HTTPStatus.NO_CONTENT
                ],
        )

    def touch(self, path, truncate=True, **kwargs):
        if not truncate:  # TODO
            raise ValueError("only truncate touch supported")
        container, path = split_container(path)
        self._client.put_object(
            container, path, raise_for_status=[HTTPStatus.OK],
        )

    def info(self, path, **kwargs):
        """Give details of entry at path
        Returns a single dictionary, with exactly the same information as 
        ``ls`` would with ``detail=True``.
        The default implementation should calls ls and could be overridden by a
        shortcut. kwargs are passed on to ```ls()``.
        Some file systems might not be able to measure the file's size, in
        which case, the returned dict will include ``'size': None``.
        Returns
        -------
        dict with keys: name (full path in the FS), size (in bytes), 
        type (file, directory, or something else) and other FS-specific keys.
        """

        out = self.ls(path, detail=True, **kwargs)
        path = path.rstrip("/")
        out1 = [o for o in out if o["name"].rstrip("/") == path]
        if len(out1) == 1:
            if "size" not in out1[0]:
                out1[0]["size"] = None
            return out1[0]
        elif len(out1) > 1 or out:
            return {"name": path, "size": 0, "type": "directory"}
        else:
            raise FileNotFoundError(path)

    def find(self, path, maxdepth=None, withdirs=False, **kwargs):
        """List all files below path.
        Like posix ``find`` command without conditions
        Parameters
        ----------
        path : str
        maxdepth: int or None
            If not None, the maximum number of levels to descend
        withdirs: bool
            Whether to include directory paths in the output. This is True
            when used by glob, but users usually only want files.
        kwargs are passed to ``ls``.
        """
        # TODO: allow equivalent of -name parameter
        path = self._strip_protocol(path)
        out = dict()
        detail = kwargs.pop("detail", False)
        for path, dirs, files in self.walk(path, maxdepth, detail=True, **kwargs):
            if withdirs:
                files.update(dirs)
            out.update({info["name"]: info for name, info in files.items()})
        if self.isfile(path) and path not in out:
            # walk works on directories, but find should also return [path]
            # when path happens to be a file
            out[path] = {}
        names = sorted(out)
        if not detail:
            return names
        else:
            return {name: out[name] for name in names}

    def _open(
        self,
        path,
        mode="rb",
        block_size=None,
        autocommit=True,
        cache_options=None,
        **kw,
    ):
        return V3ioFile(
            fs=self,
            path=path,
            mode=mode,
            block_size=block_size,
            autocommit=autocommit,
            cache_options=cache_options,
            **kw,
        )


def container_path(container):
    return f"/{container.name}"


def container_info(container):
    return {
        "name": container.name,
        "size": None,
    }


def prefix_path(container_name, prefix):
    if not isinstance(prefix, str):
        prefix = prefix.prefix.lstrip()
    # prefix already have a leading /
    return unslash(f"/{container_name}/{prefix}")


def prefix_info(container_name, prefix):
    return info_of(container_name, prefix, "prefix")


def object_path(container_name, object):
    # object.key already have a leading /
    return f"/{container_name}{object.key}"


def object_info(container_name, object):
    return info_of(container_name, object, "key")


def info_of(container_name, obj, name_key):
    return {
        "name": prefix_path(container_name, getattr(obj, name_key)),
        "size": getattr(obj, "size", None),
    }


def parse_time(creation_date):
    # '2020-03-26T09:42:57.504000+00:00'
    # '2020-03-26T09:42:57.71Z'
    i = creation_date.rfind("+")  # If not found will be -1, good for Z
    dt = datetime.strptime(creation_date[:i], "%Y-%m-%dT%H:%M:%S.%f")
    dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def split_auth(url):
    """
    >>> split_auth('v3io://api_key:s3cr3t@domain.company.com')
    ('v3io://domain.company.com', 's3cr3t')
    >>> split_auth('v3io://domain.company.com')
    ('v3io://domain.company.com', '')
    """
    u = urlparse(url)
    if "@" not in u.netloc:
        return (url, "")

    auth, netloc = u.netloc.split("@", 1)
    if ":" not in auth:
        raise ValueError("missing : in auth")
    _, key = auth.split(":", 1)
    u = u._replace(netloc=netloc)
    return (u.geturl(), key)
