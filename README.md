# v3iofs

[![CI](https://github.com/353solutions/v3iofs/workflows/CI/badge.svg)](https://github.com/353solutions/v3iofs/actions?query=workflow%3ACI)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)


An [fsspec][fsspec] driver for [v3io][v3io].

**THIS IS ALPHA QUALITY WORK IN PROGRESS, DO NOT USE**

## Example

```python
>>> from v3iofs import V3ioFS
>>> fs = V3ioFS('api.app.yh48.iguazio-cd2.com', v3io_access_key='s3cr3t')
>>> fs.ls('/container/path')
```

## Dask Example

```python
>>> from v3iofs import V3ioFS
>>> from dask import bag
>>> url = 'v3io://api.app.yh48.iguazio-cd2.com/container/path'

# Use V3IO_ACCESS_KEY from environment
>>> file = bag.read_text(url)
>>> data, _ = file.compute()

# Pass key in storage_options
>>> file = bag.read_text(url, storage_options={'v3io_access_key': 's3cr3t'})
>>> data, _ = file.compute()

# Pass key in URL
>>> url = 'v3io://api_key:s3cr3t@api.app.yh48.iguazio-cd2.com/container/path'
>>> file = bag.read_text(url)
>>> data, _ = file.compute()
```


[fsspec]: (https://filesystem-spec.readthedocs.io)
[v3io]: https://www.iguazio.com/docs/tutorials/latest-release/getting-started/containers/
