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

# Use V3IO_ACCESS_KEY & V3IO_API from environment
>>> url = 'v3io://container/path'
>>> file = bag.read_text(url)
>>> data, _ = file.compute()

# Pass key in storage_options
>>> storage_options={
...     'v3io_api': 'webapi.app.iguazio.com',
...     'v3io_access_key': 's3cr3t',
... }
>>> file = bag.read_text(url, storage_options=storage_options)
>>> data, _ = file.compute()
```

## Development

Run `make test` to run the tests. You need to set `V3IO_ACCESS_KEY` and
`V3IO_API` environment variables first.


[fsspec]: (https://filesystem-spec.readthedocs.io)
[v3io]: https://www.iguazio.com/docs/tutorials/latest-release/getting-started/containers/
