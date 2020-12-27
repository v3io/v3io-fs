def handle_v3io_errors(response, file_path):
    if response.status_code == 200:
        return response.body
    if response.status_code in {404, 409}:
        raise FileNotFoundError(f'{file_path!r} not found')
    raise Exception(f'{response.status_code} received while accessing {file_path!r}')
