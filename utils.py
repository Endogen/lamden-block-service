# TODO: Append current PID to file
def create_kill_script(path: str):
    import os

    bang = '#!/bin/bash'
    kill = f'kill -9 {os.getpid()}'

    with open(path, 'w') as f:
        f.truncate(0)
        f.write(f'{bang}\n\n{kill}')

    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2
    os.chmod(path, mode)


# TODO: Still needed?
def unwrap_fixed(value: str) -> str:
    if type(value) is dict and len(value) == 1 and '__fixed__' in value:
        return value['__fixed__']
    else:
        return value
