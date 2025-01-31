import os


def component_mapping(filename):
    if filename is None:
        return None

    try:
        paths = str(filename).split(os.sep)
        if paths[1] == 'Services' or paths[1] == 'Microservices':
            return paths[2]
        return None
    except IndexError:
        return None