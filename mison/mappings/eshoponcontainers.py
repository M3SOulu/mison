import os


def component_mapping(filename):
    if filename is None:
        return None

    try:
        paths = str(filename).split(os.sep)
        if paths[2] == 'Services' or paths[2] == 'Microservices':
            return paths[3]
        return None
    except IndexError:
        return None