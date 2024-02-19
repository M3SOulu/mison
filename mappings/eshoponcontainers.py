import os


def microservice_mapping(filename):
    if filename is None:
        return None
    return str(filename).split(os.sep)[2]