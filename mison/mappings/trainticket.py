import os


def microservice_mapping(filename):
    if filename is None:
        return None
    service = str(filename).split(os.sep)[0]
    if service.startswith('ts-') and "service" in service:
        return service
    else:
        return None