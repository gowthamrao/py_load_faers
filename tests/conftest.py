import sys

if sys.platform == "win32":
    # Workaround for testcontainers on Windows
    # See: https://github.com/testcontainers/testcontainers-python/issues/141
    from testcontainers.core.docker_client import DockerClient

    DockerClient(host="npipe:////./pipe/docker_engine")
