# tests/conftest.py
import os
import sys

# Only apply these workarounds if not in a CI environment, as the CI
# environment uses a service container and does not need testcontainers.
if not os.environ.get("CI"):
    if sys.platform == "darwin":
        # On macOS, testcontainers might have trouble finding the Docker socket.
        # This explicitly sets the host to the default socket path.
        os.environ["TC_HOST"] = "unix:///var/run/docker.sock"

    elif sys.platform == "win32":
        # On Windows, testcontainers might need the named pipe specified.
        os.environ["TC_HOST"] = "npipe:////./pipe/docker_engine"
