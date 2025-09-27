# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.
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
