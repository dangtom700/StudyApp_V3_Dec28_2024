import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
BUILD_DIR = REPO_ROOT / "build"
BINARY = BUILD_DIR / "retrieval_engine.exe"
UCRT64_BIN = r"C:\msys64\ucrt64\bin"


@pytest.fixture(scope="session")
def retrieval_engine_binary():
    """Configures and builds the C++ engine once per test session.

    Always reconfigures+rebuilds (cheap for a project this size, via
    incremental Make) so tests never run against a stale binary."""
    subprocess.run(
        ["cmake", "-B", str(BUILD_DIR), "-DCMAKE_PREFIX_PATH=C:/msys64/ucrt64",
         "-G", "MinGW Makefiles", str(REPO_ROOT)],
        check=True, capture_output=True, text=True,
    )
    subprocess.run(["cmake", "--build", str(BUILD_DIR)], check=True, capture_output=True, text=True)
    assert BINARY.exists(), f"Build did not produce {BINARY}"
    return BINARY


def run_engine(binary, args, **kwargs):
    """Runs retrieval_engine.exe with C:\\msys64\\ucrt64\\bin prepended to
    PATH -- without this, the binary fails with exit code 3221225785
    (STATUS_DLL_NOT_FOUND) because this machine's ambient PATH resolves
    libstdc++-6.dll from an incompatible /mingw64/bin install first."""
    env = dict(os.environ)
    env["PATH"] = UCRT64_BIN + os.pathsep + env.get("PATH", "")
    return subprocess.run([str(binary)] + args, capture_output=True, text=True, env=env, **kwargs)
