from conftest import run_engine


def test_binary_builds_and_shows_usage_on_no_args(retrieval_engine_binary):
    result = run_engine(retrieval_engine_binary, [])
    assert result.returncode == 1
    assert "usage" in result.stderr


def test_unknown_subcommand_errors_cleanly(retrieval_engine_binary):
    result = run_engine(retrieval_engine_binary, ["--bogus"])
    assert result.returncode == 1
    assert "unknown subcommand" in result.stderr
