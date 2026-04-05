import subprocess
import sys


def test_run_emergent_help():
    result = subprocess.run(
        [sys.executable, "-m", "tools.xraybench.cli", "run-emergent", "--help"],
        capture_output=True, text=True,
        cwd="/Users/sendlane/github_projects/xraygraph-bench",
    )
    assert result.returncode == 0
    assert "learning-curve" in result.stdout
    assert "invalidation" in result.stdout
    assert "--engine" in result.stdout
    assert "--query" in result.stdout
    assert "--mutation" in result.stdout
