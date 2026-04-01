import os
import tempfile
import shutil
import subprocess
import sys


class TestCLIGenerate:
    def setup_method(self):
        self.data_dir = tempfile.mkdtemp(prefix="xraybench_cli_test_")

    def teardown_method(self):
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_generate_chain(self):
        result = subprocess.run(
            [sys.executable, "-m", "tools.xraybench.cli",
             "generate", "--generator", "chain",
             "--param", "length=50", "--param", "seed=42",
             "--name", "cli-test-chain", "--data-dir", self.data_dir],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        dataset_dir = os.path.join(self.data_dir, "synthetic", "cli-test-chain")
        assert os.path.isfile(os.path.join(dataset_dir, "manifest.yaml"))
        assert os.path.isfile(os.path.join(dataset_dir, "edges.bin"))

    def test_generate_deep_traversal(self):
        result = subprocess.run(
            [sys.executable, "-m", "tools.xraybench.cli",
             "generate", "--generator", "deep_traversal",
             "--param", "num_roots=1", "--param", "fanout_per_level=[5,3]",
             "--param", "seed=42", "--name", "cli-test-deep",
             "--data-dir", self.data_dir],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

    def test_verify_valid(self):
        subprocess.run(
            [sys.executable, "-m", "tools.xraybench.cli",
             "generate", "--generator", "chain",
             "--param", "length=20", "--param", "seed=42",
             "--name", "cli-verify", "--data-dir", self.data_dir],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        result = subprocess.run(
            [sys.executable, "-m", "tools.xraybench.cli",
             "verify-dataset", "--type", "synthetic",
             "--name", "cli-verify", "--data-dir", self.data_dir],
            capture_output=True, text=True,
            cwd="/Users/sendlane/github_projects/xraygraph-bench",
        )
        assert result.returncode == 0
