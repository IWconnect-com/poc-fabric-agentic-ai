"""Static checks that keep the security rules true as the code grows."""
import re

from conftest import SRC

PY_FILES = sorted(SRC.rglob("*.py"))


def test_only_secrets_module_reads_environment():
    access = re.compile(r"\bos\.environ\b|\bgetenv\b|import\s+environ\b|\bos\.putenv\b")
    offenders = [f.name for f in PY_FILES if f.name != "secrets.py" and access.search(f.read_text())]
    assert offenders == []


def test_the_only_env_var_is_the_key_vault_url():
    text = (SRC / "secrets.py").read_text()
    reads = re.findall(r"environ\.get\(([^)]*)\)|environ\[([^\]]*)\]|getenv\(([^)]*)\)", text)
    args = {next(a for a in group if a) for group in reads}
    assert args == {"KEYVAULT_URL_ENV"}
    assert 'KEYVAULT_URL_ENV = "AZURE_KEYVAULT_URL"' in text


def test_no_urls_hardcoded_in_code():
    pattern = re.compile(r"""["']https?://""")
    offenders = [f.name for f in PY_FILES if pattern.search(f.read_text())]
    assert offenders == []


def test_no_dotenv_usage():
    assert [f.name for f in PY_FILES if "dotenv" in f.read_text()] == []
