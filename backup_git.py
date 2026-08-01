"""
backup_git.py

Backup automático dos JSONs do disco persistente (clientes_gerenciados,
clientes_autoatendimento, sessoes) pra um repositório no GitHub — uma
segurança extra além do snapshot diário que a própria Render já faz do
disco, guardando uma cópia legível fora da Render também.

Não usa o binário `git` (não precisa estar instalado no container) — sobe
os arquivos direto pela API de conteúdo do GitHub, com `requests`.

Configuração (variáveis de ambiente):
    BACKUP_GIT_REPO_URL   -> "usuario/repo" ou a URL completa do GitHub
                              (https://github.com/usuario/repo)
    BACKUP_GIT_TOKEN      -> Personal Access Token do GitHub, com
                              permissão de escrita no repo (classic token
                              com escopo "repo", ou fine-grained com
                              "Contents: Read and write")
    BACKUP_GIT_BRANCH     -> branch de destino (padrão: "main")
    BACKUP_INTERVALO_HORAS -> de quanto em quanto tempo repete (padrão: 24)

Se BACKUP_GIT_REPO_URL ou BACKUP_GIT_TOKEN não estiverem configuradas, o
backup fica desativado — a thread só loga um aviso uma vez e não faz mais
nada (não trava o servidor por causa disso).
"""

import os
import re
import time
import base64
import requests

from pathlib import Path

DISCO_PATH = Path(os.environ.get("DISCO_PATH", "."))

ARQUIVOS_PARA_BACKUP = [
    "clientes_gerenciados.json",
    "clientes_autoatendimento.json",
    "sessoes.json",
]

BACKUP_GIT_REPO_URL = os.environ.get("BACKUP_GIT_REPO_URL", "")
BACKUP_GIT_TOKEN = os.environ.get("BACKUP_GIT_TOKEN", "")
BACKUP_GIT_BRANCH = os.environ.get("BACKUP_GIT_BRANCH", "main")
INTERVALO_SEGUNDOS = float(os.environ.get("BACKUP_INTERVALO_HORAS", "24")) * 3600

GITHUB_API = "https://api.github.com"


def _extrair_owner_repo(valor: str) -> tuple[str, str] | None:
    """Aceita tanto 'usuario/repo' quanto a URL completa do GitHub."""
    valor = valor.strip().rstrip("/")
    if valor.startswith("http://") or valor.startswith("https://"):
        m = re.search(r"github\.com/([^/]+)/([^/]+?)(\.git)?$", valor)
        if not m:
            return None
        return m.group(1), m.group(2)
    partes = valor.split("/")
    if len(partes) == 2 and all(partes):
        return partes[0], partes[1]
    return None


def _headers():
    return {
        "Authorization": f"Bearer {BACKUP_GIT_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def _sha_atual(owner: str, repo: str, caminho_repo: str) -> str | None:
    """Pega o sha do arquivo no GitHub, se ele já existir lá — necessário
    pra fazer update em vez de tentar criar um arquivo que já existe."""
    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{caminho_repo}"
    resp = requests.get(url, headers=_headers(), params={"ref": BACKUP_GIT_BRANCH}, timeout=15)
    if resp.status_code == 200:
        return resp.json().get("sha")
    if resp.status_code == 404:
        return None
    print(f"[backup_git] Aviso: não consegui checar sha de {caminho_repo} (HTTP {resp.status_code}): {resp.text}")
    return None


def _subir_arquivo(owner: str, repo: str, caminho_local: Path, nome_arquivo: str) -> None:
    if not caminho_local.exists():
        return

    conteudo_b64 = base64.b64encode(caminho_local.read_bytes()).decode("utf-8")
    caminho_repo = f"backups/{nome_arquivo}"
    sha = _sha_atual(owner, repo, caminho_repo)

    payload = {
        "message": f"Backup automático — {nome_arquivo} ({time.strftime('%Y-%m-%d %H:%M:%S')})",
        "content": conteudo_b64,
        "branch": BACKUP_GIT_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    url = f"{GITHUB_API}/repos/{owner}/{repo}/contents/{caminho_repo}"
    resp = requests.put(url, headers=_headers(), json=payload, timeout=15)
    if resp.status_code not in (200, 201):
        print(f"[backup_git] Erro ao subir {nome_arquivo} (HTTP {resp.status_code}): {resp.text}")
    else:
        print(f"[backup_git] Backup de {nome_arquivo} enviado com sucesso.")


def fazer_backup() -> None:
    """Sobe pro GitHub cada um dos JSONs do disco persistente que existir
    no momento — se algum ainda não foi criado (ex: sessoes.json antes do
    primeiro login), simplesmente pula ele."""
    if not BACKUP_GIT_REPO_URL or not BACKUP_GIT_TOKEN:
        return

    owner_repo = _extrair_owner_repo(BACKUP_GIT_REPO_URL)
    if not owner_repo:
        print(f"[backup_git] BACKUP_GIT_REPO_URL inválida: '{BACKUP_GIT_REPO_URL}'. "
              "Use 'usuario/repo' ou a URL completa do GitHub.")
        return
    owner, repo = owner_repo

    for nome_arquivo in ARQUIVOS_PARA_BACKUP:
        try:
            _subir_arquivo(owner, repo, DISCO_PATH / nome_arquivo, nome_arquivo)
        except Exception as e:
            print(f"[backup_git] Exceção ao fazer backup de {nome_arquivo}: {e}")


def loop_backup_diario() -> None:
    """Chamado numa thread de fundo pelo servidor.py — roda pra sempre,
    fazendo backup a cada INTERVALO_SEGUNDOS. Se as variáveis de ambiente
    não estiverem configuradas, avisa uma vez só e fica ocioso (não
    derruba a thread, só não faz nada útil)."""
    if not BACKUP_GIT_REPO_URL or not BACKUP_GIT_TOKEN:
        print("[backup_git] Backup automático desativado — configure "
              "BACKUP_GIT_REPO_URL e BACKUP_GIT_TOKEN pra ativar.")
        return

    while True:
        fazer_backup()
        time.sleep(INTERVALO_SEGUNDOS)
