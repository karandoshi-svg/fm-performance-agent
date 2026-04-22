#!/usr/bin/env python3
"""
Superset MCP OAuth 2.0 PKCE authentication helper.

First run: opens browser → Okta login → stores access + refresh tokens.
Subsequent runs: uses refresh token silently. No browser needed.
"""

import base64
import hashlib
import json
import os
import secrets
import time
import urllib.parse
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Optional

import requests

SUPERSET_BASE = "https://superset.robinhood.com"
TOKEN_FILE = os.path.expanduser("~/.pfm-superset-tokens.json")
REDIRECT_PORT = 8437
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}/callback"
SCOPES = "openid email profile offline_access"
CLIENT_NAME = "pfm-weekly-agent"


# ── PKCE helpers ──────────────────────────────────────────────────────────────

def _pkce_pair() -> tuple[str, str]:
    verifier = base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()
    digest = hashlib.sha256(verifier.encode()).digest()
    challenge = base64.urlsafe_b64encode(digest).rstrip(b"=").decode()
    return verifier, challenge


# ── Token persistence ─────────────────────────────────────────────────────────

def _load_tokens() -> dict:
    try:
        return json.loads(Path(TOKEN_FILE).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_tokens(data: dict):
    Path(TOKEN_FILE).write_text(json.dumps(data, indent=2))
    os.chmod(TOKEN_FILE, 0o600)


# ── Dynamic client registration ───────────────────────────────────────────────

def _register_client() -> tuple[str, str]:
    """Register a new OAuth client and return (client_id, client_secret)."""
    tokens = _load_tokens()
    if tokens.get("client_id") and tokens.get("client_secret"):
        return tokens["client_id"], tokens["client_secret"]

    resp = requests.post(
        f"{SUPERSET_BASE}/register",
        json={
            "client_name": CLIENT_NAME,
            "redirect_uris": [REDIRECT_URI],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "client_secret_post",
            "scope": SCOPES,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    client_id = data["client_id"]
    client_secret = data.get("client_secret", "")
    tokens.update({"client_id": client_id, "client_secret": client_secret})
    _save_tokens(tokens)
    print(f"  ✓ Registered OAuth client: {client_id}")
    return client_id, client_secret


# ── Authorization code capture ────────────────────────────────────────────────

_auth_code: Optional[str] = None


class _CallbackHandler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_GET(self):
        global _auth_code
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/callback":
            params = urllib.parse.parse_qs(parsed.query)
            _auth_code = params.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<html><body><h2>Authorized!</h2>"
                b"<p>You can close this tab and return to your terminal.</p>"
                b"</body></html>"
            )
        else:
            self.send_response(404)
            self.end_headers()


def _browser_flow(client_id: str) -> str:
    """Run PKCE browser flow and return authorization code."""
    verifier, challenge = _pkce_pair()
    state = secrets.token_urlsafe(16)

    params = urllib.parse.urlencode({
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    })
    url = f"{SUPERSET_BASE}/authorize?{params}"

    print(f"\n  Opening browser for Superset authorization...")
    print(f"  If browser doesn't open, visit:\n  {url}\n")
    webbrowser.open(url)

    server = HTTPServer(("localhost", REDIRECT_PORT), _CallbackHandler)
    print(f"  Waiting for callback on port {REDIRECT_PORT}...")
    server.handle_request()
    server.server_close()

    if not _auth_code:
        raise RuntimeError("No authorization code received from browser callback.")
    return _auth_code, verifier


# ── Token exchange ────────────────────────────────────────────────────────────

def _exchange_code(client_id: str, client_secret: str, code: str, verifier: str) -> dict:
    resp = requests.post(
        f"{SUPERSET_BASE}/token",
        data={
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
            "redirect_uri": REDIRECT_URI,
            "code_verifier": verifier,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


def _refresh_access_token(client_id: str, client_secret: str, refresh_token: str) -> dict:
    resp = requests.post(
        f"{SUPERSET_BASE}/token",
        data={
            "grant_type": "refresh_token",
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        },
        timeout=15,
    )
    resp.raise_for_status()
    return resp.json()


# ── Public API ────────────────────────────────────────────────────────────────

def get_access_token() -> str:
    """
    Return a valid access token for the Superset MCP.
    - Uses stored refresh token if available (no browser).
    - Falls back to browser OAuth flow on first run.
    """
    tokens = _load_tokens()
    client_id, client_secret = _register_client()

    # Refresh silently if we have a refresh token
    if tokens.get("refresh_token"):
        exp = tokens.get("access_token_expires_at", 0)
        if time.time() < exp - 60:
            return tokens["access_token"]
        try:
            print("  ♻️  Refreshing Superset MCP token...")
            result = _refresh_access_token(client_id, client_secret, tokens["refresh_token"])
            tokens["access_token"] = result["access_token"]
            tokens["access_token_expires_at"] = time.time() + result.get("expires_in", 3600)
            if "refresh_token" in result:
                tokens["refresh_token"] = result["refresh_token"]
            _save_tokens(tokens)
            return tokens["access_token"]
        except Exception as e:
            print(f"  ⚠️  Token refresh failed ({e}), re-authenticating via browser...")

    # First-time or refresh failed: browser flow
    code, verifier = _browser_flow(client_id)
    result = _exchange_code(client_id, client_secret, code, verifier)
    tokens["access_token"] = result["access_token"]
    tokens["access_token_expires_at"] = time.time() + result.get("expires_in", 3600)
    tokens["refresh_token"] = result.get("refresh_token", "")
    _save_tokens(tokens)
    print("  ✓ Superset MCP authentication complete.")
    return tokens["access_token"]


if __name__ == "__main__":
    token = get_access_token()
    print(f"\n✅ Token obtained: {token[:40]}...")
