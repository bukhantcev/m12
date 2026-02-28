import re
from dataclasses import dataclass
from typing import Optional

import httpx

API = "https://cloud-api.yandex.net/v1/disk"


def sanitize_name(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"[\\/:\*\?\"<>\|]", "_", s)
    s = re.sub(r"\s+", " ", s)
    return (s[:120] if s else "Без_названия").strip()


def month_prefix_from_ddmmyyyy(date_str: str) -> str:
    # DD.MM.YYYY -> "02"
    try:
        return date_str.split(".")[1]
    except Exception:
        return "00"


@dataclass
class YDisk:
    token: str

    def _headers(self) -> dict:
        return {"Authorization": f"OAuth {self.token}"}

    async def ensure_folder(self, path: str) -> None:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.put(f"{API}/resources", headers=self._headers(), params={"path": path})
            if r.status_code in (201, 409):
                return
            r.raise_for_status()

    async def upload_bytes(self, disk_path: str, data: bytes, overwrite: bool = True) -> None:
        async with httpx.AsyncClient(timeout=120) as client:
            r = await client.get(
                f"{API}/resources/upload",
                headers=self._headers(),
                params={"path": disk_path, "overwrite": str(overwrite).lower()},
            )
            r.raise_for_status()
            href = r.json()["href"]
            up = await client.put(href, content=data)
            up.raise_for_status()

    async def delete(self, path: str, permanently: bool = False) -> None:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.delete(
                f"{API}/resources",
                headers=self._headers(),
                params={"path": path, "permanently": str(permanently).lower()},
            )
            if r.status_code in (202, 204, 404):
                return
            r.raise_for_status()

    async def publish(self, path: str) -> Optional[str]:
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.put(f"{API}/resources/publish", headers=self._headers(), params={"path": path})
            if r.status_code not in (200, 201, 409):
                r.raise_for_status()
            meta = await client.get(f"{API}/resources", headers=self._headers(), params={"path": path})
            meta.raise_for_status()
            return meta.json().get("public_url")

    async def list_files(self, path: str, limit: int = 50) -> list[dict]:
        """List only files in a folder. Returns [{'name': str, 'path': str}]."""
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(
                f"{API}/resources",
                headers=self._headers(),
                params={"path": path, "limit": str(limit)},
            )
            if r.status_code == 404:
                return []
            r.raise_for_status()
            data = r.json()

        items = (data.get("_embedded") or {}).get("items") or []
        out: list[dict] = []
        for it in items:
            if it.get("type") == "file":
                out.append({"name": it.get("name") or "", "path": it.get("path") or ""})
        return out

    async def get_download_url(self, path: str) -> str:
        """Return temporary direct download URL (href) for a file."""
        async with httpx.AsyncClient(timeout=60) as client:
            r = await client.get(
                f"{API}/resources/download",
                headers=self._headers(),
                params={"path": path},
            )
            r.raise_for_status()
            data = r.json()
        return data.get("href") or ""