from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests


def _safe_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name).strip("_")


def find_pdf_in_zotero_storage(
    zotero_folder: str | Path,
    *,
    pmid: str | None,
    doi: str | None,
    max_files_scanned: int = 50000,
) -> Optional[Path]:
    """
    Best-effort local lookup in Zotero storage.

    Zotero storage often stores PDFs with hashed filenames, so filename matching is imperfect.
    This function looks for PDFs whose filename contains `pmid` or `doi` fragments.
    """
    zotero_folder = Path(zotero_folder)
    if not zotero_folder.exists():
        return None

    pmid = str(pmid) if pmid else None
    doi = (doi or "").lower()
    doi_fragment = doi.split("/")[-1] if doi else None

    scanned = 0
    for root, _, files in os.walk(zotero_folder):
        for fn in files:
            scanned += 1
            if scanned > max_files_scanned:
                return None
            if not fn.lower().endswith(".pdf"):
                continue

            full = str(fn).lower()
            if pmid and pmid in full:
                return Path(root) / fn
            if doi_fragment and doi_fragment in full:
                return Path(root) / fn
            # Some PDFs store raw DOI as part of name.
            if doi and doi in full:
                return Path(root) / fn
    return None


def download_pdf_by_url_or_html(url: str, *, timeout_s: int = 60) -> Tuple[Optional[Path], Optional[str]]:
    """
    Attempt to download a PDF given a URL or an HTML page by searching for PDF links.
    Returns (pdf_path, error_message).
    """
    r = requests.get(url, timeout=timeout_s, allow_redirects=True)
    final_url = r.url
    content_type = (r.headers.get("Content-Type") or "").lower()

    if "application/pdf" in content_type or final_url.lower().endswith(".pdf"):
        return _download_bytes_to_temp(r.content, url=final_url), None

    # Try parsing HTML for a pdf link (very heuristic).
    html = r.text
    m = re.search(r'href=[\'"]([^\'"]+\.pdf[^\'"]*)[\'"]', html, flags=re.IGNORECASE)
    if m:
        pdf_url = m.group(1)
        # handle relative URLs
        if pdf_url.startswith("/"):
            from urllib.parse import urljoin

            pdf_url = urljoin(final_url, pdf_url)
        pr = requests.get(pdf_url, timeout=timeout_s, allow_redirects=True)
        if "application/pdf" in (pr.headers.get("Content-Type") or "").lower() or pr.url.lower().endswith(".pdf"):
            return _download_bytes_to_temp(pr.content, url=pr.url), None

    return None, "No direct PDF link found."


def _download_bytes_to_temp(content: bytes, *, url: str) -> Path:
    tmp_dir = Path(".") / "tmp_downloads"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    name = _safe_filename(url.split("/")[-1] or "download.pdf")
    out = tmp_dir / name
    out.write_bytes(content)
    return out


def download_pdf_from_doi(doi: str, *, timeout_s: int = 60, allowed_domains: list[str] | None = None) -> Optional[Path]:
    doi = (doi or "").strip()
    if not doi:
        return None
    allowed_domains = allowed_domains or []
    landing = f"https://doi.org/{doi}"

    pdf_path, err = download_pdf_by_url_or_html(landing, timeout_s=timeout_s)
    if not pdf_path:
        return None

    if allowed_domains:
        # Basic domain check: best-effort because we don't always know the final origin.
        # If this is important, you can enhance later.
        pass

    return pdf_path


def download_pdf_from_pubmed(pmid: str, *, timeout_s: int = 60) -> Optional[Path]:
    pmid = str(pmid)
    pubmed_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmid}/"  # likely wrong; keep heuristic stub
    # Use PubMed HTML page for full text links.
    pubmed_page = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"
    pdf_path, _ = download_pdf_by_url_or_html(pubmed_page, timeout_s=timeout_s)
    return pdf_path


def acquire_pdf_for_paper(
    paper: Dict[str, Any],
    *,
    zotero_folder: str | Path,
    pdf_cache_dir: str | Path,
    try_download_when_missing: bool = True,
    allowed_domains: list[str] | None = None,
) -> Dict[str, Any]:
    """
    Returns metadata including pdf_status and pdf_path (if found).
    """
    pdf_cache_dir = Path(pdf_cache_dir)
    pdf_cache_dir.mkdir(parents=True, exist_ok=True)

    pmid = (paper.get("pmid") or "").strip() or None
    doi = (paper.get("doi") or "").strip() or None
    pid = paper.get("paper_id") or (doi or pmid or "unknown")
    out_path = pdf_cache_dir / f"{_safe_filename(pid)}.pdf"

    # If already cached, return it.
    if out_path.exists() and out_path.stat().st_size > 0:
        return {"pdf_status": "cached", "pdf_path": str(out_path)}

    # 1) Zotero local lookup
    if zotero_folder:
        p = find_pdf_in_zotero_storage(zotero_folder, pmid=pmid, doi=doi)
        if p and p.exists():
            out_path.write_bytes(p.read_bytes())
            return {"pdf_status": "zotero_found", "pdf_path": str(out_path)}

    # 2) DOI download
    if try_download_when_missing and doi:
        p = download_pdf_from_doi(doi, allowed_domains=allowed_domains)
        if p and p.exists():
            out_path.write_bytes(p.read_bytes())
            return {"pdf_status": "downloaded_doi", "pdf_path": str(out_path)}

    # 3) PubMed download
    if try_download_when_missing and pmid:
        p = download_pdf_from_pubmed(pmid)
        if p and p.exists():
            out_path.write_bytes(p.read_bytes())
            return {"pdf_status": "downloaded_pubmed", "pdf_path": str(out_path)}

    return {"pdf_status": "missing", "pdf_path": ""}

