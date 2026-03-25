from __future__ import annotations

import re
from typing import Iterable, List, Set

import requests


PMID_RE = re.compile(r"/pubmed/(\d{4,})/?", re.IGNORECASE)


def extract_pmids_from_html(html: str) -> List[str]:
    pmids: Set[str] = set(PMID_RE.findall(html))
    return sorted(pmids)


def fetch_mybibliography_pmids(public_url: str, max_pages: int = 10, timeout_s: int = 60) -> List[str]:
    """
    Fetch the user's public MyBibliography page(s) and extract PMIDs from the HTML.

    The public page supports `?page=2` style pagination.
    """
    pmids_all: Set[str] = set()

    session = requests.Session()
    for page in range(1, max_pages + 1):
        url = public_url
        if page > 1:
            sep = "&" if "?" in url else "?"
            url = f"{public_url}{sep}page={page}"

        r = session.get(url, timeout=timeout_s)
        r.raise_for_status()
        pmids = extract_pmids_from_html(r.text)
        if not pmids:
            # Stop when pagination returns no entries.
            break
        pmids_all.update(pmids)

    return sorted(pmids_all)

