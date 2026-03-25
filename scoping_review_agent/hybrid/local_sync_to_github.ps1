$ErrorActionPreference = "Stop"

param(
  [Parameter(Mandatory=$true)][string]$ZoteroStorageFolder,
  [Parameter(Mandatory=$true)][string]$InboxRepoFolder,
  [int]$MaxPdfs = 2000
)

# Hybrid model:
# - You maintain a separate GitHub repo (cloned to $InboxRepoFolder) that is ALSO inside OneDrive.
# - This script copies new/changed PDFs from Zotero storage into the repo's `inbox/pdfs/`
#   and writes `inbox/manifest.jsonl` for the cloud job.

function Get-Sha1File($Path) {
  $sha1 = [System.Security.Cryptography.SHA1]::Create()
  $stream = [System.IO.File]::OpenRead($Path)
  try {
    $hash = $sha1.ComputeHash($stream)
    return ($hash | ForEach-Object ToString x2) -join ""
  } finally {
    $stream.Dispose()
    $sha1.Dispose()
  }
}

if (-not (Test-Path $ZoteroStorageFolder)) { throw "Zotero folder not found: $ZoteroStorageFolder" }
if (-not (Test-Path $InboxRepoFolder)) { throw "Inbox repo folder not found: $InboxRepoFolder" }

$inboxDir = Join-Path $InboxRepoFolder "inbox"
$pdfOutDir = Join-Path $inboxDir "pdfs"
New-Item -ItemType Directory -Force -Path $pdfOutDir | Out-Null

$manifestPath = Join-Path $inboxDir "manifest.jsonl"
New-Item -ItemType File -Force -Path $manifestPath | Out-Null

$pdfs = Get-ChildItem -Path $ZoteroStorageFolder -Recurse -File -Filter "*.pdf" | Select-Object -First $MaxPdfs

$rows = @()
foreach ($f in $pdfs) {
  $sha1 = Get-Sha1File $f.FullName
  # We cannot reliably know DOI/PMID from Zotero storage filenames, so we keep file hash + original path.
  # The cloud pipeline will still match by DOI/PMID when it downloads PDFs; these inbox PDFs are a fallback.
  $targetName = "$sha1.pdf"
  $targetPath = Join-Path $pdfOutDir $targetName
  if (-not (Test-Path $targetPath)) {
    Copy-Item -Path $f.FullName -Destination $targetPath
  }
  $rows += (@{
    sha1 = $sha1
    source_path = $f.FullName
    inbox_pdf = ("inbox/pdfs/" + $targetName)
    filename = $f.Name
    size_bytes = $f.Length
    modified_utc = $f.LastWriteTimeUtc.ToString("o")
  } | ConvertTo-Json -Compress)
}

Set-Content -Path $manifestPath -Value ($rows -join "`n") -Encoding UTF8

Write-Host "Wrote manifest: $manifestPath"
Write-Host "Inbox PDFs: $pdfOutDir"
Write-Host "Next: git add/commit/push from $InboxRepoFolder"

