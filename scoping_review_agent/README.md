# Scoping Review Agent (local, incremental)

This folder contains a prototype “agent” that incrementally updates a scoping review by:
- ingesting candidate studies from PubMed MyBibliography (public)
- screening them against your research objective
- locating PDFs (DOI/PubMed download when available, otherwise Zotero storage)
- extracting evidence-grounded fields into a CSV dataset + generated Word document

## Files
- `codebook.yaml`: extraction schema and screening rubrics
- `config.example.yaml`: example configuration you copy to `config.yaml`
- `src/`: implementation (ingestion, screening, PDF parsing, extraction, outputs)

## LLM provider options
You can use OpenAI, Gemini, or Claude with the same pipeline:

- `llm.provider: openai` with `api_key_env: OPENAI_API_KEY`
- `llm.provider: gemini` with `api_key_env: GEMINI_API_KEY`
- `llm.provider: anthropic` with `api_key_env: ANTHROPIC_API_KEY`

Supported areas:
- objective -> PubMed query generation
- screening include/exclude/uncertain decisions
- extraction of structured fields from PDF evidence chunks

Ready provider configs are available in `configs/`:
- `configs/config.openai.yaml`
- `configs/config.gemini.yaml`
- `configs/config.claude.yaml`
- `configs/config.ollama.yaml`

## Next steps (you)
1. Create `config.yaml` from `config.example.yaml`
2. Paste your research objective into `screening.objective_text` (used when you run a single objective)
3. Set `pdf_acquisition.zotero_storage_folder`

After that, run the pipeline:
- `python scoping_review_agent/run_pipeline.py --config scoping_review_agent/config.yaml`
- Or from the repo root: `python -m scoping_review_agent.run_pipeline --config scoping_review_agent/config.yaml`

### Multi-objective run
1. Create `objectives.yaml` (see `objectives.example.yaml`)
2. Run:
   - `python scoping_review_agent/run_pipeline.py --config scoping_review_agent/config.yaml --objectives_file scoping_review_agent/objectives.yaml`
   - Or from the repo root: `python -m scoping_review_agent.run_pipeline --config scoping_review_agent/config.yaml --objectives_file scoping_review_agent/objectives.yaml`

Per objective, you can choose:
- `source_mode: pubmed_only`
- `source_mode: local_pdf_only`
- `source_mode: pubmed_plus_local_pdf`

If you provide `local_pdf_folder`, the agent ingests those PDFs directly (NotebookLM-style manual upload workflow), then screens/extracts objective-wise.

## Hybrid Option A (recommended): local Zotero + cloud automation

Because cloud runners cannot read your local Zotero/OneDrive folders directly, use this pattern:

1. **Create a separate GitHub repo** (e.g., `scoping-review-inbox`) and clone it inside OneDrive.
2. Run the local sync script to copy/update PDFs into the repo’s `inbox/` folder and push it.
   - Script: `hybrid/local_sync_to_github.ps1`
3. The cloud workflow runs on a schedule and produces updated outputs (CSV + Word) in `outputs/`.

### GitHub Actions configuration

Set repository secrets:
- `OPENAI_API_KEY`: optional (required only when provider=openai)
- `GEMINI_API_KEY`: optional (required only when provider=gemini)
- `ANTHROPIC_API_KEY`: optional (required only when provider=anthropic)
- `SCOPING_AGENT_CONFIG_YAML`: the full YAML config (paste contents of `config.yaml`)

Then the workflow in `.github/workflows/scoping_review_update.yml` runs daily and also supports manual runs.

## Colab quickstart (same pipeline)
1. Upload or clone this repo in Colab.
2. Install dependencies:
   - `pip install -r scoping_review_agent/requirements.txt`
3. Set one API key in Colab environment:
   - `OPENAI_API_KEY` or `GEMINI_API_KEY` or `ANTHROPIC_API_KEY`
4. Set `llm.provider`, `llm.model`, `llm.api_key_env` in `config.yaml`.
5. Run:
   - `python scoping_review_agent/run_pipeline.py --config scoping_review_agent/config.yaml --objectives_file scoping_review_agent/objectives.yaml`
   - Or from the repo root: `python -m scoping_review_agent.run_pipeline --config scoping_review_agent/config.yaml --objectives_file scoping_review_agent/objectives.yaml`

## No external API key option (Ollama)
You can run fully local with Ollama:
1. Install and run Ollama locally.
2. Pull a model, e.g.:
   - `ollama pull llama3.1:8b-instruct-q4_K_M`
3. Use `configs/config.ollama.yaml`.
4. (Optional) Set custom host:
   - `set OLLAMA_HOST=http://127.0.0.1:11434`

## Human review
The first version generates an editable Word document containing:
- extracted fields + evidence snippets
- a “Human decision” section you can edit

To re-import human edits after you update `extraction_human_review.docx`:

```bash
python -c "from scoping_review_agent.src.human_review.reimport import reimport_from_word; reimport_from_word(word_docx_path='PATH_TO_DOCX', extractions_jsonl_path='PATH_TO_extractions.jsonl', output_dir='PATH_TO_OUTPUT_DIR')"
```

This produces:
- `human_updates.csv`
- `extractions_with_human.csv`

