# Lack Lineage — Plan

Ceramics lineage knowledge graph: potters, relationships, documents, places.

## Stack
Python (SQLite, vision_loop.py OCR), vanilla HTML/CSS/JS (12+ pages)

## Current State
- Active, deployed via CNAME
- lineage.db: persons, documents, document_match tables
- Review server for document verification
- OCR pipeline for scanning documents
- Multiple audit/enrichment scripts

## Roadmap
- [ ] Supabase deploy (cloud database)
- [ ] Phase 14 KG enrichment (birth years, orphan prune, d3 viz)
- [ ] Stub re-pass for incomplete profiles
- [ ] Expand potter coverage (international, contemporary)
- [ ] Improve document scan matching accuracy
