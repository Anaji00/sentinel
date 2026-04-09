# Run everything: honcho start
# One service:   honcho start ais
# Phase 1 only:  honcho start ais adsb news enrichment correlation

ais:          python -m services.collector-ais.main
adsb:         python -m services.collector-adsb.main
news:         python -m services.collector-news.main
enrichment:   python -m services.enrichment.main
correlation:  python -m services.correlation.main
alerts:       python -m services.alert-manager.main
reasoning:    python -m services.reasoning.main
api:          uvicorn services.api.main:app --host 0.0.0.0 --port 8000
