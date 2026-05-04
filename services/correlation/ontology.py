"""
services/correlation/ontology.py
Agentic Ontology Bridge powered by Local Llama 3 and Redis.
"""
import requests
import json
import logging
from typing import List, Set
from shared.db import get_redis

logger = logging.getLogger("correlation.ontology")

class OntologyBridge:
    def __init__(self):
        self.redis = get_redis()
        self.llm_url = "http://sentinel-ollama:11434/api/generate"

        self.core_concepts = [
            "data_center", "ai", "semiconductor", "middle_east", 
            "taiwan", "ransomware", "infrastructure", "cryptocurrency", "macro_economics",
            "geopolitics", "cybersecurity", "supply_chain", "finance", "energy", "satellites",
            "defense", "software", "iran", "china", "russia", "us_politics", "sanctions",
            "natural_gas", "oil", "renewables", "aviation", "maritime", "space", "emerging_tech",
            "quantum_computing", "5g", "ai_chips", "agentic_agents", "agentic_software"
        ]
        self._llm_cache = set()
    
    def _get_dynamic_bridge(self) -> dict:
            """Pulls the latest AI-generated ontology from Redis."""
            bridge = {concept: set() for concept in self.core_concepts}
            try:
                for concept in self.core_concepts:
                    members = self.redis.smembers(f"sentinel:ontology:{concept}")
                    if members:
                        bridge[concept] = {m.decode('utf-8') for m in members}
            except Exception as e:
                logger.error(f"Redis ontology fetch failed: {e}")
            return bridge
    
    def _ask_agent(self, entity: str):
        """Asks local Llama3 to categorize an unknown entity."""
        if entity in self._llm_cache or not entity.strip():
            return
        self._llm_cache.get(entity, set())
        prompt = f"""
        You are an elite geopolitical and financial intelligence router.
        Categorize the following entity, stock ticker, or prediction market slug: '{entity}'

        map it to these core concepts, choose the 3 that most strongly correlate:
        {self.core_concepts}

        Respond ONLY with a valid JSON list of strings. Do not include markdown formatting or explanations.
        Example output: ["ai", "semiconductor"]
        """
        try:
            response = requests.post(self.llm_url, json={
                "model": "llama3",
                "prompt": prompt, 
                "stream": False, 
                "format": "json"
            }, timeout=10)

            concepts = json.loads(response.json()['response'])

            # Dynamically update the Global Knowledge Graph in Redis
            for concept in concepts:
                self.redis.raw.sadd(f"sentinel:ontology:{concept}", entity.lower())
                logger.info(f"✅ Agentic Map Created: {entity} -> {concept}")

        except Exception as e:
            logger.error(f"LLM categorization failed for '{entity}': {e}")

    def expand_tags(self, tags: List[str]) -> Set[str]:
        " Expands a list of tags into a broader set using the Ontology Bridge."
        expanded = set(t.lower() for t in tags)
        bridge = self._get_dynamic_bridge()
        known_entities = {item for subset in bridge.values() for item in subset}
        for tag in list(expanded):
            if tag not in self.core_concepts and tag not in known_entities:
                import threading
                threading.Thread(target=self._ask_agent, args=(tag,)).start()

            for concept, entities in bridge.items():
                if tag in entities:
                    expanded.add(concept)
            
            if tag in bridge:
                expanded.update(bridge[tag])
        return expanded
        


