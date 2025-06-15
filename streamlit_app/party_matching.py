# -*- coding: utf-8 -*-
"""
Enhanced party matching functionality for VotoTransparente.
Handles complex party name patterns including:
- Space-separated multiple parties
- Full party names vs acronyms
- Alternative names
- Special characters and accents
- Boundary matching
"""

import re
import unicodedata
import pandas as pd
import json
from typing import List, Set

# Define comprehensive party mappings
PARTY_MAPPINGS = {
    # Main target parties (acronyms)
    'PS': ['PS', 'Partido Socialista'],
    'PSD': ['PSD', 'Partido Social Democrata'],
    'CH': ['CH', 'CHEGA', 'Chega', 'André Ventura', 'Andre Ventura'],
    'IL': ['IL', 'Iniciativa Liberal'],
    'PCP': ['PCP', 'Partido Comunista Português'],
    'BE': ['BE', 'Bloco de Esquerda'],
    'PAN': ['PAN', 'Pessoas Animais Natureza'],
    'L': ['L', 'LIVRE', 'Livre'],
    'CDS-PP': ['CDS-PP', 'CDS', 'Centro Democrático Social'],
    'PEV': ['PEV', 'Os Verdes', 'Partido Ecologista Os Verdes', 'Verdes']
}

def normalize_text(text: str) -> str:
    """
    Normalize text by removing accents, converting to lowercase, and removing special characters.
    """
    if not text:
        return ""
    
    # Remove accents
    nfkd_form = unicodedata.normalize('NFKD', str(text))
    text_without_accents = "".join([c for c in nfkd_form if not unicodedata.combining(c)])
    
    # Convert to lowercase and remove special characters (keep alphanumeric, spaces, and common separators)
    text_normalized = re.sub(r'[^a-zA-Z0-9\s/\-_]', '', text_without_accents).lower()
    
    return text_normalized.strip()

# Create reverse lookup for all variations
PARTY_NAME_TO_ACRONYM = {}
for acronym, variations in PARTY_MAPPINGS.items():
    for variation in variations:
        PARTY_NAME_TO_ACRONYM[normalize_text(variation)] = acronym

def extract_parties_from_text(text: str) -> Set[str]:
    """
    Extract party acronyms from a text string that may contain multiple parties.
    Handles space-separated parties, full names, and alternative names.
    """
    if not text or pd.isna(text):
        return set()
    
    matched_parties = set()
    normalized_text = normalize_text(text)
    
    # First, try to match full party names and alternative names in the original text
    for acronym, variations in PARTY_MAPPINGS.items():
        for variation in variations:
            normalized_variation = normalize_text(variation)
            # Use word boundary matching
            if re.search(r'\b' + re.escape(normalized_variation) + r'\b', normalized_text):
                matched_parties.add(acronym)
    
    # Then split by common separators and check individual parts
    # Handle cases like "PSD CDS-PP", "PS PSD CDS-PP Chega", "GP/PAN GP/PS"
    separators = [' ', '/', '-', '_']
    parts = [normalized_text]
    
    for sep in separators:
        new_parts = []
        for part in parts:
            new_parts.extend(part.split(sep))
        parts = new_parts
    
    # Clean up parts - remove empty strings and common non-party words
    parts = [p.strip() for p in parts if p.strip()]
    non_party_words = {'grupo', 'parlamentar', 'gp', 'deputado', 'deputada', 'deputados', 
                       'deputadas', 'comissao', 'assembleia', 'republica', 'governo',
                       'presidente', 'nao', 'inscrita', 'inscrito', 'signatarios'}
    
    # Try to match each part individually
    for part in parts:
        part = part.strip()
        if not part or part in non_party_words:
            continue
            
        # Direct lookup in our mapping
        if part in PARTY_NAME_TO_ACRONYM:
            matched_parties.add(PARTY_NAME_TO_ACRONYM[part])
            continue
        
        # Try partial matching for longer names
        for normalized_name, acronym in PARTY_NAME_TO_ACRONYM.items():
            # Use word boundary matching to avoid PS matching in PSD
            if re.search(r'\b' + re.escape(normalized_name) + r'\b', part):
                matched_parties.add(acronym)
                break
    
    return matched_parties

def parse_proposing_party_list(proposing_party_val) -> List[str]:
    """
    Enhanced parsing function that handles all the complex cases.
    """
    
    if pd.isna(proposing_party_val) or str(proposing_party_val).lower() in ['nan', '', 'none', 'n/a']:
        return []
    
    try:
        # Convert to string first
        proposing_party_str = str(proposing_party_val)
        
        # Check if it's a JSON array string
        if proposing_party_str.startswith('[') and proposing_party_str.endswith(']'):
            party_list = json.loads(proposing_party_str.replace("'", '"'))
            if isinstance(party_list, list):
                # Process each item in the JSON list
                all_parties = set()
                for party_item in party_list:
                    if str(party_item).strip():
                        parties_found = extract_parties_from_text(str(party_item))
                        all_parties.update(parties_found)
                return sorted(list(all_parties))
        
        # For non-JSON strings, extract parties directly
        parties_found = extract_parties_from_text(proposing_party_str)
        return sorted(list(parties_found))
        
    except (json.JSONDecodeError, ValueError):
        # Fallback: treat as single string and extract parties
        parties_found = extract_parties_from_text(str(proposing_party_val))
        return sorted(list(parties_found))

def test_party_matching():
    """
    Test function to validate the party matching logic.
    """
    test_cases = [
        ("PSD CDS-PP", {"PSD", "CDS-PP"}),
        ("PS PSD CDS-PP Chega", {"PS", "PSD", "CDS-PP", "CH"}),
        ("PSD IL PS CH", {"PSD", "IL", "PS", "CH"}),
        ("Partido Socialista", {"PS"}),
        ("Bloco de Esquerda", {"BE"}),
        ("André Ventura", {"CH"}),
        ("CHEGA", {"CH"}),
        ("Iniciativa Liberal", {"IL"}),
        ("PSD PS CHEGA IL", {"PSD", "PS", "CH", "IL"}),
        ("GP/PAN GP/PS", {"PAN", "PS"}),
        ("André Ventura Grupo Parlamentar do CDS-PP Grupo Parlamentar do PSD", {"CH", "CDS-PP", "PSD"}),
        ("Os Verdes", {"PEV"}),
        ("Partido Ecologista Os Verdes", {"PEV"}),
    ]
    
    print("Testing party matching...")
    for test_input, expected in test_cases:
        result = extract_parties_from_text(test_input)
        status = "✓" if result == expected else "✗"
        print(f"{status} '{test_input}' -> {result} (expected: {expected})")
    
    print("\nTesting parse_proposing_party_list...")
    for test_input, expected in test_cases:
        result = set(parse_proposing_party_list(test_input))
        status = "✓" if result == expected else "✗"
        print(f"{status} '{test_input}' -> {result} (expected: {expected})")

if __name__ == "__main__":
    test_party_matching()
