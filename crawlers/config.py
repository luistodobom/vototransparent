import os
from datetime import date
from dotenv import load_dotenv

load_dotenv()
# --- Configuration ---

YEAR = 2021
DATAFRAME_PATH = f"data/parliament_data_{YEAR}.csv"
SESSION_PDF_DIR = "data/session_pdfs"
PROPOSAL_DOC_DIR = "data/proposal_docs"
DOWNLOAD_TIMEOUT = 60  # seconds for requests timeout
LLM_RETRY_ATTEMPTS = 3
LLM_RETRY_DELAY = 5  # seconds
PDF_PAGE_PARTITION_SIZE = 13  # Process PDFs in chunks of this many pages
NUM_THREADS = 1
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

legislature_data = {
    date(2022, 3, 30): {
        "name": "XXIII Legislatura (30/03/2022 - 02/04/2024)",
        "end_date": date(2024, 4, 2),
        "parties": {
            "PS": 120, "PSD": 77, "CH": 12, "IL": 8, "BE": 5,
            "PCP-PEV": 6, "PAN": 1, "L": 1
        },
        "total_mps": 230,
        "notes": "Nota: PCP-PEV representa a coligação entre PCP e PEV."
    },
    date(2024, 4, 2): {
        "name": "XXIV Legislatura (02/04/2024 - 05/06/2025)",
        "end_date": date(2025, 6, 5),
        "parties": {
            "PSD/CDS-PP": 80, "PS": 78, "CH": 50, "IL": 8, "BE": 5,
            "PCP-PEV": 4, "L": 4, "PAN": 1
        },
        "total_mps": 230,
        "notes": "Notas: PSD/CDS-PP representa a coligação Aliança Democrática (AD). PCP-PEV representa a Coligação Democrática Unitária (CDU)."
    },
    date(2025, 6, 5): {
        "name": "XXV Legislatura (05/06/2025 - 01/10/2029)",
        "end_date": date(2029, 10, 1),
        "parties": {
            "PSD/CDS-PP": 91, "PS": 58, "CH": 60, "IL": 9, "L": 6,
            "PCP-PEV": 3, "BE": 1, "PAN": 1, "JPP": 1
        },
        "total_mps": 230,
        "notes": "Notas: PSD/CDS-PP representa a coligação Aliança Democrática (AD). PCP-PEV representa a Coligação Democrática Unitária (CDU)."
    },
    date(2019, 10, 26): {
        "name": "XXII Legislatura (26/10/2019 - 30/03/2022)",
        "end_date": date(2022, 3, 30),
        "parties": {
            "PS": 108, "PSD": 79, "BE": 19, "PCP": 10, "PEV": 2,
            "CDS-PP": 5, "PAN": 4, "CH": 1, "IL": 1, "L": 1
        },
        "total_mps": 230,
        "notes": "Nota: PCP e PEV frequentemente votam em conjunto como CDU - Coligação Democrática Unitária"
    },
    date(2015, 11, 26): {
        "name": "XXI Legislatura (26/11/2015 - 25/10/2019)",
        "end_date": date(2019, 10, 25),
        "parties": {
            "PSD": 89, "PS": 86, "BE": 19, "CDS-PP": 18, "PCP": 15, "PEV": 2, "PAN": 1
        },
        "total_mps": 230,
        "notes": "Nota: PCP e PEV frequentemente votam em conjunto como CDU - Coligação Democrática Unitária"
    }
    # Add more legislatures here as needed, with the start_date as the key
}


party_name_map = {
    "PS": "Partido Socialista",
    "PSD": "Partido Social Democrata",
    "BE": "Bloco de Esquerda",
    "PCP": "Partido Comunista Português",
    "PEV": "Partido Ecologista \"Os Verdes\"",
    "CDS-PP": "CDS - Partido Popular",
    "PAN": "Pessoas-Animais-Natureza",
    "CH": "Chega",
    "IL": "Iniciativa Liberal",
    "L": "LIVRE"
}