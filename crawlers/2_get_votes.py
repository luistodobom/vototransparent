import os
from dotenv import load_dotenv
import requests
import json
import pypdf

# Load environment variables from .env file
load_dotenv()

# Get Gemini API key
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    raise ValueError("GEMINI_API_KEY not found in .env file")

# Define the prompt
prompt = """This is the voting record on issues on a given day in Parliament in Portugal. I want to extract from here for each issue being voted on: 1. The name (which is also a unique identifier) of the thing being voted on (eg. Projeto de Lei 404/XVI/1 or Proposta de Lei 39/XVI/1 etc.) 2. The hyperlink associated with the issue being voted on. This is good, but the hyperlinks are definitely there. 3. For each political party, I want to know how they voted and how many people voted how per political party (In Favor, Against, Abstain). These are displayed in a table, so extract that information into a table as well. The table may contain numbers, which indicate directly how many voted, or a cross, in which case it means all MPs voted in the same way and the number of MPs is displayed at the top of that table. Output the data in a JSON format"""

# Placeholder for document text (replace with actual PDF text extraction if available)
with open('data/example.pdf', 'rb') as file:
    reader = pypdf.PdfReader(file)
    document_text = ''
    for page in reader.pages:
        document_text += page.extract_text()
# Combine prompt with document text
full_prompt = f"{prompt}\n\nDocument content:\n{document_text}"

# Gemini API endpoint (adjust if using a different model/version)
url = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-lite:generateContent"

# Prepare the request payload
payload = {
    "contents": [
        {
            "parts": [
                {"text": full_prompt}
            ]
        }
    ]
}

# Set headers
headers = {
    "Content-Type": "application/json",
    "x-goog-api-key": GEMINI_API_KEY
}

try:
    # Send request to Gemini API
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()  # Raise exception for bad status codes

    # Parse response
    result = response.json()
    generated_text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "No response received")

    # Print the response
    print("Summary from Gemini API:")
    print(generated_text)

except requests.exceptions.RequestException as e:
    print(f"Error communicating with Gemini API: {e}")
except json.JSONDecodeError:
    print("Error decoding API response")
except Exception as e:
    print(f"An unexpected error occurred: {e}")