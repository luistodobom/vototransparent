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
prompt = """Provide this answer in Portuguese from Portugal: This is a government proposal that was voted on in the Portuguese Parliament in Portugal and so is full of legal language. Summarize this document into 4 bullet points, avoiding all the legalese completely and summarize in normal vocabulary. The first bullet point should be a general summary of the proposal. The second should you should think critically about the document and point out inconsistencies if there are any, and if not show how the implementation details align with the goal. The third bullet point should be an educated estimate if the proposal will increase or decrease government spending and increase or decrease government revenue as well, and what the net effect may be. Last bullet point should be another summary, but in more colloquial language."""

# Placeholder for document text (replace with actual PDF text extraction if available)
with open('data/314470.pdf', 'rb') as file:
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