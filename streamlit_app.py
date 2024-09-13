import threading
import pandas as pd
import time
import csv
import json
import re
import os
import streamlit as st
from openai import OpenAI
from tqdm import tqdm

# Configure the OpenAI API
client = OpenAI(api_key="sk-ffE7yC0bCTbV6hase0HTVaz1VI49ya2Kg9bUFyhZeZT3BlbkFJensRSTmorC8I9a5gCGPoCFPXt_2FByP8ODjPMAfRoA")

# Function to call the GPT API
def call_gpt_api(prompt):
    response = client.chat.completions.create(
        model="gpt-4o",
        temperature=0,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    return response.choices[0].message.content

# Function to parse JSON-like strings and save to CSV
def parse_and_save_to_csv(response_text, output_path):
    data = []
    
    # Use regex to find all JSON-like objects
    json_objects = re.findall(r'\{[^}]+\}', response_text)
    
    for json_str in json_objects:
        try:
            # Remove any trailing commas within the JSON string
            json_str = re.sub(r',\s*}', '}', json_str)
            obj = json.loads(json_str)
            data.append(obj)
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON object: {e}")
            print(f"Problematic JSON string: {json_str}")
    
    # Write data to CSV if there are valid JSON objects
    if data:
        with open(output_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            for item in data:
                writer.writerow([
                    item.get('Name 1', ''),
                    item.get('Name 2', ''),
                    item.get('Response', ''),
                    item.get('Confidence', ''),
                    item.get('Reasoning', '')
                ])
    return len(data)

# Function to process a batch with retry mechanism
def process_batch(batch, output_path, batch_number, max_retries=5):
    valid_entries = []

    for attempt in range(max_retries):
        result = ""
        for _, row in batch.iterrows():
            name1 = row['name1']
            name2 = row['name2']
            result += f"({name1}, {name2})\n"
        
        prompt = f"""
You are an expert system designed to match Indian names, accounting for various spelling differences, typographical errors, and cultural variations. Your task is to determine if two given names likely refer to the same person.

Consider the following factors:

- *Common Indian naming conventions and structures:* Handle common titles (e.g., "Kumar," "Devi," "Lal"), both as part of a name and as a standalone title.
- *Typical spelling variations in Indian names:* Recognize common letter substitutions (e.g., "V" for "B," "T" for "D," "S" for "Z") and potential missing or extra letters.
- *Possible typographical errors or data entry mistakes:* Consider letter swaps, doubled letters, and adjacent key errors.
- *Cultural nicknames or shortened versions of names:* Account for common nicknames, diminutives, or name variations within Indian culture.
- *Titles added in front of the name:* Identify and disregard common prefixes like "Mr," "Mrs," "Shri," "Km," etc., when matching names.
- *Differences in transliteration from Indian languages to English:* Recognize different transliterations that may lead to variations in spelling (e.g., "Sushma" vs. "Sushmaa").

*Avoid overfitting:* Be cautious not to overfit by recognizing false matches, especially with common surnames like "Kumar" or "Singh." If unsure, classify the names as "Doubtful" to minimize false positives.

*Consider context:* If the context suggests the names might refer to the same person despite minor differences, increase confidence. However, prioritize avoiding false positives by flagging uncertain matches as "Doubtful."

For each pair of names, respond with the following in JSON format:

- "Name 1": "<name1>",
- "Name 2": "<name2>",
- "Response": "<Yes/No/Doubtful>",
- "Confidence": "<High/Medium/Low>",
- "Reasoning": "<Your reasoning>"

*Examples to guide the analysis:*

1. *KM SITA - SITA DEVI:* Yes, High. "KM" is a title, and "Devi" is a common suffix; the core name "SITA" matches.
2. *PETEKHRIE - PETEKHRIEU:* Yes, High. The difference is a likely minor spelling variation.
3. *BABY DEVI - BABY KUMARI:* Yes, High. The names match except for the suffix, which could indicate marital status.
4. *KHAN TARANNUM BANO IMAM - TARANNUM BANO IMAM KHAN:* Yes, High. The names match despite the order being different.
5. *ARPITA SINHA - ARPIRA SINHA:* Yes, High. Likely a typographical error with "T" swapped with "R."
6. *ANTIMA - ANTINA:* No, High. While similar, this likely represents different individuals.
7. *BIRENDRA KUMAR - BIJENDRA KUMAR:* No, High. Despite the similar structure, these names are distinct.

*List of cases that require a refined approach:*

1. *ASHISHEK KUMAR - ABHISHEK KUMAR:* Yes, High. The difference is likely a typographical error with a missing "H."
2. *BRIJEH YADAV - BRIJESH YADAV:* Yes, High. The difference is likely a typographical error with a missing "S."
3. *DINESH SHUKLA - DIVESH:* Doubtful, Medium. While "DINESH" and "DIVESH" are similar, they may represent different individuals.
4. *KM NESH BHASKAR - NEHA BHASKAR:* Doubtful, Medium. The core names "NESH" and "NEHA" differ, despite the title "KM."
5. *RAJESH YADAV - RAJNESH YADAV:* Doubtful, Medium. The names are similar but distinct, so it requires further context to determine.
6. *SHUNHAN SAINI - SHUBHAM SAINI:* Doubtful, Medium. Different core names, but the same surname suggests possible family relation, not a match.
7. *SHIVAM SINGH - SHIVAN SINGH:* Yes, High. Likely a minor typographical error with a missing letter.


Analyse the below cases

{result}"""
        
        # Call the GPT API using the prompt
        response_text = call_gpt_api(prompt)
        
        # Parse the response but don't save yet
        valid_entries = []
        json_objects = re.findall(r'\{[^}]+\}', response_text)
        for json_str in json_objects:
            try:
                json_str = re.sub(r',\s*}', '}', json_str)
                obj = json.loads(json_str)
                valid_entries.append(obj)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON object: {e}")
                print(f"Problematic JSON string: {json_str}")
        
        # Check if the number of valid entries matches the number of name pairs in the batch
        if len(valid_entries) == len(batch):
            # Save valid entries to CSV
            with open(output_path, 'a', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)
                for item in valid_entries:
                    writer.writerow([
                        item.get('Name 1', ''),
                        item.get('Name 2', ''),
                        item.get('Response', ''),
                        item.get('Confidence', ''),
                        item.get('Reasoning', '')
                    ])
            print(f"Batch {batch_number} processed successfully on attempt {attempt + 1}.")
            break
        else:
            print(f"Batch {batch_number} failed on attempt {attempt + 1}. Retrying...")
            time.sleep(2)  # Add delay before retrying
    
    # If after all retries, the batch still fails, save what was processed
    if len(valid_entries) < len(batch):
        with open(output_path, 'a', newline='', encoding='utf-8') as csv_file:
            writer = csv.writer(csv_file)
            for item in valid_entries:
                writer.writerow([
                    item.get('Name 1', ''),
                    item.get('Name 2', ''),
                    item.get('Response', ''),
                    item.get('Confidence', ''),
