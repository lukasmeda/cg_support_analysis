import json
import os
import csv
import time
import requests
import google.generativeai as genai
from typing import Dict, List, Literal
from dotenv import load_dotenv

# Load environment variables
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, 'env.env')
load_dotenv(env_path)

class SubcategoryStandardizer:
    def __init__(self, api_key: str, prompt_template: str, model: Literal["gemini", "deepseek"] = "deepseek"):
        """
        Initialize the standardizer with API key and prompt template
        
        Args:
            api_key: The API key for the selected model
            prompt_template: The template for the analysis prompt
            model: Which model to use ("gemini" or "deepseek")
        """
        self.model_type = model
        self.prompt_template = prompt_template
        
        if model == "gemini":
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel('models/gemini-2.0-flash-lite')
        else:  # deepseek
            self.api_key = api_key
            self.api_url = "https://api.deepseek.com/v1/chat/completions"

    def standardize_subcategory(self, case_data: Dict, issue_types: List[Dict]) -> str:
        """
        Analyze a single case and determine the appropriate tag_name(s)
        
        Args:
            case_data: Dictionary containing the case data
            issue_types: List of issue type definitions
            
        Returns:
            str: Comma-separated list of matching tag_names
        """
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Format the prompt with the case data and issue types
                prompt = self.prompt_template.format(
                    summary=case_data['summary'],
                    raw_discovery_tags=case_data['raw_discovery_tags'],
                    issue_types=json.dumps(issue_types, indent=2)
                )
                
                if self.model_type == "gemini":
                    # Make API call with Gemini
                    response = self.model.generate_content(
                        contents=[{
                            "parts": [{
                                "text": prompt
                            }]
                        }],
                        safety_settings=[
                            {
                                "category": "HARM_CATEGORY_HARASSMENT",
                                "threshold": "BLOCK_NONE"
                            },
                            {
                                "category": "HARM_CATEGORY_HATE_SPEECH",
                                "threshold": "BLOCK_NONE"
                            },
                            {
                                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                                "threshold": "BLOCK_NONE"
                            },
                            {
                                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                                "threshold": "BLOCK_NONE"
                            }
                        ]
                    )
                    content = response.text
                else:
                    # Make API call with DeepSeek
                    headers = {
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json"
                    }
                    
                    data = {
                        "model": "deepseek-chat",
                        "messages": [
                            {"role": "system", "content": "You are a business operations manager at CoinGate."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.7
                    }
                    
                    response = requests.post(self.api_url, headers=headers, json=data)
                    response.raise_for_status()
                    content = response.json()['choices'][0]['message']['content']
                
                # Try to parse the response
                try:
                    # Strip markdown code blocks if present
                    if content.startswith('```'):
                        content = content.split('\n', 1)[1]  # Remove first line (```json)
                        content = content.rsplit('\n', 1)[0]  # Remove last line (```)
                    content = content.strip()
                    
                    # Fix escaped quotes before parsing JSON
                    content = content.replace('\\\'', "'")
                    
                    # Parse the response as JSON
                    result = json.loads(content)
                    return result.get('tag_names', '')
                    
                except Exception as e:
                    print(f"Error parsing response content: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return ''
                
            except Exception as e:
                error_str = str(e)
                # Check if it's a rate limit error
                if "rate limit" in error_str.lower() or (self.model_type == "deepseek" and hasattr(response, 'status_code') and response.status_code == 429):
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"\nRate limit reached, retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return ''
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Error in standardize_subcategory: {error_str}")
                        print(f"Retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return ''
        
        # If we've exhausted all retries
        return ''

def extract_ticket_id_from_url(url: str) -> int:
    """Extract ticket ID from Zendesk URL or Google Sheets HYPERLINK formula"""
    if url.startswith('=HYPERLINK'):
        # Extract ID from HYPERLINK formula
        return int(url.split('"')[3])  # Get the ID from the second quoted part
    else:
        # Handle regular URL format
        return int(url.replace("https://coingate.zendesk.com/agent/tickets/", ""))

def get_processed_ticket_ids(csv_path: str) -> set:
    """Get set of already processed ticket IDs from CSV"""
    processed_ids = set()
    if os.path.exists(csv_path):
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticket_id = extract_ticket_id_from_url(row['ticket_id'])
                processed_ids.add(ticket_id)
    return processed_ids

def main():
    # Get API keys from environment variables
    deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    # Select which model to use (can be changed to "gemini" to use Gemini)
    selected_model = "deepseek"
    
    if selected_model == "deepseek" and not deepseek_api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
    elif selected_model == "gemini" and not gemini_api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    
    # Use the appropriate API key based on selected model
    api_key = deepseek_api_key if selected_model == "deepseek" else gemini_api_key
    
    # Load issue types
    with open(os.path.join(current_dir, 'issue_types.json'), 'r') as f:
        issue_types = json.load(f)['Standardized_Issue_Tags']
    
    # Default prompt template
    prompt_template = """
    You are a business operations manager at CoinGate. Your task is to analyze a customer support case and determine which standardized issue tag(s) it belongs to.

    Case Summary: {summary}
    Raw Discovery Tags: {raw_discovery_tags}

    Available Issue Types:
    {issue_types}

    Instructions:
    1. Analyze the case summary and raw discovery tags
    2. For each issue type, consider ALL available information:
       - The tag_name
       - The definition
       - The raw_tags list
    3. Use this information to understand what each category represents and how it applies to the case
    4. Match the case to one or more of the available issue types
    5. Return ONLY the matching tag_name(s) as a comma-separated list
    6. If no clear match is found, return an empty string

    Your response MUST be a valid JSON object with exactly this field:
    * "tag_names": A comma-separated string of matching tag_name(s), or an empty string if no match is found

    Example response:
    {{"tag_names": "User Role Management, Account Limitation/Closure"}}
    """
    
    # Initialize standardizer
    standardizer = SubcategoryStandardizer(api_key, prompt_template, model=selected_model)
    
    # Define input and output paths
    input_csv = os.path.join(current_dir, 'conversation_analysis_7.csv')
    output_csv = os.path.join(current_dir, 'cs_report_final.csv')
    
    # Get already processed ticket IDs
    processed_ids = get_processed_ticket_ids(output_csv)
    print(f"Found {len(processed_ids)} already processed tickets")
    
    # Process the CSV file
    with open(input_csv, 'r') as infile:
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        # Create or append to output file
        file_exists = os.path.exists(output_csv)
        with open(output_csv, 'a', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            total_rows = sum(1 for row in reader)
            infile.seek(0)
            next(reader)  # Skip header
            
            for i, row in enumerate(reader, 1):
                # Extract ticket ID
                ticket_id = extract_ticket_id_from_url(row['ticket_id'])
                
                # Skip if already processed
                if ticket_id in processed_ids:
                    print(f"\rSkipping {i}/{total_rows}", end='', flush=True)
                    continue
                
                # Skip rows without raw_discovery_tags
                if not row['raw_discovery_tags']:
                    continue
                
                print(f"\rProcessing row {i}/{total_rows}", end='', flush=True)
                
                # Get standardized subcategory
                standardized_tags = standardizer.standardize_subcategory(row, issue_types)
                
                # Update the subcategory field
                row['subcategory'] = standardized_tags
                
                # Write the updated row
                writer.writerow(row)
                
                # Add a small delay to avoid rate limiting
                time.sleep(1)
    
    print(f"\nStandardization complete! Results saved to '{output_csv}'")

if __name__ == "__main__":
    main() 