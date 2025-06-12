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

def format_ticket_url(ticket_id: int) -> str:
    """Format ticket ID as Zendesk URL in Google Sheets HYPERLINK format"""
    return f'=HYPERLINK("https://coingate.zendesk.com/agent/tickets/{ticket_id}", "{ticket_id}")'

def format_business_url(business_id: int) -> str:
    """Format business ID as admin URL in Google Sheets HYPERLINK format"""
    return f'=HYPERLINK("https://admin.coingate.com/admin/businesses/{business_id}", "{business_id}")'

class ConversationAnalyzer:
    def __init__(self, api_key: str, prompt_template: str, model: Literal["gemini", "deepseek"] = "deepseek"):
        """
        Initialize the analyzer with API key and prompt template
        
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

    def analyze_conversation(self, conversation: str, ticket_id: int) -> Dict:
        """
        Analyze a single conversation using the selected API with retry mechanism
        
        Args:
            conversation: The conversation text to analyze
            ticket_id: The ticket ID for logging purposes
            
        Returns:
            Dict: The structured analysis result
        """
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Format the prompt with the conversation
                prompt = self.prompt_template.format(conversation=conversation)
                
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
                
                # Try to access the content
                try:
                    # Strip markdown code blocks if present
                    if content.startswith('```'):
                        content = content.split('\n', 1)[1]  # Remove first line (```json)
                        content = content.rsplit('\n', 1)[0]  # Remove last line (```)
                    content = content.strip()
                    
                    # Fix escaped quotes before parsing JSON
                    content = content.replace('\\\'', "'")
                    
                    # Parse the response as JSON
                    analysis = json.loads(content)
                    return analysis
                    
                except Exception as e:
                    print(f"Error accessing response content: {str(e)}")
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return {
                        "summary": "Error accessing response content",
                        "technical_issues": [],
                        "keywords": []
                    }
                
            except Exception as e:
                error_str = str(e)
                # Check if it's a rate limit error
                if "rate limit" in error_str.lower() or (self.model_type == "deepseek" and hasattr(response, 'status_code') and response.status_code == 429):
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"\nRate limit reached, retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return {
                        "summary": f"Error: {error_str}",
                        "technical_issues": [],
                        "keywords": []
                    }
                else:
                    retry_count += 1
                    if retry_count < max_retries:
                        print(f"Error in analyze_conversation: {error_str}")
                        print(f"Retrying in 10 seconds... (Attempt {retry_count + 1}/{max_retries})")
                        time.sleep(10)
                        continue
                    return {
                        "summary": f"Error: {error_str}",
                        "technical_issues": [],
                        "keywords": []
                    }
        
        # If we've exhausted all retries
        return {
            "summary": "Error: Maximum retry attempts reached",
            "technical_issues": [],
            "keywords": []
        }

def extract_ticket_id_from_url(url: str) -> int:
    """Extract ticket ID from Zendesk URL or Google Sheets HYPERLINK formula"""
    if url.startswith('=HYPERLINK'):
        # Extract ID from HYPERLINK formula
        return int(url.split('"')[3])  # Get the ID from the second quoted part
    else:
        # Handle regular URL format
        return int(url.replace("https://coingate.zendesk.com/agent/tickets/", ""))

def get_processed_ticket_ids(csv_path: str, input_files: List[str]) -> set:
    """Get set of already processed ticket IDs from CSV that also exist in input files"""
    processed_ids = set()
    if os.path.exists(csv_path):
        # First, get all ticket IDs from input files
        input_ticket_ids = set()
        for input_file in input_files:
            if os.path.exists(input_file):
                with open(input_file, 'r') as f:
                    conversations = json.load(f)
                    for convo in conversations:
                        input_ticket_ids.add(convo['Id'])
        
        # Then, only add IDs to processed_ids if they exist in both CSV and input files
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                ticket_id = extract_ticket_id_from_url(row['ticket_id'])
                if ticket_id in input_ticket_ids:
                    processed_ids.add(ticket_id)
    return processed_ids

def process_conversation_file(file_path: str, business_type: str, csv_writer, analyzer: ConversationAnalyzer, processed_ids: set) -> None:
    """Process a single conversation file and write results to CSV"""
    print(f"\nProcessing {business_type} conversations...")
    
    # Read the conversations
    with open(file_path, 'r') as f:
        conversations = json.load(f)
    
    total = len(conversations)
    print(f"Found {total} conversations to process")
    
    for i, convo in enumerate(conversations, 1):
        ticket_id = convo['Id']
        
        # Skip if already processed
        if ticket_id in processed_ids:
            print(f"\rSkipping {i}/{total}", end='', flush=True)
            continue
            
        try:
            # Get conversation data and format URLs
            business_id = format_business_url(convo['business_id'])
            business_order_count = convo['business_order_count']
            conversation_text = convo['cleaned_conversation']
            
            # Show progress
            print(f"\r{i}/{total}", end='', flush=True)
            
            # Analyze the conversation
            analysis = analyzer.analyze_conversation(conversation_text, ticket_id)
            
            # Only write to CSV if we got a valid response (not an error)
            if not analysis['summary'].startswith('Error:'):
                # Write each technical issue as a separate row
                if analysis['technical_issues']:
                    for issue in analysis['technical_issues']:
                        csv_writer.writerow([
                            format_ticket_url(ticket_id),
                            business_id,
                            business_type,
                            business_order_count,
                            analysis['summary'],
                            ','.join(analysis.get('raw_discovery_tags', [])),  # Join raw_discovery_tags with commas
                            issue.get('category', ''),
                            issue.get('subcategory', ''),
                            issue.get('user_intent_failed', ''),
                            issue.get('error_code', ''),
                            issue.get('system_message', ''),
                            issue.get('affected_component', ''),
                            issue.get('description', ''),
                            issue.get('resolution', ''),
                            issue.get('root_cause_hypothesis', '')
                        ])
                else:
                    # If no technical issues, write just the summary and raw_discovery_tags
                    csv_writer.writerow([
                        format_ticket_url(ticket_id),
                        business_id,
                        business_type,
                        business_order_count,
                        analysis['summary'],
                        ','.join(analysis.get('raw_discovery_tags', [])),  # Join raw_discovery_tags with commas
                        '',  # category
                        '',  # subcategory
                        '',  # user_intent_failed
                        '',  # error_code
                        '',  # system_message
                        '',  # affected_component
                        '',  # description
                        '',  # resolution
                        ''   # root_cause_hypothesis
                    ])
            else:
                print(f"\nSkipping ticket {ticket_id} due to API error")
            
        except Exception as e:
            print(f"\nError processing ticket {ticket_id}: {str(e)}")
            # Don't write error to CSV, just log it
        
        # Add a small delay to avoid rate limiting
        time.sleep(1)
    
    # Print newline after progress counter
    print()

def main():
    # Get API keys from environment variables
    deepseek_api_key = os.getenv('DEEPSEEK_API_KEY')
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    
    # Select which model to use (can be changed to "gemini" to use Gemini)
    selected_model = "gemini"
    
    if selected_model == "deepseek" and not deepseek_api_key:
        raise ValueError("DEEPSEEK_API_KEY not found in environment variables")
    elif selected_model == "gemini" and not gemini_api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    
    # Use the appropriate API key based on selected model
    api_key = deepseek_api_key if selected_model == "deepseek" else gemini_api_key
    
    # Default prompt template
    prompt_template = """
    You are a business operations manager at CoinGate. Your objective is to analyze customer support conversations from **business merchants** to identify and categorize **unresolved technical issues** on the CoinGate platform. Make sure that all information is written in English. In case the conversation is in another language, translate it, and all the information to English.

    **Technical Issue Categories (choose ONE primary category):**
    * **Platform Functionality:**
        * Onboarding/Signup (e.g., account creation errors, form submission)
        * Account Access (e.g., login, 2FA, profile switching, password reset)
        * User Interface/Experience (e.g., broken elements, navigation, dashboard display, UI bugs)
        * Notifications (e.g., missing emails, alerts)
        * Reporting/Data Export (e.g., missing reports, incorrect data, statement access)
        * System Performance (e.g., slow loading, timeouts, 500 errors not API-specific, general latency)
    * **Payments & Funds:**
        * Deposit Issues (e.g., uncredited, wrong network/currency detected, tag/memo issues, deposit method problems)
        * Withdrawal Issues (e.g., delays, failures, wrong address/network, beneficiary management, payout method issues)
        * Refund Process (e.g., uninitiated, stuck, technical limitations, no refund link)
        * Order Processing (e.g., status stuck, expiration, under/overpayment detection, suspicious flags)
        * Fiat Conversion (e.g., conversion failure, rate issues, auto-conversion problems)
        * Crypto Conversion (e.g., conversion failure, rate issues)
    * **KYC & Verification:**
        * Document Submission (e.g., upload errors, format issues, size limits)
        * Document Rejection (e.g., unclear reason, specific document type (e.g. proof of address, bank statement, tax declaration) issues, quality issues)
        * Live ID Verification (e.g., stuck step, unsupported document type, regional restrictions)
        * Compliance Holds/Blocks (e.g., account frozen, funds blocked due to KYC)
        * Business Type/Jurisdiction Support (e.g., unsupported legal entity, country restrictions)
        * Shareholder/UBO Verification (e.g., updating, missing info)
    * **API & Integrations:**
        * API Endpoint Errors (e.g., 4xx/5xx responses from specific endpoints, incorrect API responses)
        * API Authentication (e.g., token issues, credential generation)
        * Callback Issues (e.g., not sent, not received, invalid data, incorrect payload)
        * Plugin/Module Compatibility (e.g., WHMCS, Prestashop versions, specific features not working)
        * SDK/Library Issues (e.g., specific library not working as expected)
        * White-Label/Customization Issues (e.g., broken customization, feature discontinuation)
    * **Other:**
        * Security (e.g., fraud flags, account compromise, suspicious activity detection)
        * Feature Limitation (e.g., requested feature not available due to platform design, unchangeable system logic)

    **Issues to ignore (do NOT classify as technical issues):**
    * Pending verification (unless there's an underlying **technical block** preventing progress)
    * Suspended order (unless due to an underlying **technical system error**, not compliance or fraud flags)
    * When a customer **fails to provide or provides lacking documents for KYC** (focus on **system issues** preventing submission/processing or unclear instructions, not user error in document provision)
    * Late bank withdrawals (focus on **system issues** causing delays, not external bank processing times)

    **Instructions for Analysis and Output:**
    1.  **Focus only on technical issues.** If a conversation begins with a non-technical topic that later shifts to technical problems, disregard the initial non-technical content.
    2.  **Identify only *unresolved* technical issues.**
        * If a technical issue is clearly resolved within the conversation, **do not** include it in the `technical_issues` array.
        * For payment issues: If the issue is solely due to a **customer error** (e.g., wrong currency, wrong network, underpayment, sending to an expired order) AND **sufficient information** for a refund process was provided to the customer, **do not** include this specific payment issue in the `technical_issues` array.
        * However, if a payment issue (even a customer-error one) reveals **other, unrelated, unresolved technical issues** on the platform, **only include those specific, unrelated technical issues**.
    3.  If **no unresolved technical issues** are found based on the above criteria, the `technical_issues` array **must be empty** (`[]`).
    4.  For fields like `error_code`, `system_message`, and `affected_component` within the `technical_issues` array, populate them **if the information is directly mentioned or can be logically inferred from the conversation details.** If not, leave the field empty.

    **Your response MUST be a valid JSON object with exactly these fields:**
    * `"summary"`: A brief summary of the conversation's core topic (1-2 sentences, max two sentences, no lists).
    * `"raw_discovery_tags"`: An array of strings. Extract **ONLY concise, technical terms or phrases that directly describe a *problematic system behavior, error message, or specific technical component failure*. Focus on terms that would directly help an engineer diagnose the bug or understand the specific failure mode.**

        **Examples of what to INCLUDE (Focus strictly on these types of diagnostic clues):**
        * Specific HTTP error codes (e.g., "500 error", "404 error", "419 error", "error 403").
        * Exact system error messages or alert texts (e.g., "OrderIsNotValid", "Beneficiary is not valid", "Sorry, you have been blocked", "Cart cannot be loaded").
        * Names of specific system components, features, or integrations *if they are part of the technical problem* (e.g., "Live ID verification step", "WHMCS plugin", "Prestashop module", "API endpoint", "Dashboard reports", "Payout settings", "email sending system", "Support ticket system", "Live Chat Widget", "Account settings/User management section").
        * Specific failure modes of actions (e.g., "document upload failure", "2FA reset issue", "callback not sent", "payment not detected", "withdrawal stuck", "conversion error").
        * Technical protocols or states *if they are part of the problem description* (e.g., "SSL connection error", "certificate verification failed", "pending status stuck").
        * Cryptocurrency details *only if related to a system problem* (e.g., "USDT conversion failure", "wrong network detected").

        **EXCLUDE (Be strict about these exclusions - prioritize diagnostics over context):**
        * Specific dates, durations, or timestamps (e.g., "Jan 2nd 2022", "April 2024", "last months", "3 months", "20 hours", "1985-05-08").
        * Personal names or contact details (e.g., "Michael", "Jurgita", "John Doe", email addresses, phone numbers).
        * Specific personal or business financial identifiers (e.g., bank account numbers, IBANs, SWIFT/BIC codes, full transaction hashes, specific wallet addresses, specific order/ticket IDs unless they are part of a *quoted system message* or *error code*).
        * Generic conversational fillers or greetings (e.g., "thanks", "hello", "best regards").
        * URLs that are not directly diagnostic error logs or system messages.
        * Vague emotional expressions ("frustrated", "concerned") or subjective opinions ("super confusing").
        * Details about the customer's *business model* (e.g., "online consulting", "company in which I work alone", "sole trader").
        * Information describing the *customer's actions* that are *not* a system failure point (e.g., "customer sent wrong currency" - unless it highlights *CoinGate's system failure* to detect it).
        * Information about the *resolution* of the problem or its *status* (e.g., "resolved", "confirmed", "forwarded to payments team", "pending review").
        * Information that simply describes the *content* of a document or an *external system* without a clear technical issue on CoinGate's side (e.g., "privat bank statement", "verified page with a blue check mark", "reviews of my services").
        * Generic phrases that are not specific diagnostic clues (e.g., "same problem", "issue", "problem", "not working", "technical difficulties" on their own).
        * Specific numerical values or amounts unless they are part of a system limit or a diagnostic clue.

        Be comprehensive but concise. Focus *strictly* on technical failure points and diagnostic clues.
    * `"technical_issues"`: An array of objects. Each object in this array **must** have exactly these fields:
        * `"category"`: The **primary category** of the technical issue from the provided list.
        * `"user_intent_failed"`: A **highly specific** description of the user's *attempted action that failed* due to the technical issue. Frame it as a concise action. Examples: "uploading bank statement", "resetting 2FA", "creating a beneficiary", "accessing withdrawal history", "generating API credentials", "making a recurring payment", "inputting birthdate during verification", "completing Live ID verification".
        * `"error_code"`: [Optional] Any specific error code mentioned (e.g., "419", "500", "422"). Leave empty if none.
        * `"system_message"`: [Optional] The exact or closely paraphrased text of any error message or system prompt shown to the user (e.g., "'OrderIsNotValid'", "'Beneficiary is not valid.'", "'Country of registration are restricted'"). Leave empty if none.
        * `"affected_component"`: [Optional] The specific CoinGate feature, module, or section experiencing the issue (e.g., "Live ID verification step", "WHMCS plugin", "Binance Pay integration", "USDT conversion", "Dashboard reports", "Payout settings", "User role management", "Support ticket system"). Be as specific as possible.
        * `"resolution"`: If a resolution was found during the conversation, describe it here. If no resolution was found, leave this field empty. Do not assume resolutions; state only if clearly provided in the conversation.
        * `"root_cause_hypothesis"`: [Optional] A brief, technical hypothesis for the underlying cause of the issue (e.g., "Incorrect API parameter usage", "Database synchronization delay", "Frontend validation bug", "Regulatory limitation"). Leave empty if not clearly inferable from the conversation.
    If a parameter is not present in the conversation, leave the field empty but include the field in the JSON object.arameter usage", "Database synchronization delay", "Frontend validation bug", "Regulatory limitation"). Leave empty if not clearly inferable from the conversation.
        If a parameter is not present in the conversation, leave the field empty but include the field in the JSON object.

    Conversation:
    {conversation}
    """
    
    # Initialize analyzer with selected model
    analyzer = ConversationAnalyzer(api_key, prompt_template, model=selected_model)
    
    # Define input and output paths
    extracted_dir = os.path.join(current_dir, 'extracted_conversations')
    output_csv = os.path.join(current_dir, 'conversation_analysis_7.csv')
    
    # Get list of input files
    business_types = ['vip', 'verified', 'previously_verified', 'unverified']
    input_files = [os.path.join(extracted_dir, f'{business_type}_conversations.json') for business_type in business_types]
    
    # Get already processed ticket IDs that still exist in input files
    processed_ids = get_processed_ticket_ids(output_csv, input_files)
    print(f"Found {len(processed_ids)} already processed tickets that still exist in input files")
    
    # Create or append to CSV file
    file_exists = os.path.exists(output_csv)
    with open(output_csv, 'a', newline='') as f:
        writer = csv.writer(f)
        
        # Write header only if file is new
        if not file_exists:
            writer.writerow([
                'ticket_id',
                'business_id',
                'business_type',
                'business_order_count',
                'summary',
                'raw_discovery_tags',
                'category',
                'user_intent_failed',
                'error_code',
                'system_message',
                'affected_component',
                'resolution',
                'root_cause_hypothesis'
            ])
        
        # Process each business type
        for business_type in business_types:
            input_file = os.path.join(extracted_dir, f'{business_type}_conversations.json')
            if os.path.exists(input_file):
                process_conversation_file(input_file, business_type, writer, analyzer, processed_ids)
            else:
                print(f"Warning: {input_file} not found")
    
    print(f"\nAnalysis complete! Results saved to '{output_csv}'")

if __name__ == "__main__":
    main() 