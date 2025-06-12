import pandas as pd
import json
import os
import re
from typing import Dict, List, Set

def clean_message(message: str) -> str:
    """Clean a message by removing unnecessary formatting and whitespace"""
    # Remove HTML tags
    message = re.sub(r'<[^>]+>', '', message)
    # Remove markdown formatting
    message = re.sub(r'\*\*|\*|__|_', '', message)
    # Remove multiple newlines
    message = re.sub(r'\n\s*\n', '\n', message)
    # Remove leading/trailing whitespace
    message = message.strip()
    return message

def format_conversation(comments: List[Dict], requester_id: int) -> str:
    """Format a conversation into a minimal format with Merchant/Agent labels"""
    formatted_messages = []
    
    for comment in comments:
        # Print debugging info if body is missing
        if 'body' not in comment:
            print(f"\nFound comment without 'body' field:")
            print(f"Comment keys: {list(comment.keys())}")
            print(f"Comment data: {comment}")
            continue
            
        # Clean the message content
        clean_content = clean_message(comment['body'])
        
        # Skip empty messages
        if not clean_content:
            continue
            
        # Format the sender label
        if comment['author_id'] == requester_id:
            sender = "Merchant"
        else:
            sender = "Agent"
            
        # Add the formatted message
        formatted_messages.append(f"{sender}:\n{clean_content}")
    
    # Join all messages with double newlines
    return "\n\n".join(formatted_messages)

def load_filtered_csvs(filtered_dir: str) -> Dict[str, pd.DataFrame]:
    """Load filtered CSV files"""
    print("Loading filtered CSV files...")
    csv_data = {}
    
    for business_type in ['vip', 'verified', 'previously_verified', 'unverified']:
        csv_path = os.path.join(filtered_dir, f'{business_type}_conversations.csv')
        if os.path.exists(csv_path):
            df = pd.read_csv(csv_path)
            csv_data[business_type] = df
            print(f"Loaded {len(df)} rows from {business_type} CSV")
    
    return csv_data

def extract_conversations(convos_path: str, csv_data: Dict[str, pd.DataFrame], output_dir: str):
    """Extract and clean conversations from convos.json and save them by business type"""
    print("\nExtracting conversations...")
    
    # Initialize conversation lists for each business type
    conversations = {
        'vip': [],
        'verified': [],
        'previously_verified': [],
        'unverified': []
    }
    
    # Read convos.json line by line
    with open(convos_path, 'r') as f:
        for line in f:
            try:
                convo = json.loads(line.strip())
                ticket_id = convo.get('id')
                
                if ticket_id is None:
                    continue
                
                # Find which business type this ticket belongs to
                for business_type, df in csv_data.items():
                    if ticket_id in df['Id'].values:
                        # Get the CSV row data for this ticket
                        ticket_data = df[df['Id'] == ticket_id].iloc[0].to_dict()
                        
                        # Format the conversation
                        cleaned_conversation = format_conversation(
                            convo.get('comments', []),
                            convo.get('requester_id')
                        )
                        
                        # Create the final object with CSV data and cleaned conversation
                        final_convo = {
                            **ticket_data,  # Include all CSV data
                            'cleaned_conversation': cleaned_conversation
                        }
                        
                        conversations[business_type].append(final_convo)
                        break
                
            except json.JSONDecodeError:
                print(f"Warning: Skipping invalid JSON line")
                continue
    
    # Save conversations to separate JSON files
    print("\nSaving conversations...")
    for business_type, convos in conversations.items():
        if convos:
            output_path = os.path.join(output_dir, f'{business_type}_conversations.json')
            with open(output_path, 'w') as f:
                json.dump(convos, f, indent=2)
            print(f"Saved {len(convos)} conversations to {output_path}")

def main():
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define file paths
    filtered_dir = os.path.join(current_dir, 'filtered_conversations')
    convos_path = os.path.join(current_dir, 'convos.json')
    output_dir = os.path.join(current_dir, 'extracted_conversations')
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Load CSV data
        csv_data = load_filtered_csvs(filtered_dir)
        
        # Extract and save conversations
        extract_conversations(convos_path, csv_data, output_dir)
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        print(f"Error type: {type(e)}")
        raise

if __name__ == "__main__":
    main() 