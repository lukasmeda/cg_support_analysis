import pandas as pd
from google.cloud import bigquery
import json
import os

def get_vip_businesses():
    """Get top 25 businesses by order volume with their order counts"""
    print("\nStep 1: Getting VIP businesses...")
    client = bigquery.Client()
    
    query = """
    WITH business_orders AS (
        SELECT
            dim_businesses.entity_id,
            dim_businesses.entity_email,
            COUNT(*) as order_count,
            SUM(fct_orders.pay_amount_eur) as total_amount_eur
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`fct_orders` AS fct_orders
        INNER JOIN
            `coingate-production`.`dbt_prod_warehouse`.`dim_businesses` AS dim_businesses
        ON
            fct_orders.entity_id = dim_businesses.entity_id
        WHERE
            fct_orders.status = 'paid'
            AND fct_orders.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        GROUP BY
            1, 2
        ORDER BY
            SUM(fct_orders.pay_amount_eur) DESC
        LIMIT 25
    )
    SELECT
        bo.*,
        COALESCE(bo.order_count, 0) as order_count
    FROM
        business_orders bo;
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} VIP businesses")
    return df

def get_business_roles(business_ids, business_type):
    """Get all role emails for businesses"""
    print(f"\nGetting role emails for {business_type} businesses...")
    client = bigquery.Client()
    
    # Convert list of business IDs to string for SQL query (without quotes since they're numeric)
    business_ids_str = ','.join([str(id) for id in business_ids])
    
    query = f"""
    SELECT
        t0.business_id,
        t0.roles,
        t0.user_id,
        dim_users.entity_email,
        dim_businesses.entity_email as business_email
    FROM
        `coingate-production`.`dbt_prod_warehouse`.`bridge_business_users` AS t0
    INNER JOIN
        `coingate-production`.`dbt_prod_warehouse`.`dim_users` AS dim_users
    ON
        t0.user_id = dim_users.entity_id
    INNER JOIN
        `coingate-production`.`dbt_prod_warehouse`.`dim_businesses` AS dim_businesses
    ON
        t0.business_id = dim_businesses.entity_id
    WHERE
        dim_businesses.entity_id IN ({business_ids_str})
    ORDER BY
        business_id, roles;
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} role emails for {business_type} businesses")
    return df

def get_verified_businesses():
    """Get all verified businesses with their order counts"""
    print("\nStep 2: Getting verified businesses...")
    client = bigquery.Client()
    
    query = """
    WITH verified_businesses AS (
        SELECT
            dim_businesses.entity_email,
            dim_businesses.entity_id AS business_id,
            dim_businesses.verification_success_at
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`dim_businesses` AS dim_businesses
        WHERE
            dim_businesses.verification_status = 'success'
            AND dim_businesses.deleted_at IS NULL
    ),
    business_orders AS (
        SELECT
            fct_orders.entity_id,
            COUNT(*) as order_count
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`fct_orders` AS fct_orders
        WHERE
            fct_orders.status = 'paid'
            AND fct_orders.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        GROUP BY
            fct_orders.entity_id
    )
    SELECT
        vb.*,
        COALESCE(bo.order_count, 0) as order_count
    FROM
        verified_businesses vb
    LEFT JOIN
        business_orders bo
    ON
        vb.business_id = bo.entity_id;
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} verified businesses")
    print(f"Verified businesses with 0 orders in past 12 months: {len(df[df['order_count'] == 0])}")
    return df

def get_unverified_businesses():
    """Get businesses that have never been verified"""
    print("\nStep 4: Getting unverified businesses...")
    client = bigquery.Client()
    
    query = """
    WITH unverified_businesses AS (
        SELECT
            dim_businesses.entity_email,
            dim_businesses.entity_id AS business_id
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`dim_businesses` AS dim_businesses
        WHERE
            dim_businesses.verification_first_success_at IS NULL
            AND dim_businesses.verification_status != 'success'
            AND dim_businesses.deleted_at IS NULL
    ),
    business_orders AS (
        SELECT
            fct_orders.entity_id,
            COUNT(*) as order_count
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`fct_orders` AS fct_orders
        WHERE
            fct_orders.status = 'paid'
            AND fct_orders.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        GROUP BY
            fct_orders.entity_id
    )
    SELECT
        ub.*,
        COALESCE(bo.order_count, 0) as order_count
    FROM
        unverified_businesses ub
    LEFT JOIN
        business_orders bo
    ON
        ub.business_id = bo.entity_id;
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} unverified businesses")
    return df

def get_previously_verified_businesses():
    """Get businesses that were verified before but are not verified now"""
    print("\nStep 3: Getting previously verified businesses...")
    client = bigquery.Client()
    
    query = """
    WITH previously_verified_businesses AS (
        SELECT
            dim_businesses.entity_email,
            dim_businesses.entity_id AS business_id,
            dim_businesses.verification_first_success_at,
            dim_businesses.verification_status
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`dim_businesses` AS dim_businesses
        WHERE
            dim_businesses.verification_first_success_at IS NOT NULL
            AND dim_businesses.verification_status != 'success'
            AND dim_businesses.deleted_at IS NULL
    ),
    business_orders AS (
        SELECT
            fct_orders.entity_id,
            COUNT(*) as order_count
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`fct_orders` AS fct_orders
        WHERE
            fct_orders.status = 'paid'
            AND fct_orders.created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        GROUP BY
            fct_orders.entity_id
    )
    SELECT
        pvb.*,
        COALESCE(bo.order_count, 0) as order_count
    FROM
        previously_verified_businesses pvb
    LEFT JOIN
        business_orders bo
    ON
        pvb.business_id = bo.entity_id;
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} previously verified businesses")
    return df

def get_domain_matching_emails(vip_businesses):
    """Get all users whose email domains match VIP business domains"""
    print("\nGetting additional domain-matching emails for VIP businesses...")
    client = bigquery.Client()
    
    # Extract domains from VIP business emails
    domains = set()
    for email in vip_businesses['entity_email']:
        if '@' in email:
            domain = email.split('@')[1]
            domains.add(domain)
    
    # Convert domains to SQL string
    domains_str = ','.join([f"'{domain}'" for domain in domains])
    
    query = f"""
    WITH valid_emails AS (
        SELECT
            dim_users.entity_email,
            SPLIT(dim_users.entity_email, '@')[OFFSET(1)] as domain
        FROM
            `coingate-production`.`dbt_prod_warehouse`.`dim_users` AS dim_users
        WHERE
            dim_users.entity_email LIKE '%@%'
            AND dim_users.deleted_at IS NULL
            AND ARRAY_LENGTH(SPLIT(dim_users.entity_email, '@')) > 1
    )
    SELECT
        entity_email,
        domain
    FROM
        valid_emails
    WHERE
        domain IN ({domains_str});
    """
    
    df = client.query(query).to_dataframe()
    print(f"Found {len(df)} additional domain-matching emails")
    
    # Group emails by domain
    domain_emails = {}
    for _, row in df.iterrows():
        domain = row['domain']
        if domain not in domain_emails:
            domain_emails[domain] = []
        domain_emails[domain].append(row['entity_email'])
    
    return domain_emails

def main():
    # Get current directory
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    try:
        # Step 1: Get VIP businesses
        vip_businesses = get_vip_businesses()
        vip_business_ids = vip_businesses['entity_id'].tolist()
        
        # Step 2: Get verified businesses
        verified_businesses = get_verified_businesses()
        verified_business_ids = verified_businesses['business_id'].tolist()
        
        # Step 3: Get previously verified businesses
        previously_verified_businesses = get_previously_verified_businesses()
        previously_verified_business_ids = previously_verified_businesses['business_id'].tolist()
        
        # Step 4: Get unverified businesses
        unverified_businesses = get_unverified_businesses()
        
        # Step 5: Get role emails for all business types
        vip_roles = get_business_roles(vip_business_ids, "VIP")
        verified_roles = get_business_roles(verified_business_ids, "verified")
        previously_verified_roles = get_business_roles(previously_verified_business_ids, "previously verified")
        
        # Step 6: Get additional domain-matching emails for VIP businesses
        domain_emails = get_domain_matching_emails(vip_businesses)
        
        # Create a dictionary to store all business information
        print("\nStep 7: Creating final business dictionary...")
        businesses = {}
        
        # Process VIP businesses
        for _, row in vip_businesses.iterrows():
            business_id = row['entity_id']
            business_email = row['entity_email']
            domain = business_email.split('@')[1]
            
            # Get role emails
            role_emails = vip_roles[vip_roles['business_id'] == business_id]['entity_email'].tolist()
            
            # Add domain-matching emails that aren't already in role_emails
            if domain in domain_emails:
                additional_emails = [email for email in domain_emails[domain] if email not in role_emails]
                role_emails.extend(additional_emails)
            
            businesses[business_id] = {
                'type': 'vip',
                'main_email': business_email,
                'role_emails': role_emails,
                'order_count': int(row['order_count']),
                'total_amount_eur': float(row['total_amount_eur'])
            }
        
        # Process verified businesses (excluding VIP ones)
        for _, row in verified_businesses.iterrows():
            business_id = row['business_id']
            if business_id not in vip_business_ids:  # Skip if it's a VIP business
                businesses[business_id] = {
                    'type': 'verified',
                    'main_email': row['entity_email'],
                    'role_emails': verified_roles[verified_roles['business_id'] == business_id]['entity_email'].tolist(),
                    'order_count': int(row['order_count']),
                    'verification_date': row['verification_success_at'].isoformat() if pd.notnull(row['verification_success_at']) else None
                }
        
        # Process previously verified businesses (excluding VIP ones)
        for _, row in previously_verified_businesses.iterrows():
            business_id = row['business_id']
            if business_id not in vip_business_ids:  # Skip if it's a VIP business
                businesses[business_id] = {
                    'type': 'previously_verified',
                    'main_email': row['entity_email'],
                    'role_emails': previously_verified_roles[previously_verified_roles['business_id'] == business_id]['entity_email'].tolist(),
                    'order_count': int(row['order_count']),
                    'verification_date': row['verification_first_success_at'].isoformat() if pd.notnull(row['verification_first_success_at']) else None
                }
        
        # Process unverified businesses (excluding VIP ones)
        for _, row in unverified_businesses.iterrows():
            business_id = row['business_id']
            if business_id not in vip_business_ids:  # Skip if it's a VIP business
                businesses[business_id] = {
                    'type': 'unverified',
                    'main_email': row['entity_email'],
                    'role_emails': [],  # Unverified businesses don't have role emails
                    'order_count': int(row['order_count'])
                }
        
        # Save the results
        output_path = os.path.join(current_dir, 'businesses.json')
        with open(output_path, 'w') as f:
            json.dump(businesses, f, indent=2)
        
        # Print detailed statistics
        print(f"\nFinal results:")
        print(f"- Total businesses processed: {len(businesses)}")
        print(f"- VIP businesses: {len(vip_businesses)}")
        print(f"- Verified businesses: {len(verified_businesses) - len(vip_businesses)}")
        print(f"- Previously verified businesses: {len(previously_verified_businesses)}")
        print(f"- Unverified businesses: {len(unverified_businesses)}")
        
        # Print role email statistics
        total_vip_roles = sum(len(b['role_emails']) for b in businesses.values() if b['type'] == 'vip')
        total_verified_roles = len(verified_roles)
        total_previously_verified_roles = len(previously_verified_roles)
        
        avg_vip_roles = total_vip_roles / len(vip_businesses) if len(vip_businesses) > 0 else 0
        avg_verified_roles = total_verified_roles / (len(verified_businesses) - len(vip_businesses)) if (len(verified_businesses) - len(vip_businesses)) > 0 else 0
        avg_previously_verified_roles = total_previously_verified_roles / len(previously_verified_businesses) if len(previously_verified_businesses) > 0 else 0
        
        print(f"\nRole email statistics:")
        print(f"- Total VIP business emails (including domain matches): {total_vip_roles}")
        print(f"- Average emails per VIP business: {avg_vip_roles:.2f}")
        print(f"- Total verified business role emails: {total_verified_roles}")
        print(f"- Average roles per verified business: {avg_verified_roles:.2f}")
        print(f"- Total previously verified business role emails: {total_previously_verified_roles}")
        print(f"- Average roles per previously verified business: {avg_previously_verified_roles:.2f}")
        
        print(f"\nResults saved to '{output_path}'")
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        print(f"Error type: {type(e)}")
        raise

if __name__ == "__main__":
    main() 