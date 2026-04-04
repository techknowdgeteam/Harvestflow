import connectwithinfinitydb as db
import json
import os
from datetime import datetime
import time
from webdriver_manager.chrome import ChromeDriverManager

DEFAULT_PATH = r"C:\xampp\htdocs\synapse\synarex"
FETCHED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\fetched_investors.json"
UPDATED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\updated_investors.json"

def fetch_insiders_rows():
    try:
        print(f"[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Starting fetch...")
        
        # Ensure correct table name
        query = "SELECT * FROM insiders" 
        result = db.execute_query(query)
        
        if result.get('status') != 'success':
            print(f"QUERY ERROR: {result.get('message')}")
            return
            
        rows = result.get('results', [])

        if not rows:
            print("WARNING: Database returned 'success' but the results list is empty.")
            print("Check if the table 'insiders' actually has rows in the PHP interface.")
            return
            
        print(f"SUCCESS: Fetched {len(rows)} records from 'insiders'")
        
        investors_data = {}
        for row in rows:
            # Safely identify the unique ID for the JSON key
            record_id = str(row.get('id') or row.get('ID') or "")
            if record_id:
                investors_data[record_id] = row
            else:
                # Fallback if no ID is found (uses timestamp as key)
                temp_key = f"unknown_{datetime.now().timestamp()}"
                investors_data[temp_key] = row
        
        # Save to file
        os.makedirs(os.path.dirname(FETCHED_INVESTORS), exist_ok=True)
        with open(FETCHED_INVESTORS, 'w', encoding='utf-8') as f:
            json.dump(investors_data, f, indent=4, default=str)
            
        print(f"DONE: Data saved to {FETCHED_INVESTORS}")
        
    except Exception as e:
        print(f"CRITICAL ERROR in fetch process: {e}")
    finally:
        db.shutdown()


def update_insiders_from_json():
    """
    Updates the insiders database table with data from updated_investors.json.
    Properly handles JSON fields including trades and unauthorized_actions.
    """
    try:
        print(f"[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Starting update process...")

        if not os.path.exists(UPDATED_INVESTORS):
            print(f"Error: File not found at {UPDATED_INVESTORS}")
            return

        with open(UPDATED_INVESTORS, 'r', encoding='utf-8') as f:
            updated_data = json.load(f)

        print(f"Loaded {len(updated_data)} investor records from {UPDATED_INVESTORS}")
        print("-" * 80)

        success_count = 0
        error_count = 0
        skip_count = 0

        for record_key, data in updated_data.items():
            target_id = record_key 
            
            print(f"\n📋 Processing ID: {target_id}")
            
            # --- STEP 1: VERIFY IF ID EXISTS ---
            check_query = f"SELECT id FROM insiders WHERE id = '{target_id}'"
            check_result = db.execute_query(check_query)
            
            if not check_result.get('results'):
                print(f"   ❌ SKIP: ID '{target_id}' not found in database. Moving to next...")
                skip_count += 1
                continue

            print(f"   ✅ ID verified in database")
            
            # --- STEP 2: DATA PREPARATION ---
            # Helper function to properly format JSON for SQL
            def prepare_json_field(val):
                """Convert Python object to JSON string for SQL storage"""
                if val is None:
                    return '{}'
                if isinstance(val, str):
                    # If it's already a string, try to parse and re-stringify to ensure valid JSON
                    try:
                        parsed = json.loads(val)
                        return json.dumps(parsed, separators=(',', ':'))
                    except:
                        return val
                # Convert Python dict/list to JSON string
                return json.dumps(val, separators=(',', ':'))
            
            def escape_sql_string(val):
                """Escape single quotes for SQL string insertion"""
                if val is None:
                    return ''
                return str(val).replace("'", "''")

            # Extract and prepare data
            server = escape_sql_string(data.get('server', ''))
            login = escape_sql_string(data.get('login', ''))
            password = escape_sql_string(data.get('password', ''))
            application_status = escape_sql_string(data.get('application_status', 'pending'))
            
            # Numeric fields
            broker_balance = data.get('broker_balance', 0)
            if broker_balance is None:
                broker_balance = 0
            
            profitandloss = data.get('profitandloss', 0)
            if profitandloss is None:
                profitandloss = 0
            
            contract_days_left = data.get('contract_days_left', 30)
            if contract_days_left is None:
                contract_days_left = 30
            
            # JSON fields - preserve exact structure
            trades_data = data.get('trades', {})
            trades_json = prepare_json_field(trades_data)
            
            unauthorized_actions = data.get('unauthorized_actions', {})
            unauthorized_json = prepare_json_field(unauthorized_actions)
            
            # Additional fields that might be in the JSON
            execution_start_date = escape_sql_string(data.get('execution_start_date', ''))
            last_audit_timestamp = escape_sql_string(data.get('last_audit_timestamp', ''))
            current_balance = data.get('current_balance', broker_balance)
            if current_balance is None:
                current_balance = broker_balance
            
            authorized_tickets_count = data.get('authorized_tickets_count', 0)
            magic_number = data.get('magic_number', 0)
            
            # Check for bypass note
            bypass_note = escape_sql_string(data.get('bypass_note', ''))
            message = escape_sql_string(data.get('message', ''))
            
            print(f"   📊 Data prepared:")
            print(f"      • Server: {server}")
            print(f"      • Login: {login}")
            print(f"      • Status: {application_status}")
            print(f"      • Balance: ${broker_balance:.2f}")
            print(f"      • P&L: ${profitandloss:.2f}")
            print(f"      • Contract Days: {contract_days_left}")
            print(f"      • Authorized Tickets: {authorized_tickets_count}")
            print(f"      • Magic Number: {magic_number}")
            
            # --- STEP 3: CONSTRUCT UPDATE QUERY ---
            # Build the UPDATE query with all fields
            update_query = f"""
                UPDATE insiders 
                SET 
                    server = '{server}',
                    login = '{login}',
                    password = '{password}',
                    application_status = '{application_status}',
                    broker_balance = {broker_balance},
                    profitandloss = {profitandloss},
                    contract_days_left = {contract_days_left},
                    trades = '{trades_json}',
                    unauthorized_actions = '{unauthorized_json}',
                    execution_start_date = '{execution_start_date}',
                    last_audit_timestamp = '{last_audit_timestamp}',
                    current_balance = {current_balance},
                    authorized_tickets_count = {authorized_tickets_count},
                    magic_number = {magic_number},
                    bypass_note = '{bypass_note}',
                    message = '{message}',
                    last_updated = NOW()
                WHERE id = '{target_id}'
            """.strip()
            
            # Debug: Print first 500 chars of query
            print(f"   🔍 Executing UPDATE for ID {target_id}...")
            if len(update_query) > 500:
                print(f"      Query preview: {update_query[:500]}...")
            else:
                print(f"      Query: {update_query}")
            
            # Execute the update
            result = db.execute_query(update_query)
            
            if result.get('status') == 'success':
                print(f"   ✅ SUCCESS: Updated ID {target_id}")
                success_count += 1
                
                # Optional: Verify the update - handle different return formats
                verify_query = f"SELECT id, application_status, broker_balance, profitandloss FROM insiders WHERE id = '{target_id}'"
                verify_result = db.execute_query(verify_query)
                
                if verify_result.get('results'):
                    verified_row = verify_result['results'][0]
                    
                    # Handle different return types (tuple, list, or dict)
                    if isinstance(verified_row, dict):
                        # Dictionary format
                        verified_id = verified_row.get('id')
                        verified_status = verified_row.get('application_status')
                        verified_balance = verified_row.get('broker_balance')
                        verified_pnl = verified_row.get('profitandloss')
                        print(f"      Verified: ID={verified_id}, Status={verified_status}, Balance=${float(verified_balance or 0):.2f}, P&L=${float(verified_pnl or 0):.2f}")
                    elif isinstance(verified_row, (list, tuple)):
                        # List or tuple format
                        verified_id = verified_row[0] if len(verified_row) > 0 else 'N/A'
                        verified_status = verified_row[1] if len(verified_row) > 1 else 'N/A'
                        verified_balance = verified_row[2] if len(verified_row) > 2 else 0
                        verified_pnl = verified_row[3] if len(verified_row) > 3 else 0
                        print(f"      Verified: ID={verified_id}, Status={verified_status}, Balance=${float(verified_balance or 0):.2f}, P&L=${float(verified_pnl or 0):.2f}")
                    else:
                        print(f"      Verified: Update confirmed (unable to parse return format)")
            else:
                print(f"   ❌ ERROR: Failed to update ID {target_id}: {result.get('message')}")
                error_count += 1
            
            # Small pause to prevent rate-limiting
            time.sleep(0.5)
        
        # Print final summary
        print("\n" + "="*80)
        print("📊 UPDATE SUMMARY")
        print("="*80)
        print(f"   • Total records processed: {len(updated_data)}")
        print(f"   • Successfully updated: {success_count}")
        print(f"   • Errors: {error_count}")
        print(f"   • Skipped (ID not found): {skip_count}")
        print("="*80)
        
        return {
            'success': success_count,
            'errors': error_count,
            'skipped': skip_count,
            'total': len(updated_data)
        }

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.shutdown()
        print(f"\n[ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ] Database connection closed.")
               
if __name__ == "__main__":
   update_insiders_from_json()
    