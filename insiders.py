import connectwithinfinitydb as db
import json
import os
from datetime import datetime
import time
import MetaTrader5 as mt5
import multiprocessing as mp
from pathlib import Path
from webdriver_manager.chrome import ChromeDriverManager
import shutil
from datetime import datetime, date
from decimal import Decimal
import json
import re
from typing import Any, Dict, List, Union


DEFAULT_MT5_PATH = r"C:\xampp\htdocs\harvcore\mt5\MetaTrader 5"
MT5_DESTINATION_PATH = r"C:\xampp\htdocs\harvcore\mt5"
INV_PATH = r"C:\xampp\htdocs\harvcore\harvox\usersdata\investors"
DEFAULT_PATH = r"C:\xampp\htdocs\harvcore\harvox"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\harvcore\harvox\harvcore_accountmanagement.json"
SUSPENDED_ACCOUNTS = r"C:\xampp\htdocs\harvcore\harvox\suspended_accounts.json"
FETCHED_INVESTORS = r"C:\xampp\htdocs\harvcore\harvox\fetched_investors.json"
UPDATED_INVESTORS = r"C:\xampp\htdocs\harvcore\harvox\updated_investors.json"


def work_only_in_specific_timerange():
    """
    Function: Checks if current time falls within any of the allowed work time ranges
    from default_accountmanagement.json (global setting).
    Function will ONLY work during specified time windows.
    Does NOT need MT5 connection - just checks time configuration.
    
    Returns:
        dict: Statistics about the time range check including whether function should work
    """
    global restricted_timerange_alert
    
    from datetime import datetime
    from pathlib import Path
    
    print(f"\n{'='*10} ⏰ WORK TIME CHECK (Only work during specified hours) {'='*10}")
    
    # --- TIME CHECK ---
    current_time = datetime.now()
    
    # --- DATA INITIALIZATION ---
    stats = {
        "processing_success": False,
        "current_time": current_time.strftime('%I:%M:%S %p'),
        "should_work": False,
        "has_time_restriction": False,
        "time_windows": [],
        "errors": []
    }
    
    # Load default configuration
    default_config = None
    default_config_path = Path(DEFAULT_ACCOUNTMANAGEMENT)
    
    if not default_config_path.exists():
        print(f"  ⚠️ Default config not found: {DEFAULT_ACCOUNTMANAGEMENT}")
        stats["errors"].append(f"Default config not found: {DEFAULT_ACCOUNTMANAGEMENT}")
        stats["processing_success"] = True  # Still success, just no restriction
        stats["should_work"] = True  # If no config, allow work
        return stats
    
    try:
        with open(default_config_path, 'r', encoding='utf-8') as f:
            default_config = json.load(f)
    except Exception as e:
        print(f"  ⚠️ Error loading default config: {e}")
        stats["errors"].append(f"Error loading default config: {e}")
        stats["processing_success"] = True
        stats["should_work"] = True  # If error loading, allow work
        return stats
    
    # Parse time strings (e.g., "12:00 am" or "12:30 pm" or "21:00")
    def parse_time_string(time_str):
        time_str = time_str.lower().strip().replace(" ", "")
        
        is_pm = "pm" in time_str
        is_am = "am" in time_str
        
        clean_time = time_str.replace("pm", "").replace("am", "")
        
        if ":" in clean_time:
            parts = clean_time.split(":")
            hour = int(parts[0])
            minute = int(parts[1]) if len(parts) > 1 else 0
        else:
            hour = int(clean_time)
            minute = 0
        
        if is_pm and hour != 12:
            hour += 12
        elif is_am and hour == 12:
            hour = 0
        
        return hour, minute
    
    # Convert to 12-hour format for display
    def to_12hr(hour, minute):
        period = "AM" if hour < 12 else "PM"
        hour_12 = hour % 12
        if hour_12 == 0:
            hour_12 = 12
        return f"{hour_12}:{minute:02d} {period}"
    
    # Try to load from default accountmanagement.json (global setting)
    has_time_restriction = False
    time_windows_list = []
    is_within_any_window = False
    matched_window = None
    
    try:
        default_settings = default_config.get("settings", {})
        time_ranges = default_settings.get("execute_function_in_time_range_of", [])
        
        # Handle both old format (single dict) and new format (list of dicts)
        if isinstance(time_ranges, dict):
            # Old format - single time range
            time_ranges = [time_ranges]
        
        if time_ranges and len(time_ranges) > 0:
            has_time_restriction = True
            print(f"  📋 Found {len(time_ranges)} time window(s) in DEFAULT config")
            
            current_time_minutes = current_time.hour * 60 + current_time.minute
            
            for idx, time_range in enumerate(time_ranges):
                if "from" in time_range and "to" in time_range:
                    try:
                        # Parse start time
                        start_hour, start_minute = parse_time_string(time_range["from"])
                        # Parse end time
                        end_hour, end_minute = parse_time_string(time_range["to"])
                        
                        # Calculate minutes
                        start_minutes = start_hour * 60 + start_minute
                        end_minutes = end_hour * 60 + end_minute
                        
                        # Check if window crosses midnight
                        crosses_midnight = end_minutes < start_minutes
                        
                        if crosses_midnight:
                            is_in_window = (current_time_minutes >= start_minutes or 
                                           current_time_minutes <= end_minutes)
                        else:
                            is_in_window = start_minutes <= current_time_minutes <= end_minutes
                        
                        # Format for display
                        start_12hr = to_12hr(start_hour, start_minute)
                        end_12hr = to_12hr(end_hour, end_minute)
                        
                        window_info = {
                            'index': idx + 1,
                            'from': time_range['from'],
                            'to': time_range['to'],
                            'from_24hr': f"{start_hour:02d}:{start_minute:02d}",
                            'to_24hr': f"{end_hour:02d}:{end_minute:02d}",
                            'from_12hr': start_12hr,
                            'to_12hr': end_12hr,
                            'is_within': is_in_window
                        }
                        
                        time_windows_list.append(window_info)
                        
                        if is_in_window:
                            is_within_any_window = True
                            matched_window = window_info
                            print(f"  🕘 Window {idx + 1}: {time_range['from']} - {time_range['to']} ✅ WITHIN")
                        else:
                            print(f"  🕘 Window {idx + 1}: {time_range['from']} - {time_range['to']}  OUTSIDE")
                            
                    except Exception as e:
                        stats["errors"].append(f"Failed to parse time range {idx}: {e}")
                        print(f"  ⚠️ Failed to parse window {idx + 1}: {e}")
            
            if is_within_any_window and matched_window:
                print(f"\n  ✅ Current time {current_time.strftime('%I:%M:%S %p')} is WITHIN window {matched_window['index']}: {matched_window['from']} - {matched_window['to']}")
            elif has_time_restriction and not is_within_any_window:
                print(f"\n   Current time {current_time.strftime('%I:%M:%S %p')} is NOT within ANY work window")
                
    except Exception as e:
        stats["errors"].append(f"Error loading time ranges: {e}")
        print(f"  ⚠️ Error processing time ranges: {e}")
    
    # If no time restriction defined = work always
    if not has_time_restriction:
        is_within_any_window = True
        print(f"  ℹ️ No time restriction defined - work always allowed")
    
    # Display current time
    print(f"  🕐 Current time: {current_time.strftime('%I:%M:%S %p')}")
    
    # Final decision
    if is_within_any_window:
        print(f"  ✅ WITHIN work time window - Function CAN work")
        stats["should_work"] = True
    else:
        print(f"   OUTSIDE work time window - Function CANNOT work")
        stats["should_work"] = False
    
    stats["has_time_restriction"] = has_time_restriction
    stats["time_windows"] = time_windows_list
    stats["matched_window"] = matched_window
    stats["processing_success"] = True

    # --- SET GLOBAL ALERT FLAG ---
    restricted_timerange_alert = {
        'is_triggered': is_within_any_window,
        'timestamp': current_time.strftime('%I:%M:%S %p'),
        'time_windows': time_windows_list,
        'matched_window': matched_window,
        'should_work': is_within_any_window
    }

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 SUMMARY {'='*10}")
    print(f"  Has time restriction: {has_time_restriction}")
    if has_time_restriction:
        print(f"  Total time windows: {len(time_windows_list)}")
        print(f"  Within any window: {is_within_any_window}")
        if matched_window:
            print(f"  Matched window: {matched_window['from']} - {matched_window['to']}")
    else:
        print(f"  Within work window: {is_within_any_window} (always allowed)")
    print(f"  Function should work: {is_within_any_window}")
    
    print(f"{'='*10} 🏁 COMPLETE {'='*10}\n")
    
    return stats

def fetch_insiders_streaming(batch_size=5000):
    """Stream results directly to file without holding all in memory"""
    
    def repair_json_field(value):
        """Intelligently detect and repair JSON fields, even if they're escaped or malformed"""
        if value is None:
            return None
        
        # If it's already a dict or list, return as is
        if isinstance(value, (dict, list)):
            return value
        
        # If it's not a string, return original
        if not isinstance(value, str):
            return value
        
        # Trim whitespace
        value = value.strip()
        
        # Check if it looks like JSON (starts with { or [)
        if not (value.startswith('{') or value.startswith('[')):
            # Check if it might be a string representation of JSON
            # Handle cases like "{\n    \"key\": \"value\"\n}"
            if (value.startswith('"{') and value.endswith('}"')) or \
               (value.startswith("'{") and value.endswith("}'")) or \
               (value.startswith('"[') and value.endswith(']"')) or \
               (value.startswith("'[") and value.endswith("]'")):
                # Remove outer quotes
                value = value[1:-1]
            
            # Check again after removing quotes
            if not (value.strip().startswith('{') or value.strip().startswith('[')):
                return value  # Not JSON-like, return as is
        
        # Try to parse JSON
        try:
            # First attempt: direct parsing
            return json.loads(value)
        except json.JSONDecodeError:
            pass
        
        # Second attempt: Fix common issues
        try:
            # Replace escaped quotes
            fixed_value = value.replace('\\"', '"').replace("\\'", "'")
            # Fix unescaped newlines in strings
            fixed_value = re.sub(r'(?<!")\n(?!")', '\\n', fixed_value)
            # Fix missing quotes around keys
            fixed_value = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_]*)\s*:', r'\1"\2":', fixed_value)
            # Fix single quotes to double quotes
            fixed_value = fixed_value.replace("'", '"')
            # Fix trailing commas
            fixed_value = re.sub(r',\s*}', '}', fixed_value)
            fixed_value = re.sub(r',\s*\]', ']', fixed_value)
            # Remove BOM if present
            if fixed_value.startswith('\ufeff'):
                fixed_value = fixed_value[1:]
            
            return json.loads(fixed_value)
        except json.JSONDecodeError:
            pass
        
        # Third attempt: Use ast.literal_eval for Python literals
        try:
            import ast
            result = ast.literal_eval(value)
            # If it parsed successfully, convert to JSON-serializable format
            if isinstance(result, (dict, list, tuple)):
                return result
        except (ValueError, SyntaxError, ImportError):
            pass
        
        # Fourth attempt: Handle nested escaped JSON
        try:
            # Try to unescape multiple times
            unescaped = value
            for _ in range(5):  # Max 5 levels of escaping
                if '\\"' in unescaped:
                    unescaped = unescaped.replace('\\"', '"')
                elif "\\'" in unescaped:
                    unescaped = unescaped.replace("\\'", "'")
                else:
                    break
            
            if unescaped != value:
                return json.loads(unescaped)
        except json.JSONDecodeError:
            pass
        
        # Fifth attempt: String to dict conversion for specific patterns
        try:
            # Check if it's a string representation of a dict/list
            if value.startswith('{') and value.endswith('}') or value.startswith('[') and value.endswith(']'):
                # Replace literal string 'NULL' with None
                fixed_value = value.replace(': "NULL"', ': null').replace(': NULL', ': null')
                fixed_value = fixed_value.replace('"NULL"', 'null')
                # Replace 'true'/'false' strings
                fixed_value = fixed_value.replace(': "true"', ': true').replace(': "false"', ': false')
                fixed_value = fixed_value.replace('"true"', 'true').replace('"false"', 'false')
                # Replace decimal strings
                fixed_value = re.sub(r'"(\d+\.\d+)"', r'\1', fixed_value)
                
                return json.loads(fixed_value)
        except json.JSONDecodeError:
            pass
        
        # If all attempts fail, return original string
        return value
    
    def clean_record(record, default_accountmanagement=None):
        """Clean a record by repairing all fields that might contain JSON"""
        cleaned = {}
        for key, value in record.items():
            if isinstance(value, str) and len(value) > 0:
                # Attempt to repair JSON fields
                repaired = repair_json_field(value)
                cleaned[key] = repaired
            else:
                cleaned[key] = value
        
        # Check if this record has empty/null accountmanagement and fill with default if needed
        if default_accountmanagement is not None:
            accountmanagement = cleaned.get('accountmanagement')
            # Check if accountmanagement is empty, null, or just whitespace
            if (accountmanagement is None or 
                accountmanagement == '' or 
                (isinstance(accountmanagement, str) and accountmanagement.strip() == '') or
                (isinstance(accountmanagement, dict) and len(accountmanagement) == 0) or
                (isinstance(accountmanagement, list) and len(accountmanagement) == 0)):
                # Fill with default accountmanagement data
                cleaned['accountmanagement'] = default_accountmanagement
                cleaned['_accountmanagement_filled'] = True  # Optional: track that it was filled
                cleaned['_filled_at'] = datetime.now().isoformat()  # Optional: timestamp
        
        return cleaned
    
    print("\n" + "="*70)
    print(f"  INSIDERS DATA EXPORT - STREAMING FETCH")
    print("="*70)
    print(f"  Start Time  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Batch Size  : {batch_size:,} records per batch")
    print("-"*70)
    
    try:
        # Step 1: Test Connection and Get Actual Data Columns
        print("\n📡 [1/7] Testing Database Connection & Fetching Schema...")
        test_query = "SELECT * FROM insiders LIMIT 1"
        test_result = db.execute_query(test_query)
        
        if test_result.get('status') != 'success':
            print(f"   Connection FAILED: {test_result.get('message')}")
            return
        print(f"  ✅ Connection SUCCESSFUL")
        
        # Get column names from the first row of actual data
        columns = None
        results = test_result.get('results', [])
        if results and len(results) > 0:
            # Get column names from the first row's keys
            columns = list(results[0].keys())
            print(f"  📋 Found {len(columns)} columns from data: {', '.join(columns[:5])}{'...' if len(columns) > 5 else ''}")
        else:
            print(f"  ⚠️  No data rows to determine schema, falling back to SELECT *")
        
        # Step 2: Get Total Count
        print("\n📊 [2/7] Counting Total Records...")
        count_query = "SELECT COUNT(*) as total FROM insiders"
        count_result = db.execute_query(count_query)
        
        total_rows = 0
        if isinstance(count_result, dict) and count_result.get('status') == 'success':
            results = count_result.get('results', [])
            if results and len(results) > 0:
                total_rows = int(results[0].get('total') or 
                               results[0].get('COUNT(*)') or 
                               results[0].get('count') or 0)
        
        print(f"  📈 Total Records Found: {total_rows:,}")
        
        if total_rows == 0:
            print(f"  ⚠️  No records to fetch. Export cancelled.")
            return
        
        # Calculate batches needed
        total_batches = (total_rows + batch_size - 1) // batch_size
        print(f"  📦 Estimated Batches: {total_batches}")
        
        # Step 3: Fetch Suspended/Blacklisted Accounts (FIXED)
        print(f"\n🚫 [3/7] Fetching Suspended/Blacklisted Accounts...")
        suspended_users = []
        
        # FIXED: Properly check for non-NULL values and exact matches
        suspended_query = """
            SELECT id, email, fullname, login, application_status, server_decision 
            FROM insiders 
            WHERE (application_status IS NOT NULL AND application_status IN ('suspended', 'blacklisted'))
               OR (server_decision IS NOT NULL AND server_decision IN ('suspended', 'blacklisted'))
            ORDER BY id
        """
        
        suspended_result = db.execute_query(suspended_query)
        if suspended_result.get('status') == 'success':
            suspended_rows = suspended_result.get('results', [])
            
            for row in suspended_rows:
                # Only add if at least one status matches exactly
                app_status = row.get('application_status')
                srv_decision = row.get('server_decision')
                
                # Double-check filtering (case-insensitive)
                is_suspended = False
                status_value = None
                
                if app_status and isinstance(app_status, str):
                    app_status_lower = app_status.lower()
                    if app_status_lower in ['suspended', 'blacklisted']:
                        is_suspended = True
                        status_value = app_status
                
                if not is_suspended and srv_decision and isinstance(srv_decision, str):
                    srv_decision_lower = srv_decision.lower()
                    if srv_decision_lower in ['suspended', 'blacklisted']:
                        is_suspended = True
                        status_value = srv_decision
                
                if is_suspended:
                    suspended_users.append({
                        'id': row.get('id'),
                        'email': row.get('email'),
                        'fullname': row.get('fullname'),
                        'login': row.get('login'),
                        'application_status': row.get('application_status'),
                        'server_decision': row.get('server_decision'),
                        'status_source': 'application_status' if app_status and app_status.lower() in ['suspended', 'blacklisted'] else 'server_decision',
                        'suspended_at': datetime.now().isoformat()
                    })
            
            # Save suspended accounts to JSON file
            os.makedirs(os.path.dirname(SUSPENDED_ACCOUNTS), exist_ok=True)
            with open(SUSPENDED_ACCOUNTS, 'w', encoding='utf-8') as f:
                json.dump({
                    'total_suspended': len(suspended_users),
                    'last_updated': datetime.now().isoformat(),
                    'suspended_accounts': suspended_users
                }, f, indent=2, default=str)
            
            if len(suspended_users) > 0:
                print(f"  ✅ Suspended Accounts Saved: {len(suspended_users)} accounts")
                # Show first few suspended users for verification
                for i, user in enumerate(suspended_users[:3]):
                    print(f"     - ID: {user['id']}, Email: {user['email']}, Status: {user['status_source']}")
                if len(suspended_users) > 3:
                    print(f"     ... and {len(suspended_users) - 3} more")
            else:
                print(f"  ℹ️  No suspended/blacklisted accounts found")
            print(f"  📁 File: {SUSPENDED_ACCOUNTS}")
        else:
            print(f"  ⚠️  Failed to fetch suspended accounts: {suspended_result.get('message')}")
        
        # Step 4: Fetch Server Account Management and Requirements (Updated)
        print(f"\n⚙️ [4/7] Fetching Default Server Account Management & Requirements...")
        
        # Updated query to fetch min_broker_balance and contract_duration
        server_acct_query = """
            SELECT 
                accountmanagement,
                min_broker_balance,
                contract_duration
            FROM server_account 
            LIMIT 1
        """
        server_result = db.execute_query(server_acct_query)
        
        default_accountmanagement = None  # Store default value for filling empty user accountmanagement
        
        if server_result.get('status') == 'success':
            server_rows = server_result.get('results', [])
            if server_rows and len(server_rows) > 0:
                server_row = server_rows[0]
                server_acct_management = server_row.get('accountmanagement')
                min_broker_balance = server_row.get('min_broker_balance')
                contract_duration = server_row.get('contract_duration')
                
                # Parse the accountmanagement JSON
                parsed_management = None
                if server_acct_management:
                    try:
                        # Try to repair/parse the JSON
                        if isinstance(server_acct_management, str):
                            parsed_management = repair_json_field(server_acct_management)
                        else:
                            parsed_management = server_acct_management
                        
                        # Ensure parsed_management is a dictionary
                        if not isinstance(parsed_management, dict):
                            if isinstance(parsed_management, list):
                                # Convert list to dict with 'data' key
                                parsed_management = {'data': parsed_management}
                            else:
                                # Create new dict with original data
                                parsed_management = {'value': parsed_management}
                    except Exception as e:
                        print(f"  ⚠️  Failed to parse accountmanagement: {str(e)}")
                        parsed_management = {}
                else:
                    parsed_management = {}
                
                # Ensure parsed_management is a dict
                if not isinstance(parsed_management, dict):
                    parsed_management = {}
                
                # Add requirements section with fetched values
                requirements = {}
                
                # Add contract_duration if not None
                if contract_duration is not None:
                    requirements['contract_duration'] = contract_duration
                else:
                    requirements['contract_duration'] = None
                    print(f"  ⚠️  contract_duration is NULL in server_account")
                
                # Add min_broker_balance if not None
                if min_broker_balance is not None:
                    # Convert Decimal to float for JSON serialization
                    if isinstance(min_broker_balance, Decimal):
                        requirements['min_broker_balance'] = float(min_broker_balance)
                    else:
                        requirements['min_broker_balance'] = min_broker_balance
                else:
                    requirements['min_broker_balance'] = None
                    print(f"  ⚠️  min_broker_balance is NULL in server_account")
                
                # Add requirements to the parsed management data
                parsed_management['requirements'] = requirements
                
                # Store as default for filling empty user accountmanagement
                default_accountmanagement = parsed_management
                
                # Save directly as JSON (not nested under a field)
                os.makedirs(os.path.dirname(DEFAULT_ACCOUNTMANAGEMENT), exist_ok=True)
                with open(DEFAULT_ACCOUNTMANAGEMENT, 'w', encoding='utf-8') as f:
                    json.dump(parsed_management, f, indent=2, default=str)
                
                print(f"  ✅ Default Server Account Management Loaded with Requirements")
                print(f"  📁 File: {DEFAULT_ACCOUNTMANAGEMENT}")
                print(f"  📋 Type: {type(parsed_management).__name__}")
                
                # Show preview of default data including new requirements
                print(f"  🔍 Requirements Added:")
                print(f"     - contract_duration: {requirements.get('contract_duration')} days")
                print(f"     - min_broker_balance: ${requirements.get('min_broker_balance')}")
                
                # Show preview of existing data keys
                existing_keys = [k for k in parsed_management.keys() if k != 'requirements']
                if existing_keys:
                    print(f"  🔍 Existing Keys: {existing_keys[:3]}{'...' if len(existing_keys) > 3 else ''}")
            else:
                print(f"  ⚠️  No server_account records found")
                default_accountmanagement = {'requirements': {'contract_duration': None, 'min_broker_balance': None}}
                with open(DEFAULT_ACCOUNTMANAGEMENT, 'w', encoding='utf-8') as f:
                    json.dump(default_accountmanagement, f, indent=2)
        else:
            print(f"  ⚠️  Failed to fetch server account management: {server_result.get('message')}")
            default_accountmanagement = {'requirements': {'contract_duration': None, 'min_broker_balance': None}}
        
        # Step 5: Prepare Output Directory for Insiders Data
        print(f"\n📁 [5/7] Preparing Output Directory for Insiders Data...")
        os.makedirs(os.path.dirname(FETCHED_INVESTORS), exist_ok=True)
        print(f"  ✅ Directory ready: {os.path.dirname(FETCHED_INVESTORS)}")
        
        # Step 6: Count records with empty accountmanagement (for reporting)
        print(f"\n📊 [6/7] Analyzing Account Management Data...")
        
        empty_acct_count = 0
        acct_check_query = """
            SELECT COUNT(*) as empty_count 
            FROM insiders 
            WHERE accountmanagement IS NULL 
               OR accountmanagement = '' 
               OR TRIM(accountmanagement) = ''
               OR accountmanagement = '{}'
               OR accountmanagement = '[]'
               OR accountmanagement = 'null'
        """
        acct_check_result = db.execute_query(acct_check_query)
        if acct_check_result.get('status') == 'success':
            acct_rows = acct_check_result.get('results', [])
            if acct_rows and len(acct_rows) > 0:
                empty_acct_count = int(acct_rows[0].get('empty_count') or 0)
        
        print(f"  📈 Users with Empty AccountManagement: {empty_acct_count:,} / {total_rows:,} ({empty_acct_count/total_rows*100:.1f}%)")
        
        if empty_acct_count > 0 and default_accountmanagement:
            print(f"  🔧 Will fill {empty_acct_count:,} users with default account management data")
            # Show requirements that will be filled
            reqs = default_accountmanagement.get('requirements', {})
            if reqs:
                print(f"  📋 Default Requirements to fill:")
                print(f"     - contract_duration: {reqs.get('contract_duration')} days")
                print(f"     - min_broker_balance: ${reqs.get('min_broker_balance')}")
        elif empty_acct_count > 0 and not default_accountmanagement:
            print(f"  ⚠️  No default account management available - will leave empty")
        
        # Step 7: Stream Insiders Data with JSON Repair and AccountManagement Filling
        print(f"\n📥 [7/7] Streaming Insiders Records to File (with JSON repair & account management fill)...")
        print("-"*70)
        
        start_time = datetime.now()
        bytes_written = 0
        current_batch = 0
        json_repaired_count = 0
        accountmanagement_filled_count = 0
        
        # Build column list for SELECT query if needed
        select_clause = "*"
        if columns:
            # Escape column names with backticks to handle reserved words
            select_clause = ", ".join([f"`{col}`" for col in columns])
        
        with open(FETCHED_INVESTORS, 'w', encoding='utf-8') as f:
            f.write('{\n')
            first_record = True
            offset = 0
            
            while offset < total_rows:
                current_batch += 1
                batch_start = datetime.now()
                
                # Fetch batch with explicit column list
                query = f"SELECT {select_clause} FROM insiders LIMIT {batch_size} OFFSET {offset}"
                result = db.execute_query(query)
                
                if result.get('status') != 'success':
                    print(f"\n   QUERY ERROR at batch {current_batch}: {result.get('message')}")
                    break
                    
                rows = result.get('results', [])
                if not rows:
                    print(f"\n  ⚠️  No rows returned at offset {offset:,}. Stopping.")
                    break
                
                # Write batch to file with pretty formatting
                batch_bytes = 0
                for row in rows:
                    # Use id if available, otherwise fallback to offset
                    record_id = str(row.get('id') or row.get('ID') or f"record_{offset}")
                    
                    if not first_record:
                        f.write(',\n')
                    
                    # Clean the row data by repairing JSON fields AND filling empty accountmanagement
                    cleaned_row = clean_record(row, default_accountmanagement)
                    
                    # Track if accountmanagement was filled
                    if cleaned_row.get('_accountmanagement_filled'):
                        accountmanagement_filled_count += 1
                        # Remove tracking fields if you don't want them in final output
                        # Uncomment the next lines to exclude tracking fields
                        # cleaned_row.pop('_accountmanagement_filled', None)
                        # cleaned_row.pop('_filled_at', None)
                    
                    # Additional type conversions
                    for key, value in cleaned_row.items():
                        if value is None:
                            cleaned_row[key] = None
                        elif isinstance(value, (datetime, date)):
                            cleaned_row[key] = value.isoformat()
                        elif isinstance(value, Decimal):
                            cleaned_row[key] = float(value)
                    
                    # Track if any JSON was repaired in this row
                    for key, value in cleaned_row.items():
                        if isinstance(value, (dict, list)) and key in row and isinstance(row[key], str):
                            json_repaired_count += 1
                    
                    # Format each record with indentation for readability
                    json_str = json.dumps(cleaned_row, default=str, indent=2)
                    # Indent the entire JSON object to align with the key
                    lines = json_str.split('\n')
                    indented_lines = ['    ' + line for line in lines]
                    formatted_json = '\n'.join(indented_lines)
                    
                    line = f'  "{record_id}": {formatted_json}'
                    f.write(line)
                    
                    batch_bytes += len(line.encode('utf-8'))
                    first_record = False
                
                offset += len(rows)
                bytes_written += batch_bytes
                
                # Batch progress
                batch_time = (datetime.now() - batch_start).total_seconds()
                records_per_sec = len(rows) / batch_time if batch_time > 0 else 0
                
                # Progress bar
                progress = (offset / total_rows) * 100
                bar_length = 30
                filled = int(bar_length * offset // total_rows)
                bar = '█' * filled + '░' * (bar_length - filled)
                
                print(f"  Batch {current_batch:>3}/{total_batches:<3} [{bar}] {progress:5.1f}% | "
                      f"Records: {offset:>{len(str(total_rows))},}/{total_rows:,} | "
                      f"Filled: {accountmanagement_filled_count:,} | "
                      f"Speed: {records_per_sec:>6,.0f} rec/s | "
                      f"Size: {bytes_written/1024:>8,.1f} KB")
            
            f.write('\n}')
        
        # Final Summary
        elapsed_time = (datetime.now() - start_time).total_seconds()
        avg_speed = offset / elapsed_time if elapsed_time > 0 else 0
        
        print("-"*70)
        print(f"\n📋 EXPORT SUMMARY")
        print("="*70)
        print(f"  ✅ Status           : SUCCESS")
        print(f"  📊 Records Exported : {offset:,} / {total_rows:,}")
        print(f"  📦 Batches Used     : {current_batch}")
        print(f"  📋 Schema Columns   : {len(columns) if columns else 'Dynamic (SELECT *)'}")
        print(f"  🔧 JSON Repairs     : {json_repaired_count} fields repaired")
        print(f"  🔄 Account Mgmt Filled: {accountmanagement_filled_count:,} users")
        print(f"     - Empty Users Found: {empty_acct_count:,}")
        print(f"     - Successfully Filled: {accountmanagement_filled_count:,}")
        if empty_acct_count > 0 and accountmanagement_filled_count < empty_acct_count:
            print(f"     ⚠️  Warning: {empty_acct_count - accountmanagement_filled_count} users could not be filled")
        print(f"  💾 File Size        : {bytes_written/1024:,.1f} KB ({bytes_written/1048576:.2f} MB)")
        print(f"  ⏱️  Total Time       : {elapsed_time:.1f} seconds")
        print(f"  ⚡ Average Speed    : {avg_speed:,.0f} records/second")
        print(f"  📁 Output File      : {FETCHED_INVESTORS}")
        print("="*70)
        print(f"\n📋 ADDITIONAL EXPORTS")
        print("="*70)
        print(f"  🚫 Suspended Accounts: {SUSPENDED_ACCOUNTS}")
        print(f"     Total Suspended   : {len(suspended_users)} accounts")
        if len(suspended_users) > 0:
            print(f"     Status Check      : Verified exact matches only (case-insensitive)")
        print(f"  ⚙️  Default Server Mgmt: {DEFAULT_ACCOUNTMANAGEMENT}")
        print(f"     Used to fill {accountmanagement_filled_count:,} empty user accountmanagement fields")
        
        # Show requirements that were added
        if default_accountmanagement and isinstance(default_accountmanagement, dict):
            reqs = default_accountmanagement.get('requirements', {})
            if reqs:
                print(f"     📋 Requirements added to default:")
                print(f"        - contract_duration: {reqs.get('contract_duration')} days")
                print(f"        - min_broker_balance: ${reqs.get('min_broker_balance')}")
        
        print(f"  🕐 Completion Time  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"   CRITICAL ERROR")
        print(f"{'='*70}")
        print(f"  Error Type : {type(e).__name__}")
        print(f"  Message    : {str(e)}")
        print(f"{'='*70}")
        
        import traceback
        print(f"\n  📜 Full Traceback:")
        traceback.print_exc()
        
    finally:
        db.shutdown()
        print(f"\n🔒 Database connection closed.")
        
def update_insiders_streaming(batch_size=5000):
    """Stream updates from UPDATED_INVESTORS JSON to database without holding all in memory"""
    
    print("\n" + "="*70)
    print(f"  INSIDERS DATA UPDATE - STREAMING UPDATE")
    print("="*70)
    print(f"  Start Time  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Batch Size  : {batch_size:,} records per batch")
    print("-"*70)
    
    try:
        # Step 1: Check if update file exists
        print("\n📁 [1/5] Checking Update File...")
        if not os.path.exists(UPDATED_INVESTORS):
            print(f"   Update file not found: {UPDATED_INVESTORS}")
            return
        
        file_size = os.path.getsize(UPDATED_INVESTORS)
        print(f"  ✅ Update file found: {UPDATED_INVESTORS}")
        print(f"  📦 File Size: {file_size/1024:,.1f} KB ({file_size/1048576:.2f} MB)")
        
        # Step 2: Test Database Connection
        print("\n📡 [2/5] Testing Database Connection...")
        test_query = "SELECT id FROM insiders LIMIT 1"
        test_result = db.execute_query(test_query)
        
        if test_result.get('status') != 'success':
            print(f"   Connection FAILED: {test_result.get('message')}")
            return
        print(f"  ✅ Connection SUCCESSFUL")
        
        # Step 3: Get existing IDs for validation
        print("\n🔍 [3/5] Fetching Existing Record IDs...")
        existing_ids_query = "SELECT id FROM insiders"
        existing_result = db.execute_query(existing_ids_query)
        
        existing_ids = set()
        if existing_result.get('status') == 'success':
            for row in existing_result.get('results', []):
                existing_ids.add(str(row.get('id')))
        
        print(f"  📊 Existing Records in DB: {len(existing_ids):,}")
        
        # Step 4: Parse JSON and identify records to process
        print(f"\n📖 [4/5] Reading Update File...")
        
        # Read entire JSON
        with open(UPDATED_INVESTORS, 'r', encoding='utf-8') as f:
            investors_data = json.load(f)
        
        total_investors = len(investors_data)
        print(f"  📊 Total Investors in File: {total_investors:,}")
        
        # Identify which records need updating (exist in DB) and which to remove
        investors_to_update = {}
        investors_to_remove = []  # Non-existing records
        successfully_updated_ids = []  # Will be populated after successful updates
        
        for investor_id, investor_data in investors_data.items():
            if investor_id in existing_ids:
                investors_to_update[investor_id] = investor_data
            else:
                investors_to_remove.append(investor_id)
        
        print(f"  ✅ Records to Update: {len(investors_to_update):,}")
        print(f"  🗑️  Records Not in DB: {len(investors_to_remove):,}")
        
        if not investors_to_update and not investors_to_remove:
            print(f"  ⚠️  File is empty. Nothing to process.")
            return
        
        # Step 5: Update Database in Batches
        if investors_to_update:
            print(f"\n📤 [5/5] Updating Database Records...")
            print("-"*70)
            
            start_time = datetime.now()
            updated_count = 0
            failed_count = 0
            current_batch = 0
            
            investor_ids = list(investors_to_update.keys())
            total_batches = (len(investor_ids) + batch_size - 1) // batch_size
            
            for i in range(0, len(investor_ids), batch_size):
                current_batch += 1
                batch_start = datetime.now()
                
                batch_ids = investor_ids[i:i + batch_size]
                batch_updates = 0
                batch_failed = 0
                
                for investor_id in batch_ids:
                    investor = investors_to_update[investor_id]
                    
                    # Build UPDATE query dynamically based on available fields
                    update_parts = []
                    
                    # Map JSON fields to database columns with proper escaping
                    field_mappings = {
                        'server': 'server',
                        'login': 'login',
                        'password': 'password',
                        'application_status': 'application_status',
                        'broker_balance': 'broker_balance',
                        'profitandloss': 'profitandloss',
                        'contract_days_left': 'contract_days_left',
                        'execution_start_date': 'execution_start_date',
                    }
                    
                    # Handle special fields that need JSON encoding
                    json_fields = {
                        'trades': 'trades',
                        'unauthorized_actions': 'unauthorized_actions',
                    }
                    
                    # Process direct mappings
                    for json_field, db_column in field_mappings.items():
                        if json_field in investor and investor[json_field] is not None:
                            value = investor[json_field]
                            
                            # Escape single quotes and format value based on type
                            if isinstance(value, (int, float)):
                                # Numeric values - no quotes needed
                                escaped_value = str(value)
                            else:
                                # String values - wrap in quotes and escape single quotes
                                escaped_value = "'" + str(value).replace("'", "\\'") + "'"
                            
                            update_parts.append(f"`{db_column}` = {escaped_value}")
                    
                    # Process JSON fields (convert to JSON strings)
                    for json_field, db_column in json_fields.items():
                        if json_field in investor and investor[json_field] is not None:
                            # Convert to JSON string and escape properly
                            json_str = json.dumps(investor[json_field])
                            escaped_json = json_str.replace("'", "\\'")
                            update_parts.append(f"`{db_column}` = '{escaped_json}'")
                    
                    # Skip if no fields to update
                    if not update_parts:
                        continue
                    
                    # Build complete query with embedded values
                    set_clause = ", ".join(update_parts)
                    query = f"UPDATE insiders SET {set_clause} WHERE id = {int(investor_id)}"
                    
                    # Execute the query
                    result = db.execute_query(query)
                    
                    if result.get('status') == 'success':
                        batch_updates += 1
                        updated_count += 1
                        successfully_updated_ids.append(investor_id)  # Track successful updates
                    else:
                        batch_failed += 1
                        failed_count += 1
                
                # Batch progress
                batch_time = (datetime.now() - batch_start).total_seconds()
                records_per_sec = len(batch_ids) / batch_time if batch_time > 0 else 0
                
                # Progress bar
                progress = ((i + len(batch_ids)) / len(investor_ids)) * 100
                bar_length = 30
                filled = int(bar_length * (i + len(batch_ids)) // len(investor_ids))
                bar = '█' * filled + '░' * (bar_length - filled)
                
                print(f"  Batch {current_batch:>3}/{total_batches:<3} [{bar}] {progress:5.1f}% | "
                      f"Updated: {batch_updates:>4} | Failed: {batch_failed:>3} | "
                      f"Speed: {records_per_sec:>6,.0f} rec/s | "
                      f"Total: {updated_count:>{len(str(len(investor_ids)))},}/{len(investor_ids):,}")
            
            # Update timing
            elapsed_time = (datetime.now() - start_time).total_seconds()
            avg_speed = updated_count / elapsed_time if elapsed_time > 0 else 0
        else:
            elapsed_time = 0
            avg_speed = 0
            updated_count = 0
            failed_count = 0
        
        # Step 6: Clean JSON file - Remove ALL processed records
        print(f"\n🧹 [6/6] Cleaning JSON File...")
        
        # Combine all IDs to remove: non-existing + successfully updated
        all_ids_to_remove = investors_to_remove + successfully_updated_ids
        total_removed = len(all_ids_to_remove)
        
        if all_ids_to_remove:
            # Remove all processed records from the dictionary
            for investor_id in all_ids_to_remove:
                if investor_id in investors_data:
                    del investors_data[investor_id]
            
            # Write the cleaned data back to the file
            with open(UPDATED_INVESTORS, 'w', encoding='utf-8') as f:
                json.dump(investors_data, f, indent=2, ensure_ascii=False)
            
            print(f"  ✅ Cleaned JSON file successfully")
            print(f"  🗑️  Total Removed: {total_removed:,}")
            print(f"     - Non-existing records: {len(investors_to_remove):,}")
            print(f"     - Successfully updated: {len(successfully_updated_ids):,}")
            print(f"  📊 Remaining in File: {len(investors_data):,}")
            
            # Recalculate file size
            new_file_size = os.path.getsize(UPDATED_INVESTORS)
            print(f"  📦 New File Size: {new_file_size/1024:,.1f} KB ({new_file_size/1048576:.2f} MB)")
        else:
            print(f"  ℹ️  No records to remove from file")
        
        # Final Summary
        print("-"*70)
        print(f"\n📋 UPDATE SUMMARY")
        print("="*70)
        print(f"  ✅ Status              : {'SUCCESS' if failed_count == 0 else 'COMPLETED WITH ERRORS'}")
        print(f"  📊 Original in File    : {total_investors:,}")
        print(f"  🗑️  Total Removed       : {total_removed:,}")
        print(f"     - Non-existing      : {len(investors_to_remove):,}")
        print(f"     - Successfully Updated: {len(successfully_updated_ids):,}")
        print(f"  Failed Updates      : {failed_count:,} (kept in file)")
        print(f"  📊 Final in File       : {len(investors_data):,}")
        print(f"  ⏱️  Total Time          : {elapsed_time:.1f} seconds")
        print(f"  ⚡ Average Speed       : {avg_speed:,.0f} records/second")
        print(f"  📁 Source File         : {UPDATED_INVESTORS}")
        print(f"  🕐 Completion Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show remaining failed IDs if any
        if failed_count > 0:
            failed_ids = [id for id in investors_to_update.keys() if id not in successfully_updated_ids]
            print(f"\n⚠️  Failed Updates (kept in file, first 10): {failed_ids[:10]}")
            if len(failed_ids) > 10:
                print(f"  ... and {len(failed_ids) - 10} more")
        
        print("="*70)
        
    except json.JSONDecodeError as e:
        print(f"\n{'='*70}")
        print(f"   JSON PARSE ERROR")
        print(f"{'='*70}")
        print(f"  Error: {str(e)}")
        print(f"  File : {UPDATED_INVESTORS}")
        print(f"{'='*70}")
        
    except Exception as e:
        print(f"\n{'='*70}")
        print(f"   CRITICAL ERROR")
        print(f"{'='*70}")
        print(f"  Error Type : {type(e).__name__}")
        print(f"  Message    : {str(e)}")
        print(f"{'='*70}")
        
        import traceback
        print(f"\n  📜 Full Traceback:")
        traceback.print_exc()
        
    finally:
        db.shutdown()
        print(f"\n🔒 Database connection closed.")

def create_investor_mt5_files(inv_id=None):
    """
    Creates MT5 terminal folders for investors by copying from DEFAULT_MT5_PATH
    
    Args:
        inv_id: Optional - specific investor ID to process. If None, processes all investors.
    
    Logic:
        1. If user is suspended/blacklisted -> IGNORE completely (no folder creation/deletion)
        2. If folder doesn't exist and user NOT suspended -> CREATE new folder
        3. If folder exists and user NOT suspended -> ENSURE TERMINAL_PATH is set in record
        4. If folder exists and user IS suspended -> DELETE folder immediately (storage cleanup)
    
    For each investor in the fetched investors file (FETCHED_INVESTORS), this function:
        1. Checks if investor is suspended (by checking suspended_accounts.json)
        2. Based on suspension status and folder existence, decides action:
           - Suspended users: Always skip creation, delete existing folders
           - Non-suspended users: Create if missing, ensure TERMINAL_PATH exists if folder exists
        3. Updates TERMINAL_PATH and application_status for newly created folders
        4. Always ensures TERMINAL_PATH is set for clean users with existing folders
    
    Returns:
        tuple: (created_count, deleted_count, skipped_count, error_count)
    """
    
    print(f"\n{'='*60}")
    print(f"📦 CREATE/MAINTAIN MT5 FILES")
    if inv_id:
        print(f"   Target: {inv_id}")
    print(f"{'='*60}")
    
    # Check if source MT5 folder exists
    if not os.path.exists(DEFAULT_MT5_PATH) or not os.path.isdir(DEFAULT_MT5_PATH):
        print(f" Source MT5 folder not found: {DEFAULT_MT5_PATH}")
        return (0, 0, 0, 1)
    
    # Check if fetched investors file exists
    if not os.path.exists(FETCHED_INVESTORS):
        print(f" Fetched investors file not found: {FETCHED_INVESTORS}")
        return (0, 0, 0, 1)
    
    # Load suspended accounts
    suspended_ids = set()
    suspended_data = {}
    if os.path.exists(SUSPENDED_ACCOUNTS):
        try:
            with open(SUSPENDED_ACCOUNTS, 'r', encoding='utf-8') as f:
                suspended_json = json.load(f)
                suspended_accounts = suspended_json.get('suspended_accounts', [])
                for account in suspended_accounts:
                    account_id = str(account.get('id')) if account.get('id') else None
                    if account_id:
                        suspended_ids.add(account_id)
                        suspended_data[account_id] = account
            if suspended_ids:
                print(f"🚫 Loaded {len(suspended_ids)} suspended/blacklisted accounts")
        except Exception as e:
            print(f"⚠️ Error loading suspended accounts: {e}")
    else:
        print(f"ℹ️ No suspended accounts file found - all users will be processed normally")
    
    # Load fetched investors data
    try:
        with open(FETCHED_INVESTORS, 'r', encoding='utf-8') as f:
            investors_data = json.load(f)
        print(f"📋 Loaded {len(investors_data)} investors from file")
    except Exception as e:
        print(f" Error loading investors: {e}")
        return (0, 0, 0, 1)
    
    # Filter investors if inv_id is specified
    if inv_id:
        inv_id_str = str(inv_id)
        if inv_id_str not in investors_data:
            print(f" Investor {inv_id} not found in data")
            return (0, 0, 0, 1)
        investors_to_process = {inv_id_str: investors_data[inv_id_str]}
    else:
        investors_to_process = investors_data
    
    # Ensure MT5 destination directory exists
    os.makedirs(MT5_DESTINATION_PATH, exist_ok=True)
    
    # Statistics
    created = 0
    deleted = 0
    skipped = 0
    errors = 0
    suspended_skipped = 0
    path_updates = 0  # Track path updates for existing users
    investors_modified = False
    
    for investor_id, investor_data in investors_to_process.items():
        investor_id_str = str(investor_id)
        
        # Extract broker and id
        broker = investor_data.get('broker', '').strip()
        investor_id_value = investor_data.get('id', '').strip()
        
        if not broker or not investor_id_value:
            print(f"⚠️ Investor {investor_id} missing broker or id, skipping")
            skipped += 1
            continue
        
        # Check if user is suspended/blacklisted
        is_suspended = investor_id_str in suspended_ids
        
        # Create target paths
        folder_name = f"MetaTrader 5 {broker} {investor_id_value}"
        target_folder = os.path.join(MT5_DESTINATION_PATH, folder_name)
        target_exe = os.path.join(target_folder, "terminal64.exe")
        normalized_path = target_exe.replace('\\', '\\')
        
        folder_exists = os.path.exists(target_folder)
        
        # LOGIC: Handle based on suspension status and folder existence
        if is_suspended:
            # SUSPENDED USER: Delete folder if exists, always skip creation
            if folder_exists:
                try:
                    print(f"🗑️  SUSPENDED ID:{investor_id} ({broker} {investor_id_value}) - Deleting folder...")
                    shutil.rmtree(target_folder, ignore_errors=True)
                    deleted += 1
                    print(f"   ✅ Folder deleted (suspended account cleanup)")
                    
                    # Update investor data to reflect deletion
                    if 'TERMINAL_PATH' in investors_data[investor_id]:
                        investors_data[investor_id]['TERMINAL_PATH'] = ''
                        investors_modified = True
                except Exception as e:
                    errors += 1
                    print(f"    Failed to delete folder: {str(e)[:100]}")
            else:
                print(f"🚫 SUSPENDED ID:{investor_id} ({broker} {investor_id_value}) - No folder exists, skipping")
                suspended_skipped += 1
            continue  # Skip to next investor (no creation for suspended users)
        
        # NON-SUSPENDED USER (CLEAN USER): Handle based on folder existence
        if folder_exists:
            # Existing user with folder - ALWAYS ensure TERMINAL_PATH is set correctly
            current_path = investor_data.get('TERMINAL_PATH', '')
            
            # Check if path needs to be set or updated
            if not current_path or current_path != normalized_path:
                # Path is missing or incorrect - update it
                investors_data[investor_id]['TERMINAL_PATH'] = normalized_path
                investors_modified = True
                path_updates += 1
                
                if not current_path:
                    print(f"🔧 ID:{investor_id} → TERMINAL_PATH was MISSING, now set to: {normalized_path[:80]}...")
                else:
                    print(f"🔧 ID:{investor_id} → TERMINAL_PATH updated (was incorrect)")
            else:
                # Path exists and is correct
                print(f"✓ ID:{investor_id} → TERMINAL_PATH verified, folder exists")
            
            skipped += 1
            continue
        
        # Create new folder for non-suspended user (folder doesn't exist)
        print(f"🆕 ID:{investor_id} ({broker} {investor_id_value}) - Creating new folder...")
        
        try:
            # Copy the entire MT5 folder
            shutil.copytree(DEFAULT_MT5_PATH, target_folder, 
                          ignore_dangling_symlinks=True,
                          ignore=shutil.ignore_patterns('*.lock', '*.log'))
            
            # Update investor data
            investors_data[investor_id]['TERMINAL_PATH'] = normalized_path
            investors_data[investor_id]['application_status'] = 'just-joined'
            investors_modified = True
            created += 1
            print(f"   ✅ Folder created and application_status set to 'just-joined'")
            print(f"   📍 TERMINAL_PATH: {normalized_path[:80]}...")
            
        except Exception as e:
            errors += 1
            print(f"    Failed to copy folder: {str(e)[:100]}")
            # Clean up partial copy if exists
            if os.path.exists(target_folder):
                shutil.rmtree(target_folder, ignore_errors=True)
    
    # Save updated data
    if investors_modified:
        try:
            with open(FETCHED_INVESTORS, 'w', encoding='utf-8') as f:
                json.dump(investors_data, f, indent=2)
            print(f"\n💾 Saved investor data to {FETCHED_INVESTORS}")
        except Exception as e:
            print(f" Failed to save investor data: {e}")
    
    # Final Summary
    print(f"\n{'='*60}")
    print(f"📊 SUMMARY")
    print(f"{'='*60}")
    print(f"  ✅ Created folders      : {created}")
    print(f"  🗑️  Deleted folders      : {deleted}")
    print(f"  ⏭️  Skipped (existing)   : {skipped}")
    print(f"  🔧 Path updates         : {path_updates}")
    print(f"  🚫 Suspended (ignored)  : {suspended_skipped}")
    print(f"   Errors               : {errors}")
    print(f"{'='*60}")
    
    return (created, deleted, skipped, errors)

def get_investors_balance():
    """
    Get account balance for investors by initializing MT5 and logging in.
    
    Properly distinguishes between:
    - Already logged in (MT5 already running with this investor's account)
    - Fresh login (MT5 initialized and logged in now)
    - Login failed (could not authenticate)
    
    Only processes investors with 'just-joined' status. On success, updates:
    - broker_balance with current account balance
    - application_status to 'just-joined-and-valid_credentials'
    
    Then COPIES investors with 'just-joined-and-valid_credentials' status to updated_investors.json
    (without removing them from fetched_investors.json)
    
    Returns:
        bool: True if at least one investor balance was updated, False otherwise
    """
    
    print(f"\n{'='*60}")
    print(f"💰 GET BALANCES")
    print(f"{'='*60}")
    
    # Check if fetched investors file exists
    if not os.path.exists(FETCHED_INVESTORS):
        print(f"Fetched investors file not found: {FETCHED_INVESTORS}")
        return False
    
    # Load fetched investors data
    try:
        with open(FETCHED_INVESTORS, 'r', encoding='utf-8') as f:
            investors_data = json.load(f)
        print(f"📋 Loaded {len(investors_data)} investors from fetched_investors.json")
    except Exception as e:
        print(f"Error loading investors: {e}")
        return False
    
    # Load existing updated investors data
    updated_investors_data = {}
    if os.path.exists(UPDATED_INVESTORS):
        try:
            with open(UPDATED_INVESTORS, 'r', encoding='utf-8') as f:
                updated_investors_data = json.load(f)
            print(f"📋 Loaded {len(updated_investors_data)} investors from updated_investors.json")
        except Exception as e:
            print(f"⚠️ Warning: Could not load updated_investors.json: {e}")
    
    # Statistics
    processed = 0
    updated = 0
    skipped = 0
    errors = 0
    already_logged_in_count = 0
    fresh_login_count = 0
    failed_login_count = 0
    
    investors_modified = False
    
    # Define valid just-joined statuses
    just_joined_statuses = ['just-joined', 'just_joined', 'just joined', 'justjoined']
    
    for investor_id, investor_data in investors_data.items():
        app_status = investor_data.get('application_status', '').strip().lower()
        
        # Skip if not just-joined
        if app_status not in just_joined_statuses:
            if app_status:
                print(f"⏭️ ID:{investor_id} → Status: {app_status}")
            continue
        
        # Extract credentials
        login_id = investor_data.get('login', '') or investor_data.get('LOGIN_ID', '')
        password = investor_data.get('password', '') or investor_data.get('PASSWORD', '')
        server = investor_data.get('server', '') or investor_data.get('SERVER', '')
        terminal_path = investor_data.get('TERMINAL_PATH', '')
        
        if not all([login_id, password, server, terminal_path]):
            print(f"⚠️ ID:{investor_id} → Missing credentials")
            skipped += 1
            continue
        
        # Validate login_id
        try:
            login_id_int = int(login_id)
        except (ValueError, TypeError):
            print(f"⚠️ ID:{investor_id} → Invalid LOGIN_ID: {login_id}")
            skipped += 1
            continue
        
        # Check terminal exists
        if not os.path.exists(terminal_path):
            print(f"ID:{investor_id} → Terminal not found at: {terminal_path}")
            errors += 1
            continue
        
        print(f"\n🔄 ID:{investor_id} (Login:{login_id_int}) - Processing...")
        
        # Step 1: Check if MT5 is already running and logged in with this account
        mt5_already_running = False
        already_logged_in_account = None
        
        try:
            # Try to initialize without path first (use existing running instance)
            if mt5.initialize():
                account_info = mt5.account_info()
                if account_info is not None:
                    already_logged_in_account = account_info.login
                    if account_info.login == login_id_int:
                        # CASE 1: Already logged in with this exact account
                        print(f"   ✅ ALREADY LOGGED IN STATUS: Investor {login_id_int} is already logged into MT5")
                        print(f"      → No initialization or login needed, using existing session")
                        
                        # Get account info directly
                        balance = account_info.balance
                        currency = account_info.currency
                        
                        # Update broker_balance
                        balance_str = f"{balance:.2f}"
                        current_balance = investor_data.get('broker_balance', 'NULL')
                        
                        if current_balance != balance_str:
                            investor_data['broker_balance'] = balance_str
                            print(f"   ✅ Balance (already logged in): {currency} {balance:,.2f}")
                            updated += 1
                        
                        # Update status
                        old_status = investor_data.get('application_status', 'unknown')
                        investor_data['application_status'] = 'just-joined-and-valid_credentials'
                        investors_modified = True
                        print(f"   📝 Status: {old_status} → just-joined-and-valid_credentials")
                        
                        processed += 1
                        already_logged_in_count += 1
                        mt5.shutdown()
                        continue
                    else:
                        # Different account is logged in
                        print(f"   ℹ️ WARNING: Different investor {already_logged_in_account} is currently logged into MT5")
                        mt5.shutdown()
                else:
                    # MT5 initialized but no account info (not logged in)
                    print(f"   ℹ️ MT5 is running but no account is logged in")
                    mt5.shutdown()
            else:
                # MT5 not running, need fresh initialization
                print(f"   ℹ️ MT5 is not running - will need fresh initialization")
        except Exception as e:
            print(f"   ⚠️ Could not check MT5 status: {e}")
        
        # Step 2: If not already logged in, try fresh login
        if already_logged_in_account != login_id_int:
            print(f"   🔐 FRESH LOGIN ATTEMPT: Investor {login_id_int} is NOT already logged in")
            print(f"      → Will initialize MT5 and login with credentials")
            
            try:
                # Shutdown any existing connection
                if mt5.terminal_info() is not None:
                    mt5.shutdown()
                
                # Initialize MT5 with specific terminal path
                print(f"      → Initializing MT5 at: {terminal_path}")
                if not mt5.initialize(path=terminal_path, timeout=60000):
                    error_msg = mt5.last_error()
                    print(f"   INITIALIZATION FAILED: {error_msg}")
                    print(f"      → COULD NOT LOGIN - MT5 failed to start")
                    failed_login_count += 1
                    errors += 1
                    continue
                
                print(f"      → MT5 initialized successfully")
                
                # Attempt login
                print(f"      → Attempting login with credentials...")
                if not mt5.login(login_id_int, password=password, server=server):
                    error_msg = mt5.last_error()
                    print(f"   LOGIN FAILED: {error_msg}")
                    print(f"      → Investor {login_id_int} could not authenticate")
                    mt5.shutdown()
                    failed_login_count += 1
                    errors += 1
                    continue
                
                # Successful fresh login
                print(f"   ✅ FRESH LOGIN SUCCESS: Successfully logged in as {login_id_int}")
                fresh_login_count += 1
                
                # Get account info
                account_info = mt5.account_info()
                if account_info is None:
                    print(f"   No account info after login")
                    mt5.shutdown()
                    errors += 1
                    continue
                
                # Get balance
                balance = account_info.balance
                currency = account_info.currency
                
                # Update broker_balance
                balance_str = f"{balance:.2f}"
                current_balance = investor_data.get('broker_balance', 'NULL')
                
                if current_balance != balance_str:
                    investor_data['broker_balance'] = balance_str
                    print(f"   ✅ Balance (fresh login): {currency} {balance:,.2f}")
                    updated += 1
                else:
                    print(f"   ℹ️ Balance unchanged: {currency} {balance:,.2f}")
                
                # Update status
                old_status = investor_data.get('application_status', 'unknown')
                investor_data['application_status'] = 'just-joined-and-valid_credentials'
                investors_modified = True
                print(f"   📝 Status: {old_status} → just-joined-and-valid_credentials")
                
                processed += 1
                
                # Cleanup after fresh login
                mt5.shutdown()
                print(f"      → MT5 session closed")
                
            except Exception as e:
                print(f"   ERROR during fresh login: {str(e)[:100]}")
                failed_login_count += 1
                errors += 1
                try:
                    mt5.shutdown()
                except:
                    pass
    
    # Save updated fetched_investors.json (with updated statuses and balances)
    if investors_modified:
        try:
            backup_path = FETCHED_INVESTORS.replace('.json', '_backup.json')
            if not os.path.exists(backup_path):
                import shutil
                shutil.copy2(FETCHED_INVESTORS, backup_path)
                print(f"\n📦 Created backup: {backup_path}")
            
            with open(FETCHED_INVESTORS, 'w', encoding='utf-8') as f:
                json.dump(investors_data, f, indent=2)
            print(f"💾 Saved updated data to {FETCHED_INVESTORS}")
        except Exception as e:
            print(f"Save failed: {e}")
    
    # ============================================================
    # COPY investors with 'just-joined-and-valid_credentials' to updated_investors.json
    # This runs AFTER all processing and updating is done
    # ============================================================
    print(f"\n{'='*60}")
    print(f"📋 COPYING VALID CREDENTIALS INVESTORS TO UPDATED_INVESTORS.JSON")
    print(f"{'='*60}")
    
    copied_count = 0
    for investor_id, investor_data in investors_data.items():
        app_status = investor_data.get('application_status', '').strip().lower()
        
        # Check if investor has valid credentials status
        if app_status == 'just-joined-and-valid_credentials':
            # Copy to updated_investors.json (overwrite if exists)
            updated_investors_data[investor_id] = investor_data.copy()
            copied_count += 1
            print(f"✅ COPIED ID:{investor_id} to updated_investors.json")
    
    # Save updated_investors.json
    if copied_count > 0:
        try:
            # Create backup of updated_investors.json if it exists
            if os.path.exists(UPDATED_INVESTORS):
                backup_updated_path = UPDATED_INVESTORS.replace('.json', '_backup.json')
                if not os.path.exists(backup_updated_path):
                    import shutil
                    shutil.copy2(UPDATED_INVESTORS, backup_updated_path)
                    print(f"📦 Created backup of updated_investors.json: {backup_updated_path}")
            
            with open(UPDATED_INVESTORS, 'w', encoding='utf-8') as f:
                json.dump(updated_investors_data, f, indent=2)
            print(f"💾 Saved {len(updated_investors_data)} investors to {UPDATED_INVESTORS}")
            print(f"📋 New investors copied: {copied_count}")
        except Exception as e:
            print(f"Failed to save updated_investors.json: {e}")
    else:
        print(f"⚠️ No investors with 'just-joined-and-valid_credentials' status found to copy")
    
    
    # Final cleanup
    try:
        if mt5.terminal_info() is not None:
            mt5.shutdown()
    except:
        pass
    
    return updated > 0

def process_single_investor_(inv_id):
    """
    WORKER FUNCTION: Only creates MT5 folders if they don't exist
    NO MT5 INITIALIZATION OR LOGIN
    Takes investor ID directly, not folder path
    
    Args:
        inv_id: Investor ID string
        
    Returns:
        dict: Statistics about the operation
    """
    
    account_stats = {
        "inv_id": inv_id, 
        "success": False,
        "folder_created": False,
        "folder_existed": False,
        "error": None
    }
    
    # Just call the folder creation function
    try:
        create_investor_mt5_files(inv_id=inv_id)
        
    except Exception as e:
        account_stats["error"] = str(e)
        print(f"Error for {inv_id}: {e}")
    
    return account_stats

def process_single_investor(inv_id):
    """
    WORKER FUNCTION: Only creates MT5 folders if they don't exist and executes
    other operations ONLY if within allowed time range.
    NO MT5 INITIALIZATION OR LOGIN
    Takes investor ID directly, not folder path
    
    Args:
        inv_id: Investor ID string
        
    Returns:
        dict: Statistics about the operation
    """
    
    account_stats = {
        "inv_id": inv_id, 
        "success": False,
        "folder_created": False,
        "folder_existed": False,
        "within_time_range": False,
        "execution_skipped": False,
        "error": None
    }
    
    # Check if we're allowed to work within current time range
    time_check_result = work_only_in_specific_timerange()
    
    if not time_check_result.get("should_work", False):
        print(f"⏰ Skipping operations for {inv_id} - outside allowed work time range")
        account_stats["execution_skipped"] = True
        account_stats["within_time_range"] = False
        account_stats["success"] = True  # Consider this as "successfully skipped"
        return account_stats
    
    # Within time range - proceed with operations
    account_stats["within_time_range"] = True
    
    try:
        # Execute the operations only if within time range
        fetch_insiders_streaming()
        create_investor_mt5_files(inv_id=inv_id)
        get_investors_balance()
        update_insiders_streaming()
        
        account_stats["success"] = True
        
    except Exception as e:
        account_stats["error"] = str(e)
        print(f"Error for {inv_id}: {e}")
    
    return account_stats

def place_orders_parallel():
    """
    ORCHESTRATOR: Processes all investors from fetched_investors.json
    No INV_PATH dependency - just uses the JSON file
    """
    # Check if fetched investors file exists
    if not os.path.exists(FETCHED_INVESTORS):
        print(f"Fetched investors file not found: {FETCHED_INVESTORS}")
        return False
    
    # Load investors from JSON
    try:
        with open(FETCHED_INVESTORS, 'r', encoding='utf-8') as f:
            investors_data = json.load(f)
        print(f"📋 Found {len(investors_data)} investors in fetched_investors.json")
    except Exception as e:
        print(f"Error loading investors: {e}")
        return False
    
    if not investors_data:
        print(" └─ 🔘 No investors found in fetched_investors.json")
        return False

    # Get list of investor IDs
    investor_ids = list(investors_data.keys())
    print(f" 📋 Processing investors: {investor_ids}")
    print(f" 🔧 Creating pool with {len(investor_ids)} processes...")
    
    # Use a process pool
    try:
        # Use multiprocessing with spawn context (more reliable on Windows)
        mp.set_start_method('spawn', force=True)
        
        with mp.Pool(processes=len(investor_ids)) as pool:
            results = pool.map(process_single_investor, investor_ids)
        
        # Print summary
        successful = sum(1 for r in results if r.get("success", False))
        created = sum(1 for r in results if r.get("folder_created", False))
        existed = sum(1 for r in results if r.get("folder_existed", False))
        
        print(f"\n{'='*60}")
        print(f"📊 SUMMARY:")
        print(f"   Total investors: {len(results)}")
        print(f"   Successful: {successful}")
        print(f"   New folders created: {created}")
        print(f"   Existing folders: {existed}")
        print(f"{'='*60}")
        
        return True
    except Exception as e:
        print(f"Error in parallel processing: {e}")
        # Fallback to sequential processing
        print("🔄 Falling back to sequential processing...")
        results = []
        for inv_id in investor_ids:
            result = process_single_investor(inv_id)
            results.append(result)
        
        successful = sum(1 for r in results if r.get("success", False))
        print(f"✅ Sequential: {successful}/{len(results)} successful")
        
        return True

def place_orders_parallel_():
    """
    ORCHESTRATOR: Processes all investors from fetched_investors.json
    Runs in a continuous loop until manually terminated
    """
    print(f"🚀 Starting Perpetual Trading Loop (using fetched_investors.json)...")
    
    while True:  # Run indefinitely until Ctrl+C
        try:
            # Check if fetched investors file exists
            if not os.path.exists(FETCHED_INVESTORS):
                print(f"⚠️  Fetched investors file not found: {FETCHED_INVESTORS}")
                print("   Retrying in 10 seconds...")
                time.sleep(10)
                continue
            
            # Load investors from JSON
            with open(FETCHED_INVESTORS, 'r', encoding='utf-8') as f:
                investors_data = json.load(f)
            
            if not investors_data:
                print(" └─ 🔘 No investors found in fetched_investors.json")
                print("   Retrying in 10 seconds...")
                time.sleep(10)
                continue
            
            investor_ids = list(investors_data.keys())
            print(f"\n--- Cycle Start: Processing {len(investor_ids)} investors ---")
            print(f"   Investors: {investor_ids}")
            
            # Use process pool to process all investors in parallel
            with mp.Pool(processes=len(investor_ids)) as pool:
                results = pool.map(process_single_investor, investor_ids)
            
            # Quick summary
            successful = sum(1 for r in results if r.get("success", False))
            print(f"--- Cycle Complete: {successful}/{len(results)} successful ---")
            
        except KeyboardInterrupt:
            print("\n🛑 Received shutdown signal. Exiting gracefully...")
            break
        except Exception as e:
            print(f" Critical Error in Orchestrator: {e}")
            print("   Retrying in 5 seconds...")
            time.sleep(5)
        
        
# Example usage
if __name__ == "__main__":
    place_orders_parallel()
