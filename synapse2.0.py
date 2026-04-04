import os
import MetaTrader5 as mt5
import pandas as pd
import mplfinance as mpf
from datetime import datetime
import json
from PIL import Image
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import traceback
import shutil
from datetime import datetime
import re
from pathlib import Path
import math
import pytz
import multiprocessing as mp
import multiprocessing
from pathlib import Path
import time
import random


INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"
UPDATED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\updated_investors.json"
INVESTOR_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
VERIFIED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\verified_investors.json"
ISSUES_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\issues_investors.json"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\synapse\synarex\default_accountmanagement.json"
DEFAULT_PATH = r"C:\xampp\htdocs\synapse\synarex"
BASE_ERROR_FOLDER = r"C:\xampp\htdocs\synapse\synarex\usersdata\debugs"
NORM_FILE_PATH = Path(DEFAULT_PATH) / "symbols_normalization.json"
ERROR_JSON_PATH = os.path.join(BASE_ERROR_FOLDER, "chart_errors.json")
TIMEFRAME_MAP = {
        "1m": mt5.TIMEFRAME_M1,
        "5m": mt5.TIMEFRAME_M5,
        "15m": mt5.TIMEFRAME_M15,
        "30m": mt5.TIMEFRAME_M30,
        "1h": mt5.TIMEFRAME_H1,
        "4h": mt5.TIMEFRAME_H4,
        "1d": mt5.TIMEFRAME_D1,
        "1w": mt5.TIMEFRAME_W1,
        "1mn": mt5.TIMEFRAME_MN1
    }
    

def load_investors_dictionary():
    BROKERS_JSON_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
    """Load brokers config from JSON file with error handling and fallback."""
    if not os.path.exists(BROKERS_JSON_PATH):
        print(f"CRITICAL: {BROKERS_JSON_PATH} NOT FOUND! Using empty config.", "CRITICAL")
        return {}

    try:
        with open(BROKERS_JSON_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Optional: Convert numeric strings back to int where needed
        for user_brokerid, cfg in data.items():
            if "LOGIN_ID" in cfg and isinstance(cfg["LOGIN_ID"], str):
                cfg["LOGIN_ID"] = cfg["LOGIN_ID"].strip()
            if "RISKREWARD" in cfg and isinstance(cfg["RISKREWARD"], (str, float)):
                cfg["RISKREWARD"] = int(cfg["RISKREWARD"])
        
        return data

    except json.JSONDecodeError as e:
        print(f"Invalid JSON in investors.json: {e}", "CRITICAL")
        return {}
    except Exception as e:
        print(f"Failed to load investors.json: {e}", "CRITICAL")
        return {}
usersdictionary = load_investors_dictionary()


#--VERIFICATIONS AND AUTHORIZATIONS--
def move_verified_investors():
    """
    Moves verified investors from verified_investors.json to:
    Step 1: investors.json (with limited fields: LOGIN_ID, PASSWORD, SERVER, INVESTED_WITH, TERMINAL_PATH)
    Step 2: Create activities.json directly in investor root folder (NEW PATH STRUCTURE)
    Step 3: Create empty tradeshistory.json if it doesn't exist
    
    Verified investors must have:
    - INVESTED_WITH (not empty)
    - execution_start_date (not empty)
    - contract_days_left (not empty)
    - TERMINAL_PATH (not empty) - MANDATORY FIELD
    
    Strategy name is extracted by splitting INVESTED_WITH on first underscore
    e.g., "deriv6_double-levels" → strategy = "double-levels"
    
    SUPPORTS MULTI-STRATEGY: INVESTED_WITH can contain comma-separated values
    e.g., "deriv6_strat1, deriv6_strat2, deriv6_strat3"
    
    For Step 2, activities.json is created directly in INV_PATH/{inv_id}/activities.json
    This simplifies the structure and removes the need for strategy subfolders.
    
    For Step 3, tradeshistory.json is created as an empty array if it doesn't exist.
    
    NOTE: Investors are NOT removed from verified_investors.json after processing
    """
    
    print(f"\n{'='*70}")
    print(f"📦 MOVE VERIFIED INVESTORS TO INVESTOR USERS".center(70))
    print(f"{'='*70}")
    
    # Default activities template for NEW PATH structure
    DEFAULT_ACTIVITIES = {
        "activate_autotrading": True,
        "bypass_restriction": True,
        "execution_start_date": "",
        "contract_duration": 30,
        "contract_expiry_date": "",
        "unauthorized_trades": {},
        "unauthorized_withdrawals": {},
        "unauthorized_action_detected": False,
        "strategies": []  # Store all strategies in a list for the new structure
    }
    
    # Check if verified investors file exists
    if not os.path.exists(VERIFIED_INVESTORS):
        print(f" Verified investors file not found: {VERIFIED_INVESTORS}")
        return False
    
    try:
        with open(VERIFIED_INVESTORS, 'r', encoding='utf-8') as f:
            verified_data = json.load(f)
    except Exception as e:
        print(f" Error loading verified investors: {e}")
        return False
    
    if not isinstance(verified_data, dict):
        print(f" Invalid format: expected dictionary")
        return False
    
    print(f"\n📋 Found {len(verified_data)} investors in verified list")
    
    # ============================================
    # STEP 1: Move to investors.json with limited fields
    # ============================================
    print(f"\n{'─'*70}")
    print(f"🔹 STEP 1: ADDING TO INVESTORS.JSON")
    print(f"{'─'*70}")
    
    # Load existing investors.json if it exists
    investors_data = {}
    if os.path.exists(INVESTOR_USERS):
        try:
            with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                investors_data = json.load(f)
            print(f"📄 Loaded existing investors.json with {len(investors_data)} investors")
        except Exception as e:
            print(f"⚠️ Error loading existing investors.json: {e}")
            investors_data = {}
    
    investors_updated = []
    investors_skipped = []
    
    for inv_id, investor_data in verified_data.items():
        # Case-insensitive lookup
        investor_data_upper = {k.upper(): v for k, v in investor_data.items()}
        
        # Check required fields
        invested_with = investor_data_upper.get('INVESTED_WITH', '').strip()
        execution_start = investor_data_upper.get('EXECUTION_START_DATE', '').strip()
        contract_days = investor_data_upper.get('CONTRACT_DAYS_LEFT', '').strip()
        terminal_path = investor_data_upper.get('TERMINAL_PATH', '').strip()
        login_id = investor_data_upper.get('LOGIN_ID') or investor_data_upper.get('LOGIN', '')
        password = investor_data_upper.get('PASSWORD', '').strip()
        server = investor_data_upper.get('SERVER', '').strip()
        
        missing_fields = []
        if not invested_with: missing_fields.append('INVESTED_WITH')
        if not execution_start: missing_fields.append('execution_start_date')
        if not contract_days: missing_fields.append('contract_days_left')
        if not terminal_path: missing_fields.append('TERMINAL_PATH')
        if not login_id: missing_fields.append('LOGIN_ID')
        if not password: missing_fields.append('PASSWORD')
        if not server: missing_fields.append('SERVER')
        
        if missing_fields:
            investors_skipped.append(f"{inv_id} (missing: {', '.join(missing_fields)})")
            continue
        
        # Create minimal investor record
        minimal_investor = {
            "LOGIN_ID": str(login_id).strip(),
            "PASSWORD": password,
            "SERVER": server,
            "INVESTED_WITH": invested_with,
            "TERMINAL_PATH": terminal_path
        }
        
        investors_data[inv_id] = minimal_investor
        investors_updated.append(inv_id)
    
    # Save updated investors.json
    if investors_updated:
        os.makedirs(os.path.dirname(INVESTOR_USERS), exist_ok=True)
        with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
            json.dump(investors_data, f, indent=4)
        
        print(f"\n  ✅ Added/Updated: {len(investors_updated)} investors")
        if investors_updated:
            print(f"     {', '.join(investors_updated[:5])}{'...' if len(investors_updated) > 5 else ''}")
        if investors_skipped:
            print(f"  ⏭️  Skipped: {len(investors_skipped)} investors")
    
    # ============================================
    # STEP 2: Create activities.json in INVESTOR ROOT (NEW PATH)
    # ============================================
    print(f"\n{'─'*70}")
    print(f"🔹 STEP 2: CREATING ACTIVITIES.JSON IN INVESTOR ROOT (NEW PATH)")
    print(f"{'─'*70}")
    
    # ============================================
    # STEP 3: Create empty tradeshistory.json if it doesn't exist
    # ============================================
    print(f"\n{'─'*70}")
    print(f"🔹 STEP 3: CREATING EMPTY TRADESHISTORY.JSON IF NEEDED")
    print(f"{'─'*70}")
    
    processed_summary = []
    skipped_summary = []
    created_activities_summary = []
    updated_activities_summary = []
    created_tradeshistory_summary = []
    expiry_calculation_errors = []  # Track expiry calculation errors
    
    for inv_id, investor_data in verified_data.items():
        print(f"\n{'─'*50}")
        print(f"📁 Processing investor: {inv_id}")
        print(f"{'─'*50}")
        
        # Case-insensitive lookup
        investor_data_upper = {k.upper(): v for k, v in investor_data.items()}
        
        invested_with = investor_data_upper.get('INVESTED_WITH', '').strip()
        execution_start = investor_data_upper.get('EXECUTION_START_DATE', '').strip()
        contract_days_raw = investor_data_upper.get('CONTRACT_DAYS_LEFT', '').strip()
        terminal_path = investor_data_upper.get('TERMINAL_PATH', '').strip()
        
        # Debug: Show raw values
        print(f"  📋 Raw data:")
        print(f"     INVESTED_WITH: '{invested_with}'")
        print(f"     EXECUTION_START_DATE: '{execution_start}'")
        print(f"     CONTRACT_DAYS_LEFT: '{contract_days_raw}'")
        print(f"     TERMINAL_PATH: '{terminal_path}'")
        
        # Skip if missing required fields
        if not all([invested_with, execution_start, terminal_path]):
            skipped_summary.append(inv_id)
            print(f"  ⏭️  Skipped: missing required fields (invested_with, execution_start, or terminal_path)")
            continue
        
        # Handle contract_days - check for NULL, empty, or invalid values
        contract_days = None
        contract_duration_val = 30  # Default value
        contract_days_valid = False
        
        if contract_days_raw and contract_days_raw.upper() not in ['NULL', 'NONE', '']:
            try:
                contract_days = int(contract_days_raw)
                contract_duration_val = contract_days
                contract_days_valid = True
                print(f"  ✓ Contract days valid: {contract_days}")
            except ValueError:
                print(f"  ⚠️ Invalid contract_days value: '{contract_days_raw}' - using default: 30")
        else:
            print(f"  ⚠️ contract_days is NULL or empty - using default: 30")
        
        # Split INVESTED_WITH by comma to handle multiple strategies
        strategies = [s.strip() for s in invested_with.split(",") if s.strip()]
        
        # Extract strategy names (without the prefix)
        strategy_names = []
        for strat_full in strategies:
            try:
                underscore_index = strat_full.find('_')
                if underscore_index != -1:
                    strategy_name = strat_full[underscore_index + 1:]
                    strategy_names.append(strategy_name)
                else:
                    strategy_names.append(strat_full)  # Use full name if no underscore
            except:
                strategy_names.append(strat_full)
        
        print(f"  🎯 Strategies: {strategy_names}")
        
        # Format execution start date
        formatted_start_date = execution_start
        date_parse_success = False
        
        try:
            date_obj = datetime.strptime(execution_start, "%Y-%m-%d")
            formatted_start_date = date_obj.strftime("%B %d, %Y")
            date_parse_success = True
            print(f"  📅 Parsed as YYYY-MM-DD: {execution_start} → {formatted_start_date}")
        except:
            try:
                date_obj = datetime.strptime(execution_start, "%B %d, %Y")
                formatted_start_date = execution_start
                date_parse_success = True
                print(f"  📅 Already in Month DD, YYYY format: {execution_start}")
            except Exception as e:
                print(f"  ⚠️ Could not parse date: {execution_start} - {e}")
        
        # Calculate expiry date
        expiry_date_str = ""
        
        if date_parse_success and contract_days_valid:
            print(f"  🔢 Calculating expiry date with {contract_duration_val} days...")
            
            try:
                # Parse start date
                start_date = None
                try:
                    # Try parsing as YYYY-MM-DD first
                    start_date = datetime.strptime(execution_start, "%Y-%m-%d")
                    print(f"     ✓ Start date (YYYY-MM-DD): {start_date}")
                except ValueError:
                    try:
                        # Try parsing as Month DD, YYYY
                        start_date = datetime.strptime(formatted_start_date, "%B %d, %Y")
                        print(f"     ✓ Start date (Month DD, YYYY): {start_date}")
                    except ValueError as e:
                        print(f"     ❌ Failed to parse start date: {e}")
                        start_date = None
                
                if start_date:
                    # Calculate expiry date
                    expiry_date = start_date + timedelta(days=contract_duration_val)
                    expiry_date_str = expiry_date.strftime("%B %d, %Y")
                    print(f"     ✓ Expiry date calculated: {expiry_date_str}")
                    
            except Exception as e:
                error_msg = f"Expiry calculation error for {inv_id}: {e}"
                print(f"  ❌ {error_msg}")
                expiry_calculation_errors.append(error_msg)
        else:
            if not date_parse_success:
                print(f"  ⚠️ Skipping expiry calculation: date parse failed")
            if not contract_days_valid:
                print(f"  ⚠️ Skipping expiry calculation: using default duration without expiry date")
        
        # Create investor root folder if it doesn't exist
        inv_root = Path(INV_PATH) / inv_id
        try:
            inv_root.mkdir(parents=True, exist_ok=True)
            print(f"  📁 Investor folder: {inv_root}")
        except Exception as e:
            print(f"  ❌ Could not create investor folder: {e}")
            continue
        
        # ============================================
        # STEP 2: Create/Update activities.json
        # ============================================
        activities_path = inv_root / "activities.json"
        
        # Load existing activities.json if it exists
        existing_activities = {}
        is_new_activities = False
        if activities_path.exists():
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    existing_activities = json.load(f)
                print(f"  📄 Existing activities.json found")
            except Exception as e:
                print(f"  ⚠️ Could not load existing activities.json: {e}")
                existing_activities = {}
                is_new_activities = True
        else:
            is_new_activities = True
            print(f"  ✨ Creating new activities.json")
        
        # Prepare activities data (merge with existing)
        activities_data = DEFAULT_ACTIVITIES.copy()
        activities_data.update(existing_activities)
        
        # Update with new values
        changed = False
        
        # Update strategies list
        if activities_data.get("strategies") != strategy_names:
            activities_data["strategies"] = strategy_names
            changed = True
            print(f"  📋 Updated strategies: {', '.join(strategy_names)}")
        
        # Update execution_start_date
        if activities_data.get("execution_start_date") != formatted_start_date:
            old_value = activities_data.get("execution_start_date")
            activities_data["execution_start_date"] = formatted_start_date
            changed = True
            print(f"  📅 Updated execution_start_date: '{old_value}' → '{formatted_start_date}'")
        
        # Update contract_duration (always update if we have a valid value)
        if contract_days_valid and activities_data.get("contract_duration") != contract_duration_val:
            old_value = activities_data.get("contract_duration")
            activities_data["contract_duration"] = contract_duration_val
            changed = True
            print(f"  ⏱️  Updated contract_duration: {old_value} → {contract_duration_val}")
        elif not contract_days_valid and contract_duration_val == 30:
            # Ensure default is set if not present
            if activities_data.get("contract_duration") is None:
                activities_data["contract_duration"] = 30
                changed = True
                print(f"  ⏱️  Set default contract_duration: 30")
        
        # Update contract_expiry_date
        if expiry_date_str and activities_data.get("contract_expiry_date") != expiry_date_str:
            old_value = activities_data.get("contract_expiry_date")
            activities_data["contract_expiry_date"] = expiry_date_str
            changed = True
            print(f"  📆 Updated contract_expiry_date: '{old_value}' → '{expiry_date_str}'")
        elif not expiry_date_str and activities_data.get("contract_expiry_date"):
            # Optionally clear expiry date if it was previously set but now invalid
            # Uncomment if you want to clear it
            # activities_data["contract_expiry_date"] = ""
            # changed = True
            print(f"  ℹ️  No expiry date to set (calculation failed or skipped)")
        
        # Update terminal_path if provided
        if terminal_path and activities_data.get("terminal_path") != terminal_path:
            activities_data["terminal_path"] = terminal_path
            changed = True
            print(f"  💾 Updated terminal_path")
        
        # Ensure default values for other fields
        for field, default_value in DEFAULT_ACTIVITIES.items():
            if field not in activities_data or activities_data[field] is None:
                activities_data[field] = default_value
                changed = True
        
        # Save activities.json
        try:
            with open(activities_path, 'w', encoding='utf-8') as f:
                json.dump(activities_data, f, indent=4)
            
            if is_new_activities:
                created_activities_summary.append(inv_id)
                print(f"  ✅ Created new activities.json")
            elif changed:
                updated_activities_summary.append(inv_id)
                print(f"  ✅ Updated existing activities.json")
            else:
                print(f"  ℹ️  No changes needed")
            
        except Exception as e:
            print(f"  ❌ Failed to save activities.json: {e}")
            continue
        
        # ============================================
        # STEP 3: Create empty tradeshistory.json if it doesn't exist
        # ============================================
        tradeshistory_path = inv_root / "tradeshistory.json"
        
        if not tradeshistory_path.exists():
            try:
                # Create empty trades history array
                with open(tradeshistory_path, 'w', encoding='utf-8') as f:
                    json.dump([], f, indent=4)
                created_tradeshistory_summary.append(inv_id)
                print(f"  ✅ Created empty tradeshistory.json")
            except Exception as e:
                print(f"  ❌ Failed to create tradeshistory.json: {e}")
        else:
            print(f"  ℹ️  tradeshistory.json already exists - skipping creation")
        
        processed_summary.append(inv_id)
    
    # ============================================
    # SUMMARY
    # ============================================
    print(f"\n{'─'*70}")
    print(f"📊 SUMMARY")
    print(f"{'─'*70}")
    
    print(f"\n🔹 STEP 1 - INVESTORS.JSON:")
    print(f"   ✅ Added/Updated: {len(investors_updated)}")
    if investors_updated:
        print(f"      {', '.join(investors_updated[:3])}{'...' if len(investors_updated) > 3 else ''}")
    if investors_skipped:
        print(f"   ⏭️  Skipped: {len(investors_skipped)}")
        for skip in investors_skipped[:2]:
            print(f"      • {skip}")
    
    print(f"\n🔹 STEP 2 - ACTIVITIES.JSON:")
    print(f"   ✅ Created: {len(created_activities_summary)} new activities.json files")
    if created_activities_summary:
        for inv in created_activities_summary[:5]:
            print(f"      • {inv}")
        if len(created_activities_summary) > 5:
            print(f"      ... and {len(created_activities_summary)-5} more")
    
    print(f"   🔄 Updated: {len(updated_activities_summary)} existing activities.json files")
    if updated_activities_summary:
        for inv in updated_activities_summary[:3]:
            print(f"      • {inv}")
        if len(updated_activities_summary) > 3:
            print(f"      ... and {len(updated_activities_summary)-3} more")
    
    print(f"\n🔹 STEP 3 - TRADESHISTORY.JSON:")
    print(f"   ✅ Created: {len(created_tradeshistory_summary)} new tradeshistory.json files")
    if created_tradeshistory_summary:
        for inv in created_tradeshistory_summary[:5]:
            print(f"      • {inv}")
        if len(created_tradeshistory_summary) > 5:
            print(f"      ... and {len(created_tradeshistory_summary)-5} more")
    
    if skipped_summary:
        print(f"\n   ⏭️  Skipped (missing fields): {len(skipped_summary)}")
        for inv in skipped_summary[:3]:
            print(f"      • {inv}")
    
    if expiry_calculation_errors:
        print(f"\n🔹 EXPIRY CALCULATION ERRORS:")
        print(f"   ❌ {len(expiry_calculation_errors)} errors occurred:")
        for error in expiry_calculation_errors:
            print(f"      • {error}")
    
    print(f"\n🔹 VERIFIED LIST:")
    print(f"   📁 All {len(verified_data)} investors remain in verified_investors.json")
    
    print(f"\n{'='*70}")
    print(f"✅ MOVE COMPLETE".center(70))
    print(f"{'='*70}")
    
    return True

def update_verified_investors_file():
    """
    Updates verified_investors.json by:
    1. Removing the MESSAGE field after moving them to activities.json
    2. Verifying that investors have the required files at INV_PATH/{investor_id}/
    3. Moving investors to issues if they're missing critical files
    4. Removing investors from verified_investors.json if they have any issues
    """
    print("\n" + "="*80)
    print("📋 CLEANING AND VERIFYING INVESTORS FILE")
    print("="*80)
    
    verified_investors_path = Path(VERIFIED_INVESTORS)
    updated_investors_path = Path(UPDATED_INVESTORS)
    issues_investors_path = Path(ISSUES_INVESTORS)
    
    if not verified_investors_path.exists():
        print(f"⚠️  {VERIFIED_INVESTORS} not found")
        return False
    
    # Load existing files
    try:
        with open(verified_investors_path, 'r', encoding='utf-8') as f:
            verified_investors = json.load(f)
    except Exception as e:
        print(f" Error reading verified_investors.json: {e}")
        return False
    
    # Load updated_investors if exists
    if updated_investors_path.exists():
        try:
            with open(updated_investors_path, 'r', encoding='utf-8') as f:
                updated_investors = json.load(f)
        except:
            updated_investors = {}
    else:
        updated_investors = {}
    
    # Load issues_investors if exists
    if issues_investors_path.exists():
        try:
            with open(issues_investors_path, 'r', encoding='utf-8') as f:
                issues_investors = json.load(f)
        except:
            issues_investors = {}
    else:
        issues_investors = {}
    
    updated = False
    investors_to_remove = []
    investors_moved_to_issues = []
    
    for inv_id, investor_data in verified_investors.items():
        print(f"\n📋 Checking investor: {inv_id}")
        print("-" * 40)
        
        # Check if investor folder exists at new path
        inv_folder = Path(INV_PATH) / inv_id
        
        if not inv_folder.exists():
            print(f"   ❌ Investor folder not found at: {inv_folder}")
            investors_to_remove.append(inv_id)
            
            # Add to issues_investors with reason
            if inv_id not in issues_investors:
                investor_data_copy = investor_data.copy()
                investor_data_copy['MESSAGE'] = f"Investor folder missing at {inv_folder}"
                investor_data_copy['verified_status'] = 'folder_missing'
                issues_investors[inv_id] = investor_data_copy
                investors_moved_to_issues.append(inv_id)
                print(f"  ⚠️  Added to issues_investors.json (folder missing)")
            continue
        
        # Check for required files
        required_files = ['activities.json', 'tradeshistory.json']
        missing_files = []
        
        for file in required_files:
            file_path = inv_folder / file
            if not file_path.exists():
                missing_files.append(file)
        
        if missing_files:
            print(f"  ❌ Missing required files: {', '.join(missing_files)}")
            
            # Always add to issues and remove from verified if files are missing
            investor_data_copy = investor_data.copy()
            investor_data_copy['MESSAGE'] = f"Missing required files: {', '.join(missing_files)}"
            investor_data_copy['verified_status'] = 'missing_files'
            issues_investors[inv_id] = investor_data_copy
            investors_moved_to_issues.append(inv_id)
            investors_to_remove.append(inv_id)
            print(f"  ⚠️  Added to issues_investors.json (missing files)")
            continue
        
        print(f"  ✅ All required files present: {', '.join(required_files)}")
        
        # Check if activities.json has required data
        activities_path = inv_folder / "activities.json"
        try:
            with open(activities_path, 'r', encoding='utf-8') as f:
                activities = json.load(f)
            
            # Validate critical fields
            execution_start_date = activities.get('execution_start_date')
            if not execution_start_date:
                print(f"  ❌ Missing execution_start_date in activities.json")
                
                # Always add to issues and remove from verified if execution_start_date is missing
                investor_data_copy = investor_data.copy()
                investor_data_copy['MESSAGE'] = "Missing execution_start_date in activities.json"
                investor_data_copy['verified_status'] = 'missing_start_date'
                issues_investors[inv_id] = investor_data_copy
                investors_moved_to_issues.append(inv_id)
                investors_to_remove.append(inv_id)
                print(f"  ⚠️  Added to issues_investors.json (missing start date)")
                continue
            
            # Check if tradeshistory.json exists (empty is acceptable)
            tradeshistory_path = inv_folder / "tradeshistory.json"
            if tradeshistory_path.exists():
                try:
                    with open(tradeshistory_path, 'r', encoding='utf-8') as f:
                        tradeshistory = json.load(f)
                    
                    if tradeshistory:
                        print(f"  ✅ Found {len(tradeshistory)} authorized trades in tradeshistory.json")
                    else:
                        print(f"  ℹ️  tradeshistory.json exists but is empty (acceptable)")
                except Exception as e:
                    print(f"  ⚠️  Error reading tradeshistory.json: {e}")
                    # Note: If tradeshistory.json is corrupted, that's an issue
                    investor_data_copy = investor_data.copy()
                    investor_data_copy['MESSAGE'] = f"Error reading tradeshistory.json: {str(e)}"
                    investor_data_copy['verified_status'] = 'tradeshistory_read_error'
                    issues_investors[inv_id] = investor_data_copy
                    investors_moved_to_issues.append(inv_id)
                    investors_to_remove.append(inv_id)
                    continue
            else:
                print(f"  ❌ tradeshistory.json missing")
                # Missing tradeshistory.json is an issue
                investor_data_copy = investor_data.copy()
                investor_data_copy['MESSAGE'] = "tradeshistory.json missing"
                investor_data_copy['verified_status'] = 'missing_tradeshistory'
                issues_investors[inv_id] = investor_data_copy
                investors_moved_to_issues.append(inv_id)
                investors_to_remove.append(inv_id)
                continue
            
            # Remove MESSAGE field if it exists
            if 'MESSAGE' in investor_data:
                print(f"  🧹 Removing MESSAGE field for investor {inv_id}")
                del investor_data['MESSAGE']
                updated = True
            
            # Add verification status
            investor_data['verified_status'] = 'verified'
            
        except Exception as e:
            print(f"  ❌ Error reading activities.json: {e}")
            # Always add to issues and remove from verified on read error
            investor_data_copy = investor_data.copy()
            investor_data_copy['MESSAGE'] = f"Error reading activities.json: {str(e)}"
            investor_data_copy['verified_status'] = 'read_error'
            issues_investors[inv_id] = investor_data_copy
            investors_moved_to_issues.append(inv_id)
            investors_to_remove.append(inv_id)
            continue
    
    # Remove investors from verified_investors that were moved to issues
    if investors_to_remove:
        for inv_id in investors_to_remove:
            if inv_id in verified_investors:
                print(f"  🗑️  Removing {inv_id} from verified_investors.json")
                del verified_investors[inv_id]
                updated = True
    
    # Save updated files
    try:
        # Save verified_investors.json
        with open(verified_investors_path, 'w', encoding='utf-8') as f:
            json.dump(verified_investors, f, indent=4)
        
        # Save issues_investors.json
        with open(issues_investors_path, 'w', encoding='utf-8') as f:
            json.dump(issues_investors, f, indent=4)
        
        print(f"\n" + "="*80)
        if updated:
            print(f"✅ Updated verified_investors.json")
            if investors_to_remove:
                print(f"   - Removed {len(investors_to_remove)} investors with issues:")
                for inv_id in investors_to_remove:
                    print(f"     • {inv_id}")
        else:
            print(f"ℹ️  No changes made to verified_investors.json")
        
        if issues_investors:
            print(f"⚠️  Issues investors file updated with {len(issues_investors)} investors")
            if investors_moved_to_issues:
                print(f"   - Newly added investors with issues:")
                for inv_id in investors_moved_to_issues:
                    print(f"     • {inv_id}")
        print("="*80)
        
    except Exception as e:
        print(f" Error saving files: {e}")
        return False
    
    return True

def get_requirements(inv_id):
    """
    Mirroring the logic of update_investor_info to find the date 
    directly in root files (new path structure). Also checks if investor balance
    meets minimum requirement from requirements.json and moves
    them to issues_investors.json with a message if not.
    
    Core Functions:
    1. Read execution_start_date from activities.json in root folder
    2. Connect to MT5 and calculate starting balance by:
       a. Finding first deposit after execution start date
       b. Searching for existing balance between execution start date and first deposit
       c. If existing balance found, use it; otherwise use first deposit
       d. If existing balance < minimum requirement, add first deposit to check again
    3. Check minimum balance requirement from investor root folder
    4. Move non-compliant investors to issues_investors.json with error messages
    5. Update activities.json with broker_balance field (starting balance or first deposit)
    """
    execution_start_date = None
    inv_root = Path(INV_PATH) / inv_id
    
    if not inv_root.exists():
        print(f"❌ Path not found: {inv_root}")
        return None

    # 1. Check activities.json directly in root folder (NEW PATH)
    activities_path = inv_root / "activities.json"
    if activities_path.exists():
        try:
            with open(activities_path, 'r', encoding='utf-8') as f:
                activities = json.load(f)
                execution_start_date = activities.get('execution_start_date')
                if execution_start_date:
                    print(f"  📋 Found execution_start_date in activities.json: {execution_start_date}")
        except Exception as e:
            print(f"  ⚠️  Error reading activities.json: {e}")

    # 2. Backup: Check accountmanagement.json if not found in activities.json
    if not execution_start_date:
        acc_mgmt_path = inv_root / "accountmanagement.json"
        if acc_mgmt_path.exists():
            try:
                with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                    acc_mgmt = json.load(f)
                    execution_start_date = acc_mgmt.get('execution_start_date')
                    if execution_start_date:
                        print(f"  📋 Found execution_start_date in accountmanagement.json: {execution_start_date}")
            except Exception as e:
                print(f"  ⚠️  Error reading accountmanagement.json: {e}")

    if not execution_start_date:
        print(f"❌ Date not found for {inv_id} in activities.json or accountmanagement.json")
        return None

    # 3. MT5 Calculation with enhanced balance logic
    start_datetime = None
    for fmt in ["%B %d, %Y", "%Y-%m-%d"]:
        try:
            start_datetime = datetime.strptime(execution_start_date, fmt).replace(hour=0, minute=0, second=0)
            break
        except: continue

    if start_datetime:
        print(f"  🔍 Fetching deals from: {start_datetime.strftime('%Y-%m-%d')}")
        all_deals = mt5.history_deals_get(start_datetime, datetime.now())
        account_info = mt5.account_info()
        
        if account_info:
            # Find the FIRST DEPOSIT after execution start date
            first_deposit_amount = None
            first_deposit_time = None
            all_deposits = []
            
            # Collect all deposits (type = 2 for deposit in MT5)
            for deal in all_deals:
                if deal.type == 2:  # 2 = DEPOSIT in MT5
                    all_deposits.append({
                        'amount': deal.profit,
                        'time': deal.time,
                        'datetime': datetime.fromtimestamp(deal.time)
                    })
            
            # Find the first deposit (earliest timestamp)
            if all_deposits:
                all_deposits.sort(key=lambda x: x['time'])
                first_deposit = all_deposits[0]
                first_deposit_amount = first_deposit['amount']
                first_deposit_time = first_deposit['datetime'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"  💰 Found first deposit: ${first_deposit_amount:.2f} at {first_deposit_time}")
                print(f"  ℹ️  Total deposits found: {len(all_deposits)}")
            
            # ENHANCED LOGIC: Search for existing balance between execution start date and first deposit
            existing_balance = None
            if first_deposit_time:
                # Get deals BEFORE the first deposit (from execution start date up to first deposit)
                deals_before_first_deposit = []
                first_deposit_timestamp = first_deposit['time']
                
                for deal in all_deals:
                    if deal.time < first_deposit_timestamp:
                        deals_before_first_deposit.append(deal)
                
                if deals_before_first_deposit:
                    # Calculate balance from trades before first deposit
                    trades_pnl_before_deposit = sum((d.profit + d.swap + d.commission) for d in deals_before_first_deposit if d.type in [0, 1])
                    withdrawals_before_deposit = sum(d.profit for d in deals_before_first_deposit if d.type == 3)
                    
                    # The existing balance would be the starting balance from execution date
                    # We need to calculate what the balance was before the first deposit
                    # Current balance minus all deposits and trades gives us the initial balance
                    all_deposits_before_first = [d for d in all_deposits if d['time'] < first_deposit_timestamp]
                    total_deposits_before_first = sum(d['amount'] for d in all_deposits_before_first)
                    
                    # Calculate existing balance = current_balance - (first_deposit + other_deposits + trades + withdrawals)
                    # But we want the balance right before the first deposit
                    # Let's get the balance at the time just before first deposit
                    
                    # Get all deals up to (but not including) first deposit
                    balance_at_time = 0
                    for deal in sorted(deals_before_first_deposit, key=lambda x: x.time):
                        if deal.type in [0, 1]:  # Trade
                            balance_at_time += (deal.profit + deal.swap + deal.commission)
                        elif deal.type == 2:  # Deposit
                            balance_at_time += deal.profit
                        elif deal.type == 3:  # Withdrawal
                            balance_at_time += deal.profit
                    
                    # The existing balance would be the initial deposit (if any) plus trades
                    # But we need to find if there was an initial balance before any deposit
                    # Check if there were any deposits before first deposit (unlikely but possible)
                    if total_deposits_before_first > 0:
                        # There was a deposit before the first deposit we found (shouldn't happen if we sorted correctly)
                        existing_balance = total_deposits_before_first + trades_pnl_before_deposit + withdrawals_before_deposit
                    else:
                        # No deposits before first deposit, so existing balance comes from initial funds + trades
                        # Calculate starting balance from execution date
                        total_pnl_all = sum((d.profit + d.swap + d.commission) for d in all_deals if d.type in [0, 1])
                        total_withdrawals_all = sum(d.profit for d in all_deals if d.type == 3)
                        total_deposits_all = sum(d['amount'] for d in all_deposits)
                        
                        # current_balance = starting_balance + total_deposits_all + total_pnl_all + total_withdrawals_all
                        # starting_balance = current_balance - total_deposits_all - total_pnl_all - total_withdrawals_all
                        starting_balance_from_exec = account_info.balance - total_deposits_all - total_pnl_all - total_withdrawals_all
                        
                        # The existing balance before first deposit would be starting_balance_from_exec + trades_before_first
                        existing_balance = starting_balance_from_exec + trades_pnl_before_deposit + withdrawals_before_deposit
                    
                    print(f"  🔍 Found existing balance before first deposit: ${existing_balance:.2f}")
                else:
                    # No deals before first deposit
                    existing_balance = 0
                    print(f"  🔍 No deals found before first deposit, existing balance = $0")
            
            # Determine starting balance based on existing balance
            broker_balance = None
            starting_bal = None
            
            if existing_balance is not None and existing_balance > 0:
                starting_bal = existing_balance
                broker_balance = existing_balance
                print(f"  📊 Using EXISTING BALANCE (found before first deposit): ${starting_bal:.2f}")
            elif first_deposit_amount is not None:
                starting_bal = first_deposit_amount
                broker_balance = first_deposit_amount
                print(f"  📊 Using FIRST DEPOSIT (no existing balance found): ${starting_bal:.2f}")
            else:
                # Fallback: No deposits found
                total_pnl = sum((d.profit + d.swap + d.commission) for d in all_deals if d.type in [0, 1])
                starting_bal = account_info.balance - total_pnl
                broker_balance = starting_bal
                print(f"  ℹ️  No deposits found, using traditional calculation: ${starting_bal:.2f}")
            
            # Calculate additional metrics for reporting
            total_trades_pnl = sum((d.profit + d.swap + d.commission) for d in all_deals if d.type in [0, 1])
            total_withdrawals = sum(d.profit for d in all_deals if d.type == 3)
            other_deposits_total = sum(d['amount'] for d in all_deposits[1:]) if len(all_deposits) > 1 else 0
            
            print(f"  📊 Balance breakdown:")
            print(f"     Starting balance: ${starting_bal:.2f}")
            if existing_balance and existing_balance > 0:
                print(f"     (Existing balance before first deposit: ${existing_balance:.2f})")
            if first_deposit_amount:
                print(f"     First deposit: ${first_deposit_amount:.2f}")
            print(f"     Other deposits (ignored): ${other_deposits_total:.2f}")
            print(f"     Trades P&L: ${total_trades_pnl:.2f}")
            print(f"     Withdrawals: ${total_withdrawals:.2f}")
            print(f"     Current balance: ${account_info.balance:.2f}")
            
            print(f"📊 {inv_id} | Start: {execution_start_date} | Starting Balance: ${starting_bal:.2f}")
            print(f"   Current Balance: ${account_info.balance:.2f}")
            
            # --- UPDATE ACTIVITIES.JSON WITH BROKER_BALANCE ---
            if broker_balance is not None:
                try:
                    # Load current activities.json
                    if activities_path.exists():
                        with open(activities_path, 'r', encoding='utf-8') as f:
                            activities_data = json.load(f)
                    else:
                        # Create new activities.json if it doesn't exist
                        activities_data = {
                            "activate_autotrading": True,
                            "bypass_restriction": True,
                            "execution_start_date": execution_start_date,
                            "contract_duration": 30,
                            "contract_expiry_date": "",
                            "unauthorized_trades": {},
                            "unauthorized_withdrawals": {},
                            "unauthorized_action_detected": False,
                            "strategies": []
                        }
                    
                    # Update broker_balance
                    old_broker_balance = activities_data.get('broker_balance')
                    activities_data['broker_balance'] = float(broker_balance)
                    
                    # Save updated activities.json
                    with open(activities_path, 'w', encoding='utf-8') as f:
                        json.dump(activities_data, f, indent=4)
                    
                    if old_broker_balance is None:
                        print(f"  💾 Added broker_balance to activities.json: ${broker_balance:.2f}")
                    elif old_broker_balance != broker_balance:
                        print(f"  💾 Updated broker_balance in activities.json: ${old_broker_balance:.2f} → ${broker_balance:.2f}")
                    else:
                        print(f"  ℹ️  broker_balance already correct in activities.json: ${broker_balance:.2f}")
                        
                except Exception as e:
                    print(f"  ⚠️  Error updating activities.json with broker_balance: {e}")
            
            # --- CHECK minimum balance requirement with enhanced verification ---
            try:
                requirements_path = inv_root / "requirements.json"
                
                if requirements_path.exists():
                    print(f"  🔍 Found requirements.json at: {requirements_path}")
                    with open(requirements_path, 'r', encoding='utf-8') as f:
                        requirements_config = json.load(f)
                        
                        min_balance = None
                        if isinstance(requirements_config, list) and len(requirements_config) > 0:
                            min_balance = requirements_config[0].get('minimum_balance')
                        elif isinstance(requirements_config, dict):
                            min_balance = requirements_config.get('minimum_balance')
                        else:
                            print(f"  ⚠️  requirements.json has unexpected format: {type(requirements_config)}")
                        
                        if min_balance is not None:
                            print(f"  📊 Minimum balance requirement: ${min_balance}")
                            
                            # ENHANCED VERIFICATION: Check if existing balance alone meets requirement
                            balance_to_check = starting_bal
                            meets_requirement = starting_bal >= min_balance
                            
                            # If using existing balance and it doesn't meet requirement, check with first deposit added
                            if existing_balance is not None and existing_balance > 0 and not meets_requirement and first_deposit_amount:
                                combined_balance = existing_balance + first_deposit_amount
                                print(f"  🔍 Existing balance ${existing_balance:.2f} does NOT meet minimum requirement")
                                print(f"  🔍 Checking combined balance (existing + first deposit): ${combined_balance:.2f}")
                                
                                if combined_balance >= min_balance:
                                    meets_requirement = True
                                    balance_to_check = combined_balance
                                    print(f"  ✅ Combined balance MEETS minimum requirement")
                                    
                                    # Update broker_balance with combined balance if it meets requirement
                                    if combined_balance != broker_balance:
                                        broker_balance = combined_balance
                                        try:
                                            with open(activities_path, 'r', encoding='utf-8') as f:
                                                activities_data = json.load(f)
                                            activities_data['broker_balance'] = float(combined_balance)
                                            with open(activities_path, 'w', encoding='utf-8') as f:
                                                json.dump(activities_data, f, indent=4)
                                            print(f"  💾 Updated broker_balance to combined balance: ${combined_balance:.2f}")
                                        except Exception as e:
                                            print(f"  ⚠️  Error updating broker_balance with combined balance: {e}")
                                else:
                                    print(f"  ❌ Combined balance still BELOW minimum requirement")
                            
                            if meets_requirement:
                                print(f"  ✅ Balance ${balance_to_check:.2f} MEETS minimum requirement (${min_balance})")
                            else:
                                print(f"  ⚠️  Balance ${balance_to_check:.2f} is BELOW minimum requirement ${min_balance}")
                                print(f"  ❌ Moving investor {inv_id} to issues_investors.json")
                                
                                # Move investor logic
                                if os.path.exists(INVESTOR_USERS):
                                    with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                                        investors_data = json.load(f)
                                    
                                    investor_data_to_move = None
                                    if isinstance(investors_data, list):
                                        for i, inv in enumerate(investors_data):
                                            if inv_id in inv:
                                                investor_data_to_move = inv[inv_id]
                                                investors_data.pop(i)
                                                break
                                    else:
                                        if inv_id in investors_data:
                                            investor_data_to_move = investors_data[inv_id]
                                            del investors_data[inv_id]
                                    
                                    if investor_data_to_move:
                                        message = f"Balance ${balance_to_check:.2f} is below minimum requirement ${min_balance}"
                                        if existing_balance and existing_balance > 0 and first_deposit_amount:
                                            message = f"Existing balance ${existing_balance:.2f} + first deposit ${first_deposit_amount:.2f} = ${balance_to_check:.2f} is below minimum requirement ${min_balance}"
                                        investor_data_to_move['MESSAGE'] = message
                                        
                                        with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                                            json.dump(investors_data, f, indent=4)
                                        
                                        issues_data = {}
                                        if os.path.exists(ISSUES_INVESTORS):
                                            try:
                                                with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                                    issues_data = json.load(f)
                                            except: issues_data = {}
                                        
                                        issues_data[inv_id] = investor_data_to_move
                                        with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                                            json.dump(issues_data, f, indent=4)
                                        
                                        print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                                    else:
                                        print(f"  ⚠️  Investor {inv_id} not found in investors.json")
                                
                                return None  # Return None since investor is being moved
                        else:
                            print(f"  ⚠️  No minimum_balance found in requirements.json")
                else:
                    print(f"  ℹ️  No requirements.json found in investor root folder - skipping minimum balance check")
                    
            except Exception as e:
                print(f"  ⚠️  Error checking minimum balance requirement: {e}")
            
            return starting_bal

        else:
            # --- Handle Invalid Broker Login / No Account Info ---
            print(f"  ⚠️  Could not get account info for {inv_id}")
            print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to invalid login")
            
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    investor_data_to_move['MESSAGE'] = "invalid broker login please check your login details"
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
            
            return None

    else:
        print(f"  ⚠️  Could not parse start date: {execution_start_date}")

    return None

def check_and_record_authorized_actions(inv_id=None):
    """
    Check and record authorized/unauthorized actions for investors based on tradeshistory.json.
    
    This function:
    1. Reads tradeshistory.json to get authorized trades (tickets and magics)
    2. Compares with MT5 pending orders and open positions
    3. Fetches history orders from execution start date to present
    4. Identifies unauthorized orders and positions
    5. Records them in activities.json with detailed information
    6. Records completed history orders with profit/loss information
    7. Calculates profit and loss (current balance - starting balance)
    8. Records trades (both authorized and unauthorized) in proper format
    
    MATCHING STRATEGY (in order):
    1. Direct ticket number match
    2. Magic number match
    3. Placed timestamp match (within 5 seconds)
    4. Volume + symbol + time window match
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about authorized/unauthorized actions found
    """
    
    print("\n" + "="*80)
    print("  🔍 AUTHORIZED ACTIONS AUDIT".ljust(79) + "=")
    print("="*80)
    
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "investors_with_unauthorized": 0,
        "unauthorized_orders_found": 0,
        "unauthorized_positions_found": 0,
        "history_matched": {"ticket": 0, "magic": 0, "timestamp": 0, "volume_symbol": 0, "synthetic": 0},
        "history_orders_recorded": 0,
        "history_orders_updated": 0,
        "bypass_active_investors": 0,
        "autotrading_active_investors": 0,
        "unauthorized_by_investor": {},
        "processing_success": False
    }
    
    investor_ids = [inv_id] if inv_id else list(usersdictionary.keys())
    
    if not investor_ids:
        print("│\n├─ ❌ No investors found.")
        print("="*80)
        return stats
    
    for user_brokerid in investor_ids:
        # ============================================================
        # HEADER
        # ============================================================
        print(f"\n├{'─'*78}┤")
        print(f"│  📋 INVESTOR: {user_brokerid}")
        print(f"├{'─'*78}┤")
        
        inv_root = Path(INV_PATH) / user_brokerid
        if not inv_root.exists():
            print(f"│  ❌ Path not found: {inv_root}")
            continue
        
        stats["investors_processed"] += 1
        
        # ============================================================
        # LOAD CONFIGURATION FILES
        # ============================================================
        acc_mgmt_path = inv_root / "accountmanagement.json"
        tradeshistory_path = inv_root / "tradeshistory.json"
        activities_path = inv_root / "activities.json"
        
        # Load existing activities.json to get starting balance
        existing_activities = {}
        starting_balance = None
        if activities_path.exists():
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    existing_activities = json.load(f)
                    starting_balance = existing_activities.get('broker_balance')
                if starting_balance:
                    print(f"│  💰 Starting balance from activities.json: ${starting_balance:.2f}")
            except Exception as e:
                print(f"│  ⚠️ Error reading activities.json: {e}")
        
        # Load tradeshistory.json
        existing_history_dict = {}
        if tradeshistory_path.exists():
            try:
                with open(tradeshistory_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    items = data if isinstance(data, list) else list(data.values())
                    for trade in items:
                        ticket = trade.get('ticket')
                        if ticket:
                            existing_history_dict[str(ticket)] = trade
                print(f"│  📚 Loaded {len(existing_history_dict)} trades from tradeshistory.json")
            except Exception as e:
                print(f"│  ⚠️ Error reading tradeshistory.json: {e}")
        
        # Load account settings
        bypass_active = False
        autotrading_active = False
        magic_number = 0
        execution_start_date = None
        
        if acc_mgmt_path.exists():
            try:
                with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                    acc_config = json.load(f)
                    settings = acc_config.get("settings", {})
                    bypass_active = settings.get("enable_authorization_bypass", False)
                    autotrading_active = settings.get("enable_auto_trading", False)
                    magic_number = acc_config.get("magic_number", 0)
                    execution_start_date = acc_config.get('execution_start_date')
                print(f"│  ⚙️ Bypass: {bypass_active} | Auto-trading: {autotrading_active} | Magic: {magic_number}")
            except Exception as e:
                print(f"│  ⚠️ Error reading accountmanagement.json: {e}")
        
        # Get execution start date from activities.json if not found
        if not execution_start_date and activities_path.exists():
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    activities_data = json.load(f)
                    execution_start_date = activities_data.get('execution_start_date')
                if execution_start_date:
                    print(f"│  📅 Start date from activities.json: {execution_start_date}")
            except:
                pass
        
        # Parse start date
        start_datetime = None
        if execution_start_date:
            for fmt in ["%B %d, %Y", "%Y-%m-%d"]:
                try:
                    start_datetime = datetime.strptime(execution_start_date, fmt)
                    print(f"│  📅 Fetching history from: {start_datetime.strftime('%Y-%m-%d')}")
                    break
                except:
                    continue
        
        # ============================================================
        # BUILD AUTHORIZED INDEXES
        # ============================================================
        authorized_tickets = set()
        authorized_magics = set()
        authorized_by_timestamp = {}  # timestamp -> ticket
        authorized_by_volume_symbol = {}  # (symbol, volume, approx_time) -> ticket
        
        for ticket, trade in existing_history_dict.items():
            # Add ticket
            try:
                authorized_tickets.add(int(ticket))
            except:
                authorized_tickets.add(ticket)
            
            # Add magic
            magic = trade.get('magic')
            if magic:
                try:
                    authorized_magics.add(int(magic))
                except:
                    authorized_magics.add(magic)
            
            # Index by placed timestamp (for matching)
            placed_ts = trade.get('placed_timestamp')
            if placed_ts:
                try:
                    # Parse timestamp and store with 5-second tolerance
                    dt = datetime.fromisoformat(placed_ts.replace('Z', '+00:00'))
                    ts_key = int(dt.timestamp() / 5)  # 5-second windows
                    if ts_key not in authorized_by_timestamp:
                        authorized_by_timestamp[ts_key] = []
                    authorized_by_timestamp[ts_key].append(ticket)
                except:
                    pass
            
            # Index by symbol + volume + time window (15 minutes)
            symbol = trade.get('symbol')
            volume = trade.get('volume')
            if symbol and volume and placed_ts:
                try:
                    dt = datetime.fromisoformat(placed_ts.replace('Z', '+00:00'))
                    time_window = int(dt.timestamp() / 900)  # 15-minute windows
                    key = f"{symbol}_{volume}_{time_window}"
                    if key not in authorized_by_volume_symbol:
                        authorized_by_volume_symbol[key] = []
                    authorized_by_volume_symbol[key].append(ticket)
                except:
                    pass
        
        print(f"│  🔑 Authorized: {len(authorized_tickets)} tickets, {len(authorized_magics)} magics")
        
        # ============================================================
        # CONNECT TO MT5
        # ============================================================
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"│  ❌ No broker config found")
            continue
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"│  🔌 Logging into account {login_id}...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                print(f"│  ❌ Login failed: {mt5.last_error()}")
                continue
            print(f"│  ✅ Successfully logged in")
        else:
            print(f"│  ✅ Already logged in")
        
        # Get current balance after login
        current_balance = mt5.account_info().balance
        print(f"│  💰 Current balance: ${current_balance:.2f}")
        
        # Calculate profit and loss if starting balance exists
        profit_and_loss = 0.0
        if starting_balance is not None:
            profit_and_loss = current_balance - starting_balance
            print(f"│  📈 Profit & Loss: ${profit_and_loss:.2f}")
        
        # ============================================================
        # FETCH HISTORY DEALS
        # ============================================================
        history_matched = {"ticket": 0, "magic": 0, "timestamp": 0, "volume_symbol": 0, "synthetic": 0}
        history_recorded = 0
        history_updated = 0
        
        # Initialize trades lists
        authorized_closed_trades = []
        unauthorized_trades_list = []
        won_trades = 0
        lost_trades = 0
        symbols_won = {}
        symbols_lost = {}
        
        if start_datetime:
            print(f"│\n├─ 📜 FETCHING HISTORY DEALS".ljust(79) + "┤")
            
            history_deals = mt5.history_deals_get(start_datetime, datetime.now())
            history_orders = mt5.history_orders_get(start_datetime, datetime.now())
            
            # Index orders by ticket for quick lookup
            orders_by_ticket = {order.ticket: order for order in (history_orders or [])}
            
            if history_deals:
                print(f"│  ✅ Found {len(history_deals)} deals, {len(orders_by_ticket)} orders")
                
                # Group deals by order ticket
                deals_by_order = {}
                for deal in history_deals:
                    order_key = deal.order if deal.order != 0 else f"{deal.symbol}_{deal.time}_{deal.price}"
                    if order_key not in deals_by_order:
                        deals_by_order[order_key] = []
                    deals_by_order[order_key].append(deal)
                
                print(f"│  📊 Grouped into {len(deals_by_order)} unique orders")
                
                # Process each order group
                for order_key, deals in deals_by_order.items():
                    deals.sort(key=lambda x: x.time)
                    
                    # Calculate totals
                    total_profit = sum(d.profit for d in deals)
                    total_commission = sum(d.commission for d in deals)
                    total_swap = sum(d.swap for d in deals)
                    total_pnl = total_profit + total_commission + total_swap
                    deal_types = [d.type for d in deals]
                    
                    # Find entry and exit deals
                    entry_deal = next((d for d in deals if d.type in [0, 1]), None)
                    exit_deal = next((d for d in deals if d.type in [0, 1] and d != entry_deal), None)
                    
                    # Generate ticket ID
                    ticket_id = None
                    if isinstance(order_key, int) and order_key != 0:
                        ticket_id = order_key
                    elif entry_deal and entry_deal.order != 0:
                        ticket_id = entry_deal.order
                    else:
                        ticket_id = abs(hash(order_key)) % 100000000
                    
                    # ================================================
                    # MATCHING LOGIC (FALLBACK CHAIN)
                    # ================================================
                    is_authorized = False
                    match_method = None
                    matched_ticket = None
                    
                    # Method 1: Direct ticket match
                    if ticket_id and str(ticket_id) in existing_history_dict:
                        is_authorized = True
                        match_method = "ticket"
                        matched_ticket = ticket_id
                        history_matched["ticket"] += 1
                    
                    # Method 2: Magic number match
                    if not is_authorized and entry_deal and entry_deal.magic in authorized_magics:
                        is_authorized = True
                        match_method = "magic"
                        matched_ticket = f"magic_{entry_deal.magic}"
                        history_matched["magic"] += 1
                    
                    # Method 3: Placed timestamp match (within 5 seconds)
                    if not is_authorized and entry_deal:
                        deal_time = int(entry_deal.time)
                        ts_window = int(deal_time / 5)
                        if ts_window in authorized_by_timestamp:
                            is_authorized = True
                            match_method = "timestamp"
                            matched_ticket = authorized_by_timestamp[ts_window][0]
                            history_matched["timestamp"] += 1
                    
                    # Method 4: Volume + Symbol + Time window match (15 min)
                    if not is_authorized and entry_deal:
                        symbol = entry_deal.symbol
                        volume = entry_deal.volume
                        time_window = int(entry_deal.time / 900)
                        key = f"{symbol}_{volume}_{time_window}"
                        if key in authorized_by_volume_symbol:
                            is_authorized = True
                            match_method = "volume_symbol"
                            matched_ticket = authorized_by_volume_symbol[key][0]
                            history_matched["volume_symbol"] += 1
                    
                    # Method 5: Synthetic record for non-trade operations
                    if not is_authorized and entry_deal is None and (total_profit != 0 or total_commission != 0):
                        is_authorized = True
                        match_method = "synthetic"
                        history_matched["synthetic"] += 1
                    
                    # ================================================
                    # CREATE OR UPDATE TRADE RECORD
                    # ================================================
                    if entry_deal and entry_deal.type in [0, 1]:  # Only process actual trades
                        trade_record = {
                            'ticket': ticket_id,
                            'symbol': entry_deal.symbol,
                            'type': 'BUY' if entry_deal.type == 0 else 'SELL',
                            'volume': entry_deal.volume,
                            'profit': round(total_pnl, 2),
                            'time': datetime.fromtimestamp(entry_deal.time).strftime('%Y-%m-%d %H:%M:%S'),
                            'match_method': match_method if match_method else 'unauthorized'
                        }
                        
                        if is_authorized:
                            # Authorized trade
                            authorized_closed_trades.append(trade_record)
                            total_authorized_pnl = total_pnl
                            
                            # Update win/loss statistics
                            if total_pnl > 0:
                                won_trades += 1
                                symbols_won[entry_deal.symbol] = symbols_won.get(entry_deal.symbol, 0.0) + total_pnl
                            elif total_pnl < 0:
                                lost_trades += 1
                                symbols_lost[entry_deal.symbol] = symbols_lost.get(entry_deal.symbol, 0.0) + total_pnl
                            
                            profit_symbol = "📈" if total_pnl > 0 else "📉" if total_pnl < 0 else "⚖️"
                            print(f"│     {profit_symbol} Authorized #{ticket_id}: ${total_pnl:.2f} [{match_method}]")
                        else:
                            # Unauthorized trade
                            trade_record['reason'] = f"Trade NOT authorized (Ticket: {ticket_id})"
                            unauthorized_trades_list.append(trade_record)
                            profit_symbol = "🚫" if total_pnl > 0 else "⚠️" if total_pnl < 0 else "⚖️"
                            print(f"│     {profit_symbol} UNAUTHORIZED #{ticket_id}: ${total_pnl:.2f}")
                    
                    # Update existing history dictionary if needed
                    if is_authorized and ticket_id and str(ticket_id) in existing_history_dict:
                        # Update existing trade status if closed
                        trade = existing_history_dict[str(ticket_id)]
                        if trade.get('status') != 'closed' and (exit_deal or len(deals) > 1):
                            trade['status'] = 'closed'
                            trade['close_price'] = exit_deal.price if exit_deal else entry_deal.price if entry_deal else 0
                            trade['close_time'] = datetime.fromtimestamp(exit_deal.time).strftime('%Y-%m-%d %H:%M:%S') if exit_deal else datetime.fromtimestamp(entry_deal.time).strftime('%Y-%m-%d %H:%M:%S') if entry_deal else ''
                            trade['profit'] = total_pnl
                            trade['profit_and_loss'] = float(total_pnl)
                            trade['history_match_method'] = match_method
                            history_updated += 1
                
                # Print matching summary
                print(f"│\n├─ 📊 MATCHING SUMMARY")
                print(f"│  • Ticket match:     {history_matched['ticket']:>3}")
                print(f"│  • Magic match:      {history_matched['magic']:>3}")
                print(f"│  • Timestamp match:  {history_matched['timestamp']:>3}")
                print(f"│  • Volume+Symbol:    {history_matched['volume_symbol']:>3}")
                print(f"│  • Synthetic:        {history_matched['synthetic']:>3}")
                print(f"│  • Authorized trades: {len(authorized_closed_trades)}")
                print(f"│  • Unauthorized trades: {len(unauthorized_trades_list)}")
                
                # Update stats
                for key in history_matched:
                    stats["history_matched"][key] += history_matched[key]
                stats["history_orders_recorded"] += len(authorized_closed_trades)
                stats["history_orders_updated"] += history_updated
                
            else:
                print(f"│  ℹ️ No history deals found")
        else:
            print(f"│  ⚠️ No execution start date - skipping history")
        
        # ============================================================
        # CHECK CURRENT ORDERS & POSITIONS
        # ============================================================
        print(f"│\n├─ 🔄 CHECKING CURRENT STATE".ljust(79) + "┤")
        
        pending_orders = mt5.orders_get() or []
        open_positions = mt5.positions_get() or []
        
        print(f"│  📊 Pending orders: {len(pending_orders)} | Open positions: {len(open_positions)}")
        
        # Find unauthorized items
        unauthorized_orders = []
        unauthorized_positions = []
        
        for order in pending_orders:
            if order.ticket not in authorized_tickets and order.magic not in authorized_magics and order.magic != magic_number:
                unauthorized_orders.append({
                    'ticket': order.ticket,
                    'symbol': order.symbol,
                    'type': order.type,
                    'volume': order.volume_current,
                    'price': order.price_open,
                    'magic': order.magic
                })
        
        for pos in open_positions:
            if pos.ticket not in authorized_tickets and pos.magic not in authorized_magics and pos.magic != magic_number:
                unauthorized_positions.append({
                    'ticket': pos.ticket,
                    'symbol': pos.symbol,
                    'type': 'BUY' if pos.type == 0 else 'SELL',
                    'volume': pos.volume,
                    'price': pos.price_open,
                    'profit': pos.profit,
                    'magic': pos.magic
                })
        
        stats["unauthorized_orders_found"] += len(unauthorized_orders)
        stats["unauthorized_positions_found"] += len(unauthorized_positions)
        
        if unauthorized_orders or unauthorized_positions:
            stats["investors_with_unauthorized"] += 1
            stats["unauthorized_by_investor"][user_brokerid] = {
                'orders': len(unauthorized_orders),
                'positions': len(unauthorized_positions)
            }
            
            print(f"│\n├─ 🚫 UNAUTHORIZED ITEMS FOUND")
            for order in unauthorized_orders[:3]:
                print(f"│     Order #{order['ticket']}: {order['symbol']} @ {order['price']}")
            if len(unauthorized_orders) > 3:
                print(f"│     ... and {len(unauthorized_orders)-3} more orders")
            for pos in unauthorized_positions[:3]:
                print(f"│     Position #{pos['ticket']}: {pos['symbol']} ${pos['profit']:.2f}")
        else:
            print(f"│  ✅ No unauthorized items found")
        
        # ============================================================
        # BUILD TRADES STRUCTURE
        # ============================================================
        trades_structure = {
            "summary": {
                "total_trades": len(authorized_closed_trades),
                "won": won_trades,
                "lost": lost_trades,
                "symbols_that_lost": {k: round(v, 2) for k, v in symbols_lost.items()},
                "symbols_that_won": {k: round(v, 2) for k, v in symbols_won.items()}
            },
            "authorized_closed_trades": authorized_closed_trades
        }
        
        # ============================================================
        # SAVE ACTIVITIES.JSON WITH ALL DATA
        # ============================================================
        print(f"│\n├─ 💾 SAVING ACTIVITIES.JSON".ljust(79) + "┤")
        
        # Prepare activities data
        unauthorized_detected = len(unauthorized_orders) > 0 or len(unauthorized_positions) > 0 or len(unauthorized_trades_list) > 0
        
        # Determine unauthorized types
        unauthorized_type = set()
        if unauthorized_trades_list:
            unauthorized_type.add('trades')
        if unauthorized_orders or unauthorized_positions:
            unauthorized_type.add('positions')
        
        activities_data = {
            'execution_start_date': execution_start_date,
            'broker_balance': starting_balance if starting_balance is not None else 0.0,
            'profitandloss': round(profit_and_loss, 2),
            'current_balance': current_balance,
            'trades': json.dumps(trades_structure),
            'unauthorized_action_detected': unauthorized_detected,
            'bypass_restriction': bypass_active,
            'activate_autotrading': autotrading_active,
            'last_audit_timestamp': datetime.now().isoformat(),
            'history_matching_stats': history_matched,
            'unauthorized_actions': {
                'detected': unauthorized_detected,
                'bypass_active': bypass_active,
                'autotrading_active': autotrading_active,
                'type': list(unauthorized_type) if unauthorized_type else [],
                'unauthorized_trades': unauthorized_trades_list,
                'unauthorized_withdrawals': [],  # Can be populated if needed
                'unauthorized_orders': unauthorized_orders,
                'unauthorized_positions': unauthorized_positions
            },
            'authorized_summary': {
                'tickets': len(authorized_tickets),
                'magics': len(authorized_magics),
                'pending_orders': len(pending_orders),
                'open_positions': len(open_positions),
                'unauthorized_orders': len(unauthorized_orders),
                'unauthorized_positions': len(unauthorized_positions)
            }
        }
        
        # Merge with existing activities if any
        if existing_activities:
            activities_data.update(existing_activities)
            # Override with new data
            activities_data.update({
                'broker_balance': starting_balance if starting_balance is not None else existing_activities.get('broker_balance', 0.0),
                'profitandloss': round(profit_and_loss, 2),
                'current_balance': current_balance,
                'trades': json.dumps(trades_structure),
                'unauthorized_action_detected': unauthorized_detected,
                'last_audit_timestamp': datetime.now().isoformat(),
                'unauthorized_actions': activities_data['unauthorized_actions']
            })
        
        # Save activities.json
        try:
            with open(activities_path, 'w', encoding='utf-8') as f:
                json.dump(activities_data, f, indent=4)
            print(f"│  ✅ activities.json saved")
            print(f"│     • Broker balance: ${activities_data['broker_balance']:.2f}")
            print(f"│     • P&L: ${activities_data['profitandloss']:.2f}")
            print(f"│     • Authorized trades: {trades_structure['summary']['total_trades']}")
            print(f"│     • Unauthorized trades: {len(unauthorized_trades_list)}")
        except Exception as e:
            print(f"│  ❌ Error saving activities.json: {e}")
        
        # ============================================================
        # UPDATE TRADESHISTORY.JSON IF NEEDED
        # ============================================================
        if history_updated > 0:
            try:
                history_list = list(existing_history_dict.values())
                with open(tradeshistory_path, 'w', encoding='utf-8') as f:
                    json.dump(history_list, f, indent=4)
                print(f"│  ✅ tradeshistory.json updated ({len(history_list)} trades)")
            except Exception as e:
                print(f"│  ⚠️ Error saving tradeshistory.json: {e}")
        
        # Update stats
        stats["bypass_active_investors"] += 1 if bypass_active else 0
        stats["autotrading_active_investors"] += 1 if autotrading_active else 0
        
        # Print investor summary
        print(f"│\n├─ 📈 INVESTOR SUMMARY")
        print(f"│  • Starting Balance: ${starting_balance if starting_balance else 0.0:.2f}")
        print(f"│  • Current Balance: ${current_balance:.2f}")
        print(f"│  • P&L: ${profit_and_loss:.2f}")
        print(f"│  • Authorized trades: {len(authorized_closed_trades)} ({won_trades}W/{lost_trades}L)")
        print(f"│  • Unauthorized trades: {len(unauthorized_trades_list)}")
        print(f"│  • Unauthorized items: {len(unauthorized_orders)} orders, {len(unauthorized_positions)} positions")
        
        if bypass_active:
            print(f"│  ⚠️ BYPASS ACTIVE - unauthorized actions allowed")
        elif unauthorized_detected:
            print(f"│  ⛔ RESTRICTIONS ACTIVE - will be moved to issues")
    
    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    print("\n" + "="*80)
    print("  📊 FINAL SUMMARY".ljust(79) + "=")
    print("="*80)
    print(f"│  Investors processed:     {stats['investors_processed']}")
    print(f"│  Unauthorized investors:  {stats['investors_with_unauthorized']}")
    print(f"│  History matched:")
    print(f"│    • Ticket:     {stats['history_matched']['ticket']:>4}")
    print(f"│    • Magic:      {stats['history_matched']['magic']:>4}")
    print(f"│    • Timestamp:  {stats['history_matched']['timestamp']:>4}")
    print(f"│    • Vol+Symbol: {stats['history_matched']['volume_symbol']:>4}")
    print(f"│    • Synthetic:  {stats['history_matched']['synthetic']:>4}")
    print(f"│  History recorded:        {stats['history_orders_recorded']}")
    print(f"│  History updated:         {stats['history_orders_updated']}")
    print(f"│  Unauthorized orders:     {stats['unauthorized_orders_found']}")
    print(f"│  Unauthorized positions:  {stats['unauthorized_positions_found']}")
    print(f"│  Bypass active:           {stats['bypass_active_investors']}")
    print(f"│  Auto-trading active:     {stats['autotrading_active_investors']}")
    
    if stats["unauthorized_by_investor"]:
        print(f"│\n├─ 🚫 UNAUTHORIZED BY INVESTOR")
        for inv_id, counts in stats["unauthorized_by_investor"].items():
            print(f"│    {inv_id}: {counts['orders']} orders, {counts['positions']} positions")
    
    print("="*80 + "\n")
    
    stats["processing_success"] = True
    return stats

def update_investor_info(inv_id=None):
    """
    Updates investor information in UPDATED_INVESTORS.json including:
    - Balance at execution start date (from activities.json)
    - P&L from authorized trades only
    - Trade statistics (won/lost) with negative signs for losses
    - Detailed authorized closed trades list (with buy/sell type)
    - Unauthorized actions detection
    
    Investors with unauthorized actions (and no bypass) are moved to issues_investors.json
    When investors are added to updated_investors.json, their application_status is set to "approved"
    """
    print("\n" + "="*80)
    print("📊 UPDATING INVESTOR INFORMATION")
    print("="*80)
    
    updated_investors_path = Path(UPDATED_INVESTORS)
    issues_investors_path = Path(ISSUES_INVESTORS)
    
    # Load existing updated investors
    if updated_investors_path.exists():
        try:
            with open(updated_investors_path, 'r', encoding='utf-8') as f:
                updated_investors = json.load(f)
        except:
            updated_investors = {}
    else:
        updated_investors = {}
    
    # Load existing issues investors
    if issues_investors_path.exists():
        try:
            with open(issues_investors_path, 'r', encoding='utf-8') as f:
                issues_investors = json.load(f)
        except:
            issues_investors = {}
    else:
        issues_investors = {}
    
    investor_ids = [inv_id] if inv_id else list(usersdictionary.keys())
    
    for user_brokerid in investor_ids:
        print(f"\n{'='*60}")
        print(f"📋 INVESTOR: {user_brokerid}")
        print(f"{'='*60}")
        
        if user_brokerid not in usersdictionary:
            print(f"   ❌ Investor {user_brokerid} not found in usersdictionary")
            continue
            
        base_info = usersdictionary[user_brokerid].copy()
        inv_root = Path(INV_PATH) / user_brokerid
        
        if not inv_root.exists():
            print(f"   ❌ Path not found: {inv_root}")
            continue
        
        # ============================================================
        # READ FROM ACTIVITIES.JSON (Primary Source)
        # ============================================================
        activities_path = inv_root / "activities.json"
        
        if not activities_path.exists():
            print(f"   ⚠️  activities.json not found in {inv_root} - skipping investor")
            continue
        
        try:
            with open(activities_path, 'r', encoding='utf-8') as f:
                activities = json.load(f)
            print(f"   ✅ Loaded activities.json")
        except Exception as e:
            print(f"   ❌ Error reading activities.json: {e}")
            continue
        
        # Extract data from activities.json
        execution_start_date = activities.get('execution_start_date')
        starting_balance = activities.get('broker_balance', 0.0)
        profit_and_loss = activities.get('profitandloss', 0.0)
        current_balance = activities.get('current_balance', 0.0)
        bypass_active = activities.get('bypass_restriction', False)
        autotrading_active = activities.get('activate_autotrading', False)
        unauthorized_detected = activities.get('unauthorized_action_detected', False)
        last_audit_timestamp = activities.get('last_audit_timestamp', '')
        
        # Extract trades data (stored as JSON string in activities.json)
        trades_data = {}
        trades_json_str = activities.get('trades', '{}')
        if trades_json_str:
            try:
                if isinstance(trades_json_str, str):
                    trades_data = json.loads(trades_json_str)
                else:
                    trades_data = trades_json_str
                print(f"   📊 Loaded trades data from activities.json")
            except Exception as e:
                print(f"   ⚠️  Error parsing trades JSON: {e}")
                trades_data = {}
        
        # Extract unauthorized actions
        unauthorized_actions = activities.get('unauthorized_actions', {})
        unauthorized_type = unauthorized_actions.get('type', [])
        unauthorized_trades_list = unauthorized_actions.get('unauthorized_trades', [])
        unauthorized_orders = unauthorized_actions.get('unauthorized_orders', [])
        unauthorized_positions = unauthorized_actions.get('unauthorized_positions', [])
        unauthorized_withdrawals = unauthorized_actions.get('unauthorized_withdrawals', [])
        
        # Extract authorized summary
        authorized_summary = activities.get('authorized_summary', {})
        
        # Extract trades summary
        trades_summary = trades_data.get('summary', {})
        authorized_closed_trades = trades_data.get('authorized_closed_trades', [])
        
        # Get trade statistics
        won_trades = trades_summary.get('won', 0)
        lost_trades = trades_summary.get('lost', 0)
        symbols_won = trades_summary.get('symbols_that_won', {})
        symbols_lost = trades_summary.get('symbols_that_lost', {})
        
        # Print extracted information
        print(f"\n   📋 ACTIVITIES.JSON DATA:")
        print(f"      • Execution Start Date: {execution_start_date}")
        print(f"      • Starting Balance: ${starting_balance:.2f}")
        print(f"      • Current Balance: ${current_balance:.2f}")
        print(f"      • P&L: ${profit_and_loss:.2f}")
        print(f"      • Bypass Active: {bypass_active}")
        print(f"      • Auto-trading Active: {autotrading_active}")
        print(f"      • Unauthorized Detected: {unauthorized_detected}")
        print(f"      • Last Audit: {last_audit_timestamp}")
        
        print(f"\n   📊 TRADES SUMMARY:")
        print(f"      • Total Authorized Trades: {len(authorized_closed_trades)}")
        print(f"      • Won Trades: {won_trades}")
        print(f"      • Lost Trades: {lost_trades}")
        if symbols_won:
            print(f"      • Symbols Won: {symbols_won}")
        if symbols_lost:
            print(f"      • Symbols Lost: {symbols_lost}")
        
        print(f"\n   🚫 UNAUTHORIZED ACTIONS:")
        print(f"      • Type: {unauthorized_type if unauthorized_type else 'None'}")
        print(f"      • Unauthorized Trades: {len(unauthorized_trades_list)}")
        print(f"      • Unauthorized Orders: {len(unauthorized_orders)}")
        print(f"      • Unauthorized Positions: {len(unauthorized_positions)}")
        print(f"      • Unauthorized Withdrawals: {len(unauthorized_withdrawals)}")
        
        # ============================================================
        # READ FROM TRADESHISTORY.JSON (if needed for additional data)
        # ============================================================
        history_path = inv_root / "tradeshistory.json"
        authorized_tickets_count = 0
        
        if history_path.exists():
            try:
                with open(history_path, 'r', encoding='utf-8') as f:
                    history_trades = json.load(f)
                authorized_tickets_count = len(history_trades) if isinstance(history_trades, list) else len(history_trades.keys())
                print(f"\n   📚 TRADESHISTORY.JSON: {authorized_tickets_count} authorized tickets")
            except Exception as e:
                print(f"   ⚠️  Error reading tradeshistory.json: {e}")
        
        # ============================================================
        # READ FROM ACCOUNTMANAGEMENT.JSON (fallback for some data)
        # ============================================================
        acc_mgmt_path = inv_root / "accountmanagement.json"
        magic_number = 0
        
        if acc_mgmt_path.exists():
            try:
                with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                    acc_config = json.load(f)
                    magic_number = acc_config.get('magic_number', 0)
                print(f"   ⚙️  Magic Number from accountmanagement.json: {magic_number}")
            except Exception as e:
                print(f"   ⚠️  Error reading accountmanagement.json: {e}")
        
        # ============================================================
        # CONTRACT DAYS CALCULATION
        # ============================================================
        contract_days_left = "30"
        if execution_start_date:
            try:
                start = None
                for fmt in ["%Y-%m-%d", "%B %d, %Y", "%Y/%m/%d"]:
                    try: 
                        start = datetime.strptime(execution_start_date, fmt)
                        break
                    except: 
                        continue
                if start:
                    days_passed = (datetime.now() - start).days
                    contract_days_left = str(max(0, 30 - days_passed))
                    print(f"\n   📅 Contract Days Left: {contract_days_left} (Started: {execution_start_date}, Days passed: {days_passed})")
            except Exception as e:
                print(f"   ⚠️  Error calculating contract days: {e}")
        
        # ============================================================
        # BUILD TRADES STRUCTURE
        # ============================================================
        trades_info = {
            "summary": {
                "total_trades": len(authorized_closed_trades),
                "won": won_trades,
                "lost": lost_trades,
                "symbols_that_lost": {k: round(float(v), 2) for k, v in symbols_lost.items()},
                "symbols_that_won": {k: round(float(v), 2) for k, v in symbols_won.items()}
            },
            "authorized_closed_trades": authorized_closed_trades
        }
        
        # ============================================================
        # BUILD INVESTOR INFO DICTIONARY
        # ============================================================
        investor_info = {
            "id": user_brokerid,
            "server": base_info.get("SERVER", base_info.get("server", "")),
            "login": base_info.get("LOGIN_ID", base_info.get("login", "")),
            "password": base_info.get("PASSWORD", base_info.get("password", "")),
            "application_status": "approved",  # Will be overridden if moved to issues
            "broker_balance": round(float(starting_balance), 2) if starting_balance is not None else 0.0,
            "profitandloss": round(float(profit_and_loss), 2),
            "contract_days_left": contract_days_left,
            "execution_start_date": execution_start_date if execution_start_date else "",
            "last_audit_timestamp": last_audit_timestamp,
            "current_balance": round(float(current_balance), 2),
            "trades": trades_info,
            "authorized_tickets_count": authorized_tickets_count,
            "magic_number": magic_number,
            "unauthorized_actions": {
                "detected": unauthorized_detected,
                "bypass_active": bypass_active,
                "autotrading_active": autotrading_active,
                "type": unauthorized_type if unauthorized_type else [],
                "unauthorized_trades": unauthorized_trades_list,
                "unauthorized_withdrawals": unauthorized_withdrawals,
                "unauthorized_orders": unauthorized_orders,
                "unauthorized_positions": unauthorized_positions
            }
        }
        
        # ============================================================
        # DETERMINE IF INVESTOR SHOULD BE MOVED TO ISSUES
        # ============================================================
        should_move_to_issues = False
        issue_message = ""
        
        if unauthorized_detected:
            if bypass_active:
                print(f"\n   ⚠️  UNAUTHORIZED ACTIONS DETECTED BUT BYPASS ACTIVE")
                print(f"      → Keeping in updated_investors.json (bypass enabled)")
                should_move_to_issues = False
                investor_info['application_status'] = "approved_with_bypass"
                investor_info['bypass_note'] = "Unauthorized actions detected but bypass is active"
            else:
                should_move_to_issues = True
                issue_message = "Unauthorized action detected - restricted (bypass inactive)"
                print(f"\n   ⛔ UNAUTHORIZED ACTIONS DETECTED WITHOUT BYPASS")
                print(f"      → MOVING TO ISSUES INVESTORS")
                print(f"      → Reason: {issue_message}")
        else:
            print(f"\n   ✅ NO UNAUTHORIZED ACTIONS DETECTED")
            print(f"      → Keeping in updated_investors.json")
            investor_info['application_status'] = "approved"
        
        # ============================================================
        # MOVE TO ISSUES OR UPDATE UPDATED INVESTORS
        # ============================================================
        if should_move_to_issues:
            investor_info['message'] = issue_message
            investor_info['application_status'] = "rejected"
            
            # Remove from updated_investors if exists
            if user_brokerid in updated_investors:
                del updated_investors[user_brokerid]
                print(f"   🗑️  Removed from updated_investors.json")
            
            # Add to issues_investors
            issues_investors[user_brokerid] = investor_info
            print(f"   📝 Added to issues_investors.json")
            
        else:
            # Update or add to updated_investors
            updated_investors[user_brokerid] = investor_info
            print(f"\n   ✅ INVESTOR SUMMARY (Added to updated_investors.json):")
            print(f"      • Application Status: {investor_info['application_status']}")
            print(f"      • Starting Balance: ${investor_info['broker_balance']:.2f}")
            print(f"      • Current Balance: ${investor_info['current_balance']:.2f}")
            print(f"      • P&L: ${investor_info['profitandloss']:.2f}")
            print(f"      • Authorized Trades: {trades_info['summary']['total_trades']} ({won_trades}W/{lost_trades}L)")
            print(f"      • Unauthorized Detected: {unauthorized_detected}")
            if bypass_active:
                print(f"      • ⚠️  BYPASS ACTIVE - unauthorized actions allowed")
            print(f"      • Contract Days Left: {contract_days_left}")
        
        print(f"\n{'-'*60}")
    
    # ============================================================
    # SAVE UPDATED INVESTORS JSON
    # ============================================================
    try:
        with open(updated_investors_path, 'w', encoding='utf-8') as f:
            json.dump(updated_investors, f, indent=4)
        print(f"\n✅ Saved updated_investors.json with {len(updated_investors)} investors")
    except Exception as e:
        print(f"\n❌ Failed to save updated_investors.json: {e}")
    
    # ============================================================
    # SAVE ISSUES INVESTORS JSON
    # ============================================================
    try:
        with open(issues_investors_path, 'w', encoding='utf-8') as f:
            json.dump(issues_investors, f, indent=4)
        print(f"✅ Saved issues_investors.json with {len(issues_investors)} investors")
    except Exception as e:
        print(f"❌ Failed to save issues_investors.json: {e}")
    
    # ============================================================
    # FINAL SUMMARY
    # ============================================================
    print("\n" + "="*80)
    print("📊 FINAL UPDATE SUMMARY")
    print("="*80)
    print(f"   • Total investors processed: {len(investor_ids)}")
    print(f"   • Updated investors: {len(updated_investors)}")
    print(f"   • Issues investors: {len(issues_investors)}")
    print(f"   • Active bypass investors: {sum(1 for v in updated_investors.values() if v.get('unauthorized_actions', {}).get('bypass_active', False))}")
    print("="*80 + "\n")
    
    return updated_investors
#---         ----           --#


#---   #####  ----#
def fetch_ohlc_data_for_investor(inv_id):
    """
    Fetch OHLCV data and generate charts for a specific investor.
    This function combines all OHLCV/chart generation functionality into one.
    
    ASSUMES: MT5 is already initialized and logged in by the caller (process_single_investor)
    
    Parameters:
    - inv_id: The investor ID to process
    
    Returns:
    - dict: Processing results including counts, errors, and status
    """
    
    
    # =========================================================================
    # CONSTANTS
    # =========================================================================
    
    # =========================================================================
    # HELPER FUNCTIONS (nested within main function)
    # =========================================================================
    
    def save_errors(error_log):
        """Save error log to JSON file."""
        try:
            os.makedirs(BASE_ERROR_FOLDER, exist_ok=True)
            with open(ERROR_JSON_PATH, 'w') as f:
                json.dump(error_log, f, indent=4)
        except Exception as e:
            print(f"Failed to save error log: {str(e)}")
            
    def load_investor_users():
        """Load investor users config from JSON file."""
        INVESTOR_USERS_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json"
        
        if not os.path.exists(INVESTOR_USERS_PATH):
            print(f"CRITICAL: {INVESTOR_USERS_PATH} NOT FOUND! Using empty config.")
            return {}

        try:
            with open(INVESTOR_USERS_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Convert numeric strings back to int where needed
            for investor_id, cfg in data.items():
                if "LOGIN_ID" in cfg and isinstance(cfg["LOGIN_ID"], str):
                    cfg["LOGIN_ID"] = cfg["LOGIN_ID"].strip()
                
                # Extract target folder from INVESTED_WITH (text after underscore)
                if "INVESTED_WITH" in cfg:
                    invested_with = cfg["INVESTED_WITH"]
                    if "_" in invested_with:
                        target_folder = invested_with.split("_", 1)[1]
                        cfg["TARGET_FOLDER"] = target_folder
                    else:
                        cfg["TARGET_FOLDER"] = invested_with
            
            return data

        except json.JSONDecodeError as e:
            print(f"Invalid JSON in investors.json: {e}")
            return {}
        except Exception as e:
            print(f"Failed to load investors.json: {e}")
            return {}
    
    def load_accountmanagement(investor_id):
        """Load account management config for a specific investor."""
        accountmanagement_path = os.path.join(INV_PATH, investor_id, "accountmanagement.json")
        
        if not os.path.exists(accountmanagement_path):
            print(f"  ⚠️  Investor {investor_id} | accountmanagement.json not found")
            return None, None
        
        try:
            with open(accountmanagement_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract bars value if present
            bars = data.get("bars")
            if bars is None:
                print(f"  ⚠️  Investor {investor_id} | 'bars' not defined in accountmanagement.json")
                return None, None
            
            # Validate bars is a positive integer
            if not isinstance(bars, int) or bars <= 0:
                print(f"  ⚠️  Investor {investor_id} | 'bars' must be a positive integer, got: {bars}")
                return None, None
            
            # Extract timeframe list (dynamic)
            timeframes = data.get("timeframe")
            if timeframes is None:
                print(f"  ⚠️  Investor {investor_id} | 'timeframe' not defined in accountmanagement.json")
                return None, None
            
            # Validate timeframe is a list
            if not isinstance(timeframes, list):
                print(f"  ⚠️  Investor {investor_id} | 'timeframe' must be a list, got: {type(timeframes)}")
                return None, None
            
            # Validate each timeframe is supported
            valid_timeframes = []
            for tf in timeframes:
                if tf in TIMEFRAME_MAP:
                    valid_timeframes.append(tf)
                else:
                    print(f"  ⚠️  Investor {investor_id} | Unsupported timeframe '{tf}', skipping")
            
            if not valid_timeframes:
                print(f"  ❌  Investor {investor_id} | No valid timeframes provided")
                return None, None
            
            print(f"  📊  Investor {investor_id} | Using bars={bars}, timeframes={valid_timeframes}")
            return bars, valid_timeframes
            
        except json.JSONDecodeError as e:
            print(f"  ❌  Investor {investor_id} | Invalid JSON in accountmanagement.json: {e}")
            return None, None
        except Exception as e:
            print(f"  ❌  Investor {investor_id} | Failed to load accountmanagement.json: {e}")
            return None, None
    
    def load_investor_symbols(investor_id):
        """Load symbols from accountmanagement.json for a specific investor."""
        accountmanagement_path = os.path.join(INV_PATH, investor_id, "accountmanagement.json")
        
        if not os.path.exists(accountmanagement_path):
            print(f"  ⚠️  Investor {investor_id} | accountmanagement.json not found")
            return []
        
        try:
            with open(accountmanagement_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Extract symbols from symbols_dictionary
            symbols_dict = data.get("symbols_dictionary", {})
            
            if not symbols_dict:
                print(f"  ⚠️  Investor {investor_id} | No symbols_dictionary found in accountmanagement.json")
                return []
            
            # Collect all symbols from all arrays in the dictionary
            all_symbols = []
            for category, symbol_list in symbols_dict.items():
                if isinstance(symbol_list, list):
                    all_symbols.extend(symbol_list)
                elif isinstance(symbol_list, str):
                    all_symbols.append(symbol_list)
            
            # Remove duplicates while preserving order
            unique_symbols = []
            seen = set()
            for symbol in all_symbols:
                if symbol not in seen:
                    unique_symbols.append(symbol)
                    seen.add(symbol)
            
            print(f"  📊  Investor {investor_id} | Loaded {len(unique_symbols)} symbols from accountmanagement.json")
            return unique_symbols
            
        except json.JSONDecodeError as e:
            print(f"  ❌  Investor {investor_id} | Invalid JSON in accountmanagement.json: {e}")
            return []
        except Exception as e:
            print(f"  ❌  Investor {investor_id} | Failed to load symbols from accountmanagement.json: {e}")
            return []
    
    def fetch_ohlcv_data(symbol, mt5_timeframe, bars):
        """Fetch OHLCV data including the currently forming candle (index 0)."""
        error_log = []
        lagos_tz = pytz.timezone('Africa/Lagos')
        timestamp = datetime.now(lagos_tz).strftime('%Y-%m-%d %H:%M:%S.%f%z')

        # --- Step 1: Ensure symbol is selected ---
        selected = False
        for attempt in range(3):
            if mt5.symbol_select(symbol, True):
                selected = True
                break
            time.sleep(0.5)

        if not selected:
            last_err = mt5.last_error()
            err_msg = f"FAILED symbol_select('{symbol}'): {last_err}"
            print(err_msg)
            return None, [{"error": err_msg, "timestamp": timestamp}]

        # --- Step 2: Fetch rates ---
        rates = mt5.copy_rates_from_pos(symbol, mt5_timeframe, 0, bars)

        if rates is None or len(rates) == 0:
            last_err = mt5.last_error()
            err_msg = f"No data for {symbol}: {last_err}"
            print(err_msg)
            return None, [{"error": err_msg, "timestamp": timestamp}]

        available_bars = len(rates)
        
        # Convert to DataFrame
        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s")
        df = df.set_index("time")

        # Standardize dtypes
        df = df.astype({
            "open": float, "high": float, "low": float, "close": float,
            "tick_volume": float, "spread": int, "real_volume": float
        })
        df.rename(columns={"tick_volume": "volume"}, inplace=True)

        print(f"Fetched {available_bars} bars (including live candle) for {symbol}")
        return df, error_log
    
    def save_newest_oldest_df(df, symbol, timeframe_str, base_output_dir):
        """Save candles directly to base directory with filename: {symbol}_{timeframe}_candledetails.json"""
        error_log = []
        
        # Create filename with symbol and timeframe
        filename = f"{symbol}_{timeframe_str}_candledetails.json"
        file_path = os.path.join(base_output_dir, filename)
        
        lagos_tz = pytz.timezone('Africa/Lagos')
        now = datetime.now(lagos_tz)

        try:
            if len(df) < 2:
                error_msg = f"Not enough data for {symbol} ({timeframe_str})"
                print(error_msg)
                error_log.append({"error": error_msg, "timestamp": now.isoformat()})
                save_errors(error_log)
                return error_log

            # Prepare all candles (oldest first, newest last)
            all_candles = []
            for i, (ts, row) in enumerate(df.iterrows()):
                candle = row.to_dict()
                candle.update({
                    "time": ts.strftime('%Y-%m-%d %H:%M:%S'),
                    "candle_number": i,  # 0 = oldest
                    "symbol": symbol,
                    "timeframe": timeframe_str
                })
                all_candles.append(candle)

            # Save all candles to single JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(all_candles, f, indent=4)

            print(f"✓ {symbol} {timeframe_str} | JSON saved to {filename} | {len(all_candles)} candles")

        except Exception as e:
            err = f"save_newest_oldest_df failed: {str(e)}"
            print(err)
            error_log.append({"error": err, "timestamp": now.isoformat()})
            save_errors(error_log)

        return error_log
    
    def generate_and_save_chart(df, symbol, timeframe_str, base_output_dir):
        """Generate and save chart with filename: {symbol}_{timeframe}_chart.png directly in base directory."""
        error_log = []
        
        # Create filename with symbol and timeframe
        filename = f"{symbol}_{timeframe_str}_chart.png"
        chart_path = os.path.join(base_output_dir, filename)
        
        try:
            # Dynamic width calculation
            num_candles = len(df)
            
            # Configuration for readable candles
            MIN_CANDLE_WIDTH = 20
            MAX_CANDLE_WIDTH = 40
            MIN_CANDLE_SPACING = 10
            BASE_HEIGHT = 30
            MAX_IMAGE_WIDTH = 90000000
            
            # Determine optimal candle width based on number of candles
            if num_candles <= 50:
                base_candle_width = 30
                base_spacing_multiplier = 1.8
            elif num_candles <= 200:
                base_candle_width = 20
                base_spacing_multiplier = 1.6
            elif num_candles <= 1000:
                base_candle_width = 12
                base_spacing_multiplier = 1.4
            else:
                base_candle_width = MIN_CANDLE_WIDTH
                base_spacing_multiplier = 1.3
            
            # Apply constraints to candle width
            target_candle_width = max(base_candle_width, MIN_CANDLE_WIDTH)
            target_candle_width = min(target_candle_width, MAX_CANDLE_WIDTH)
            
            # Calculate spacing based on candle width and multiplier
            desired_spacing = target_candle_width * base_spacing_multiplier
            actual_spacing = max(desired_spacing, MIN_CANDLE_SPACING)
            
            # Calculate total width needed in pixels
            if num_candles > 1:
                total_width_pixels = actual_spacing * (num_candles - 1) + target_candle_width
            else:
                total_width_pixels = target_candle_width * 2
            
            # Add padding for margins
            padding_pixels = 200
            img_width_pixels = int(total_width_pixels + padding_pixels)
            img_width_pixels = min(img_width_pixels, MAX_IMAGE_WIDTH)
            
            min_width_pixels = 800
            if img_width_pixels < min_width_pixels:
                img_width_pixels = min_width_pixels
            
            # Convert pixels to inches for matplotlib
            img_width_inches = img_width_pixels / 100
            
            print(f"📊 {symbol} {timeframe_str} | {num_candles} candles → {img_width_pixels}px")
            
            # Chart style
            custom_style = mpf.make_mpf_style(
                base_mpl_style="default",
                marketcolors=mpf.make_marketcolors(
                    up="green", down="red", edge="inherit",
                    wick={"up": "green", "down": "red"}, volume="gray"
                )
            )

            # Check DataFrame columns
            required_cols = ['Open', 'High', 'Low', 'Close']
            df_cols = df.columns.tolist()
            
            col_mapping = {}
            for req_col in required_cols:
                found = False
                for df_col in df_cols:
                    if df_col.lower() == req_col.lower():
                        col_mapping[req_col] = df_col
                        found = True
                        break
                if not found:
                    raise KeyError(f"Required column '{req_col}' not found. Available: {df_cols}")
            
            if col_mapping:
                df_plot = df.rename(columns={v: k for k, v in col_mapping.items()})
            else:
                df_plot = df

            # Generate and save chart
            fig, axlist = mpf.plot(
                df_plot, 
                type='candle', 
                style=custom_style, 
                volume=False,
                title=f"{symbol} ({timeframe_str}) - {num_candles} candles", 
                returnfig=True,
                warn_too_much_data=5000,
                figsize=(img_width_inches, BASE_HEIGHT),
                scale_padding={'left': 0.5, 'right': 1.5, 'top': 0.5, 'bottom': 0.5}
            )
            
            fig.set_size_inches(img_width_inches, BASE_HEIGHT)
            
            for ax in axlist:
                ax.grid(False)
                for line in ax.get_lines():
                    if line.get_label() == '':
                        line.set_linewidth(0.5)

            fig.savefig(chart_path, bbox_inches="tight", dpi=100)
            plt.close(fig)

            print(f"✓ {symbol} {timeframe_str} | Chart saved to {filename} | {num_candles} candles")
            return chart_path, error_log

        except KeyError as e:
            print(f"Error in chart generation - column error: {e}")
            error_log.append(str(e))
            return None, error_log
        except Exception as e:
            print(f"Error in chart generation: {e}")
            error_log.append(str(e))
            return None, error_log
    
    def get_symbols_from_mt5():
        """Retrieve all available symbols from MT5."""
        error_log = []
        symbols = mt5.symbols_get()
        if not symbols:
            error_log.append({
                "timestamp": datetime.now(pytz.timezone('Africa/Lagos')).strftime('%Y-%m-%d %H:%M:%S.%f+01:00'),
                "error": f"Failed to retrieve symbols: {mt5.last_error()}",
                "broker": mt5.terminal_info().name if mt5.terminal_info() else "unknown"
            })
            save_errors(error_log)
            print(f"Failed to retrieve symbols: {mt5.last_error()}")
            return [], error_log

        available_symbols = [s.name for s in symbols]
        print(f"Retrieved {len(available_symbols)} symbols")
        return available_symbols, error_log
    
    def process_account_worker(investor_id, symbol_list, investor_timeframe_map, bars, base_output_dir):
        """Process symbols for a single investor."""
        processed_count = 0
        
        for symbol in symbol_list:
            try:
                print(f"  📈 Investor {investor_id} | Processing | {symbol} | bars={bars} | timeframes={list(investor_timeframe_map.keys())}")

                # Process only the timeframes specified in accountmanagement.json
                for tf_str, mt5_tf in investor_timeframe_map.items():
                    df, _ = fetch_ohlcv_data(symbol, mt5_tf, bars)
                    if df is not None and not df.empty:
                        df["symbol"] = symbol
                        
                        # Save candle details directly to base directory
                        save_newest_oldest_df(df, symbol, tf_str, base_output_dir)
                        
                        # Generate and save chart directly to base directory
                        chart_path, _ = generate_and_save_chart(df, symbol, tf_str, base_output_dir)
                        
                processed_count += 1
                print(f"  ✅ Investor {investor_id} | Completed | {symbol}")
                
            except Exception as e:
                print(f"  ❌ Investor {investor_id} | Error on {symbol}: {str(e)[:100]}")
                continue
        
        return processed_count
    
    # =========================================================================
    # MAIN EXECUTION FOR THE INVESTOR
    # =========================================================================
    
    print(f"\n{'='*80}")
    print(f"📊 FETCHING OHLCV DATA FOR INVESTOR: {inv_id}")
    print(f"{'='*80}")
    
    result = {
        'investor_id': inv_id,
        'success': False,
        'symbols_processed': 0,
        'total_symbols': 0,
        'errors': [],
        'output_directory': None
    }
    
    try:
        # Step 1: Load investor users configuration
        investor_users = load_investor_users()
        
        # Step 2: Get investor config
        investor_cfg = investor_users.get(inv_id)
        if not investor_cfg:
            print(f"  ❌  Investor {inv_id} | Config not found in investors.json")
            result['errors'].append("Investor config not found")
            return result
        
        # Step 3: Get target folder from INVESTED_WITH
        target_folder = investor_cfg.get("TARGET_FOLDER")
        if not target_folder:
            print(f"  ❌  Investor {inv_id} | SKIPPED - No TARGET_FOLDER extracted from INVESTED_WITH")
            result['errors'].append("No TARGET_FOLDER found")
            return result
        
        # Step 4: Load bars and timeframes from accountmanagement.json
        bars, timeframes = load_accountmanagement(inv_id)
        
        if bars is None or timeframes is None:
            print(f"  ❌  Investor {inv_id} | SKIPPED - Missing 'bars' or 'timeframe' in accountmanagement.json")
            result['errors'].append("Missing bars or timeframe configuration")
            return result
        
        # Step 5: Load symbols from accountmanagement.json
        symbol_list = load_investor_symbols(inv_id)
        
        if not symbol_list:
            print(f"  ⚠️  Investor {inv_id} | No symbols to process")
            result['errors'].append("No symbols found")
            return result
        
        result['total_symbols'] = len(symbol_list)
        
        # Step 6: Build dynamic timeframe map for this investor
        investor_timeframe_map = {}
        for tf in timeframes:
            if tf in TIMEFRAME_MAP:
                investor_timeframe_map[tf] = TIMEFRAME_MAP[tf]
        
        # Step 7: Create base output directory
        base_output_dir = os.path.join(INV_PATH, inv_id, target_folder)
        os.makedirs(base_output_dir, exist_ok=True)
        result['output_directory'] = base_output_dir
        
        # Step 8: MT5 is already initialized and logged in by process_single_investor
        print(f"  ✅ Using existing MT5 connection (initialized by process_single_investor)")
        
        # Step 9: Validate symbols against MT5 availability
        mt5_symbols, _ = get_symbols_from_mt5()
        valid_symbols = [sym for sym in symbol_list if sym in mt5_symbols]
        invalid_count = len(symbol_list) - len(valid_symbols)
        
        if invalid_count > 0:
            print(f"  ⚠️  Investor {inv_id} | {len(valid_symbols)} valid / {len(symbol_list)} total symbols ({invalid_count} invalid)")
        
        if not valid_symbols:
            print(f"  ⚠️  Investor {inv_id} | No valid symbols to process")
            result['errors'].append("No valid symbols found on MT5")
            return result
        
        # Step 10: Process the symbols
        processed_count = process_account_worker(
            inv_id, 
            valid_symbols, 
            investor_timeframe_map, 
            bars, 
            base_output_dir
        )
        
        result['symbols_processed'] = processed_count
        result['success'] = processed_count > 0
        
        print(f"\n  🏁 Investor {inv_id} | Finished | {processed_count}/{len(valid_symbols)} symbols processed\n")
        
        return result
        
    except Exception as e:
        print(f"  ❌ Investor {inv_id} | Error in fetch_ohlc_data_for_investor: {str(e)}")
        traceback.print_exc()
        result['errors'].append(str(e))
        return result

def directional_bias(inv_id=None):
    """
    Analyze directional bias based on the 2 most recent completed candles.
    Reads candle data from pre-generated JSON files.
    
    Checks for bullish (both green candles with higher highs/higher lows) or 
    bearish (both red candles with lower highs/lower lows) patterns.
    
    Timeframe is read from accountmanagement.json "timeframe" field (can be string or list).
    Supported timeframes: 1m, 5m, 15m, 30m, 45m, 1h, 2h, 4h (4h is max)
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics and signals for directional bias
    """
    print(f"\n{'='*10} 🧭  DIRECTIONAL BIAS ANALYSIS {'='*10}")
    if inv_id:
        print(f" Processing investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols": 0,
        "successful_symbols": 0,
        "failed_symbols": 0,
        "bullish_signals": 0,
        "bearish_signals": 0,
        "total_signals": 0,
        "signals_generated": False,
        "timeframes_used": []
    }
    
    def get_candle_center(candle):
        """Calculate center price of a candle's BODY (open + close) / 2"""
        return (candle['open'] + candle['close']) / 2
    
    def normalize_symbol_for_filename(raw_symbol):
        """Remove special characters from symbol for filename"""
        normalized = raw_symbol.replace('+', '').replace('-', '').replace('.', '')
        return normalized
    
    def load_investor_config(investor_id):
        """Load investor configuration from accountmanagement.json"""
        acc_mgmt_path = Path(INV_PATH) / investor_id / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f" [{investor_id}] ❌ Account management file not found")
            return None, None, None, None
        
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Get symbols dictionary
            symbols_dict = config.get("symbols_dictionary", {})
            if not symbols_dict:
                print(f" [{investor_id}] ⚠️ No symbols_dictionary found")
                return None, None, None, None
            
            # Get timeframe (can be string or list)
            timeframe_config = config.get("timeframe", "15m")
            
            # Convert to list if it's a string
            if isinstance(timeframe_config, str):
                timeframes = [timeframe_config]
            elif isinstance(timeframe_config, list):
                timeframes = timeframe_config
            else:
                print(f" [{investor_id}] ⚠️ Invalid timeframe format: {type(timeframe_config)}")
                return None, None, None, None
            
            # Validate timeframes
            valid_timeframes = []
            for tf in timeframes:
                if tf in TIMEFRAME_MAP:
                    valid_timeframes.append(tf)
                else:
                    print(f" [{investor_id}] ⚠️ Unsupported timeframe '{tf}', skipping")
            
            if not valid_timeframes:
                print(f" [{investor_id}] ❌ No valid timeframes provided")
                return None, None, None, None
            
            print(f" [{investor_id}] 📊 Using timeframes: {valid_timeframes}")
            
            # Get selected risk reward
            selected_risk_reward = config.get("selected_risk_reward", [1])
            if isinstance(selected_risk_reward, list) and len(selected_risk_reward) > 0:
                risk_reward = selected_risk_reward[0]  # Use first value from list
            else:
                risk_reward = 1  # Default value
            
            print(f" [{investor_id}] 📈 Risk/Reward: {risk_reward}")
            
            # Get target folder from investor config
            investor_users_path = Path(r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\investors.json")
            target_folder = None
            
            if investor_users_path.exists():
                with open(investor_users_path, 'r', encoding='utf-8') as f:
                    investor_users = json.load(f)
                
                investor_cfg = investor_users.get(investor_id)
                if investor_cfg:
                    invested_with = investor_cfg.get("INVESTED_WITH", "")
                    if "_" in invested_with:
                        target_folder = invested_with.split("_", 1)[1]
                    else:
                        target_folder = invested_with
            
            if not target_folder:
                print(f" [{investor_id}] ⚠️ No TARGET_FOLDER found, using 'prices'")
                target_folder = "prices"
            
            return symbols_dict, valid_timeframes, target_folder, risk_reward
            
        except Exception as e:
            print(f" [{investor_id}] ❌ Error loading config: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None, None
    
    def load_candle_data(investor_id, symbol, timeframe, target_folder):
        """Load candle data from pre-generated JSON file"""
        normalized_symbol = normalize_symbol_for_filename(symbol)
        
        filename = f"{normalized_symbol}_{timeframe}_candledetails.json"
        file_path = Path(INV_PATH) / investor_id / target_folder / filename
        
        if not file_path.exists():
            filename_original = f"{symbol}_{timeframe}_candledetails.json"
            file_path = Path(INV_PATH) / investor_id / target_folder / filename_original
            
            if not file_path.exists():
                return None, f"File not found: {filename} or {filename_original}"
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                candles = json.load(f)
            
            if len(candles) < 3:
                return None, f"Only {len(candles)} candles available (need at least 3)"
            
            current_candle = candles[-1]
            candle_2 = candles[-2]
            candle_1 = candles[-3]
            
            return {
                'current': current_candle,
                'candle_2': candle_2,
                'candle_1': candle_1
            }, None
            
        except Exception as e:
            return None, f"Error reading {filename}: {e}"
    
    def print_candle_details(symbol, timeframe, candle_data, digits):
        """Print detailed candle information"""
        current = candle_data['current']
        candle_2 = candle_data['candle_2']
        candle_1 = candle_data['candle_1']
        
        print(f"\n  📊 {symbol} [{timeframe}]")
        print(f"  ┌{'─' * 65}")
        print(f"  │ 🔄 CURRENT FORMING CANDLE:")
        print(f"  │    Time: {current.get('time', 'N/A')}")
        print(f"  │    Open: {current.get('open', 0):.{digits}f}")
        print(f"  │    High: {current.get('high', 0):.{digits}f}")
        print(f"  │    Low:  {current.get('low', 0):.{digits}f}")
        print(f"  │    Close:{current.get('close', 0):.{digits}f}")
        print(f"  │")
        
        candle_type = "🟢 BULLISH" if candle_2['close'] > candle_2['open'] else "🔴 BEARISH"
        print(f"  │ ✅ CANDLE 2 (Most Recent Completed): {candle_type}")
        print(f"  │    Time: {candle_2.get('time', 'N/A')}")
        print(f"  │    Open: {candle_2['open']:.{digits}f}")
        print(f"  │    High: {candle_2['high']:.{digits}f}")
        print(f"  │    Low:  {candle_2['low']:.{digits}f}")
        print(f"  │    Close:{candle_2['close']:.{digits}f}")
        print(f"  │")
        
        candle_type = "🟢 BULLISH" if candle_1['close'] > candle_1['open'] else "🔴 BEARISH"
        print(f"  │ 📊 CANDLE 1 (Second Most Recent): {candle_type}")
        print(f"  │    Time: {candle_1.get('time', 'N/A')}")
        print(f"  │    Open: {candle_1['open']:.{digits}f}")
        print(f"  │    High: {candle_1['high']:.{digits}f}")
        print(f"  │    Low:  {candle_1['low']:.{digits}f}")
        print(f"  │    Close:{candle_1['close']:.{digits}f}")
        print(f"  └{'─' * 65}")
    
    def calculate_exit_price(bias_type, candle_data, digits):
        """
        Calculate exit price based on bias type and current forming candle
        
        For bullish: 
          - Default exit: high price of candle 2
          - If current candle's high > candle 2 high, use current candle's high instead
        
        For bearish:
          - Default exit: low price of candle 2
          - If current candle's low < candle 2 low, use current candle's low instead
        """
        candle_2 = candle_data['candle_2']
        current_candle = candle_data['current']
        
        if bias_type == 'bullish':
            # Default exit is candle 2 high
            exit_price = candle_2['high']
            
            # Check if current candle's high exceeds candle 2 high
            if current_candle['high'] > candle_2['high']:
                exit_price = current_candle['high']
                print(f"     📈 Exit updated: Current candle high ({current_candle['high']:.{digits}f}) > Candle 2 high ({candle_2['high']:.{digits}f})")
            else:
                print(f"     📈 Exit (Candle 2 high): {exit_price:.{digits}f}")
                
        else:  # bearish
            # Default exit is candle 2 low
            exit_price = candle_2['low']
            
            # Check if current candle's low is below candle 2 low
            if current_candle['low'] < candle_2['low']:
                exit_price = current_candle['low']
                print(f"     📉 Exit updated: Current candle low ({current_candle['low']:.{digits}f}) < Candle 2 low ({candle_2['low']:.{digits}f})")
            else:
                print(f"     📉 Exit (Candle 2 low): {exit_price:.{digits}f}")
        
        return exit_price
    
    def analyze_directional_bias(candle_data, symbol, digits):
        """
        Analyze directional bias based on the 2 most recent completed candles.
        
        Bullish pattern: Both green candles with higher highs and higher lows
        - Candle 2 high > Candle 1 high
        - Candle 2 low > Candle 1 low
        - Entry: sell_stop at Candle 1 BODY center (open + close) / 2
        - Exit: high of candle 2 (or current candle high if higher)
        
        Bearish pattern: Both red candles with lower highs and lower lows
        - Candle 2 low < Candle 1 low
        - Candle 2 high < Candle 1 high
        - Entry: buy_stop at Candle 1 BODY center (open + close) / 2
        - Exit: low of candle 2 (or current candle low if lower)
        """
        candle_1 = candle_data['candle_1']
        candle_2 = candle_data['candle_2']
        
        # Check if both candles are bullish
        candle_2_bullish = candle_2['close'] > candle_2['open']
        candle_1_bullish = candle_1['close'] > candle_1['open']
        
        print(f"\n  🔍 PATTERN CHECK:")
        print(f"     Candle 2 Bullish: {candle_2_bullish} (Close: {candle_2['close']:.{digits}f} > Open: {candle_2['open']:.{digits}f})")
        print(f"     Candle 1 Bullish: {candle_1_bullish} (Close: {candle_1['close']:.{digits}f} > Open: {candle_1['open']:.{digits}f})")
        
        if candle_2_bullish and candle_1_bullish:
            print(f"     Both candles are bullish, checking higher highs/lows...")
            print(f"     Candle 2 High: {candle_2['high']:.{digits}f} > Candle 1 High: {candle_1['high']:.{digits}f}: {candle_2['high'] > candle_1['high']}")
            print(f"     Candle 2 Low: {candle_2['low']:.{digits}f} > Candle 1 Low: {candle_1['low']:.{digits}f}: {candle_2['low'] > candle_1['low']}")
            
            if candle_2['high'] > candle_1['high'] and candle_2['low'] > candle_1['low']:
                entry_price = get_candle_center(candle_1)
                exit_price = calculate_exit_price('bullish', candle_data, digits)
                
                # Calculate candle body center for display
                body_center = (candle_1['open'] + candle_1['close']) / 2
                
                print(f"\n  ✅ BULLISH PATTERN DETECTED")
                print(f"     • Entry (Candle 1 Body Center): {entry_price:.{digits}f}")
                print(f"     • Candle 1 Open: {candle_1['open']:.{digits}f}, Close: {candle_1['close']:.{digits}f}")
                print(f"     • Exit: {exit_price:.{digits}f}")
                print(f"     • Order Type: sell_stop")
                
                return 'bullish', entry_price, exit_price
            else:
                print(f"     ❌ Failed higher highs/lows condition")
        
        # Check if both candles are bearish
        candle_2_bearish = candle_2['close'] < candle_2['open']
        candle_1_bearish = candle_1['close'] < candle_1['open']
        
        print(f"     Candle 2 Bearish: {candle_2_bearish} (Close: {candle_2['close']:.{digits}f} < Open: {candle_2['open']:.{digits}f})")
        print(f"     Candle 1 Bearish: {candle_1_bearish} (Close: {candle_1['close']:.{digits}f} < Open: {candle_1['open']:.{digits}f})")
        
        if candle_2_bearish and candle_1_bearish:
            print(f"     Both candles are bearish, checking lower highs/lows...")
            print(f"     Candle 2 Low: {candle_2['low']:.{digits}f} < Candle 1 Low: {candle_1['low']:.{digits}f}: {candle_2['low'] < candle_1['low']}")
            print(f"     Candle 2 High: {candle_2['high']:.{digits}f} < Candle 1 High: {candle_1['high']:.{digits}f}: {candle_2['high'] < candle_1['high']}")
            
            if candle_2['low'] < candle_1['low'] and candle_2['high'] < candle_1['high']:
                entry_price = get_candle_center(candle_1)
                exit_price = calculate_exit_price('bearish', candle_data, digits)
                
                # Calculate candle body center for display
                body_center = (candle_1['open'] + candle_1['close']) / 2
                
                print(f"\n  ✅ BEARISH PATTERN DETECTED")
                print(f"     • Entry (Candle 1 Body Center): {entry_price:.{digits}f}")
                print(f"     • Candle 1 Open: {candle_1['open']:.{digits}f}, Close: {candle_1['close']:.{digits}f}")
                print(f"     • Exit: {exit_price:.{digits}f}")
                print(f"     • Order Type: buy_stop")
                
                return 'bearish', entry_price, exit_price
            else:
                print(f"     ❌ Failed lower highs/lows condition")
        
        print(f"\n  ❌ NO PATTERN DETECTED - No matching bias criteria")
        return None, None, None
    
    def save_directional_signals(prices_dir, all_signals, account_balance, account_currency, timeframe):
        """Save directional bias signals to JSON file"""
        signals_file = prices_dir / f"signals.json"
        
        final_data = {
            "account_type": "",
            "account_balance": account_balance,
            "account_currency": account_currency,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "timeframe": timeframe,
            "total_signals": len(all_signals),
            "signals": all_signals
        }
        
        with open(signals_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=4)
        
    
    def get_min_volume(symbol_info=None):
        """Get minimum volume for symbol"""
        return 0.01  # Minimum lot size
    
    # Main execution
    if inv_id:
        # Load investor configuration
        symbols_dict, timeframes, target_folder, risk_reward = load_investor_config(inv_id)
        
        if not symbols_dict or not timeframes:
            print(f" [{inv_id}] ❌ Failed to load configuration")
            return stats
        
        print(f"\n  ⏰ Timeframes: {timeframes}")
        print(f"  📁 Target folder: {target_folder}")
        print(f"  📈 Risk/Reward: {risk_reward}")
        
        # Get account info
        account_balance = 10000.0
        account_currency = "USD"
        
        prices_dir = Path(INV_PATH) / inv_id / "prices"
        prices_dir.mkdir(exist_ok=True)
        
        total_symbols = 0
        successful_symbols = 0
        failed_symbols = 0
        all_timeframes_signals = {}
        
        # Process each timeframe
        for timeframe in timeframes:
            print(f"\n{'='*50}")
            print(f"  Processing timeframe: {timeframe}")
            print(f"{'='*50}")
            
            timeframe_bullish = 0
            timeframe_bearish = 0
            timeframe_signals = []
            timeframe_symbols_processed = 0
            
            # Process each symbol for this timeframe
            for category, symbols in symbols_dict.items():
                for raw_symbol in symbols:
                    if not raw_symbol:
                        continue
                    
                    symbol = raw_symbol.upper()
                    
                    # Load candle data from JSON
                    candle_data, error = load_candle_data(inv_id, symbol, timeframe, target_folder)
                    
                    if candle_data is None:
                        print(f"\n  ❌ {symbol} [{timeframe}]: {error}")
                        failed_symbols += 1
                        continue
                    
                    # Determine digits for rounding
                    test_price = candle_data['candle_1']['close']
                    if test_price < 1:
                        digits = 5
                    else:
                        str_price = f"{test_price:.10f}".rstrip('0')
                        if '.' in str_price:
                            digits = len(str_price.split('.')[1])
                        else:
                            digits = 2
                    
                    # Print candle details
                    print_candle_details(symbol, timeframe, candle_data, digits)
                    
                    # Analyze directional bias
                    bias_type, entry_price, exit_price = analyze_directional_bias(candle_data, symbol, digits)
                    
                    if bias_type is None:
                        failed_symbols += 1
                        continue
                    
                    # Set order type based on bias
                    if bias_type == 'bullish':
                        order_type = "sell_stop"
                        timeframe_bullish += 1
                    else:  # bearish
                        order_type = "buy_stop"
                        timeframe_bearish += 1
                    
                    # Get minimum volume
                    min_volume = get_min_volume()
                    
                    # Create signal object with exit price
                    signal = {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "risk_reward": risk_reward,
                        "order_type": order_type,
                        "entry": round(entry_price, digits),
                        "exit": round(exit_price, digits),
                        "volume": min_volume
                    }
                    
                    timeframe_signals.append(signal)
                    timeframe_symbols_processed += 1
                    successful_symbols += 1
                    total_symbols += 1
                    
                    print(f"\n  💾 SIGNAL SAVED: {symbol} [{timeframe}] [{bias_type.upper()}]")
                    print(f"     Order: {order_type} at {round(entry_price, digits)}")
                    print(f"     Exit: {round(exit_price, digits)}")
                    print(f"     Volume: {min_volume}")
                    print(f"     Risk/Reward: {risk_reward}")
            
            # Save signals for this timeframe
            if timeframe_signals:
                # Save as array format (matching your requirement)
                signals_file = prices_dir / f"signals.json"
                
                with open(signals_file, 'w', encoding='utf-8') as f:
                    json.dump(timeframe_signals, f, indent=4)
                    
                print(f"\n  📊 SUMMARY for {timeframe}:")
                print(f"     • Symbols processed: {timeframe_symbols_processed}")
                print(f"     • Bullish signals: {timeframe_bullish}")
                print(f"     • Bearish signals: {timeframe_bearish}")
                print(f"     • Total signals: {len(timeframe_signals)}")
                
                all_timeframes_signals[timeframe] = {
                    "bullish": timeframe_bullish,
                    "bearish": timeframe_bearish,
                    "total": len(timeframe_signals),
                    "signals": timeframe_signals
                }
                
                stats["bullish_signals"] += timeframe_bullish
                stats["bearish_signals"] += timeframe_bearish
                stats["total_signals"] += len(timeframe_signals)
                stats["timeframes_used"].append(timeframe)
        
        # Final summary
        if stats["total_signals"] > 0:
            stats["signals_generated"] = True
            stats["total_symbols"] = total_symbols
            stats["successful_symbols"] = successful_symbols
            stats["failed_symbols"] = failed_symbols
            
            print(f"\n{'='*60}")
            print(f"  📊 FINAL SUMMARY for Investor {inv_id}")
            print(f"  {'='*60}")
            print(f"  • Timeframes processed: {timeframes}")
            print(f"  • Total symbols across all timeframes: {total_symbols}")
            print(f"  • Successful: {successful_symbols}")
            print(f"  • Failed: {failed_symbols}")
            print(f"  • Bullish signals: {stats['bullish_signals']}")
            print(f"  • Bearish signals: {stats['bearish_signals']}")
            print(f"  • Total signals: {stats['total_signals']}")
            print(f"  {'='*60}")
            
            # Create a master signals file combining all timeframes
            master_signals_file = prices_dir / "directional_signals_all.json"
            master_data = {
                "account_balance": account_balance,
                "account_currency": account_currency,
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "timeframes_processed": timeframes,
                "total_signals": stats['total_signals'],
                "signals_by_timeframe": all_timeframes_signals
            }
            
            with open(master_signals_file, 'w', encoding='utf-8') as f:
                json.dump(master_data, f, indent=4)
            
        else:
            print(f"\n  ⚠️ No signals generated for {inv_id}")
    
    return stats

def debug_print_all_broker_symbols():
    """
    Connects to the currently active MT5 terminal and prints 
    every available symbol name to the console.
    """
    # Ensure MT5 is initialized (if not already)
    if not mt5.initialize():
        print(f"FAILED to initialize MT5: {mt5.last_error()}")
        return

    # Get all symbols from the terminal
    symbols = mt5.symbols_get()
    
    if symbols is None:
        print("No symbols found. This might be a connection issue.")
    else:
        print(f"\n{'='*40}")
        print(f"BROKER: {mt5.account_info().server if mt5.account_info() else 'Unknown'}")
        print(f"TOTAL SYMBOLS FOUND: {len(symbols)}")
        print(f"{'='*40}")
        
        # Extract names and sort them alphabetically for easier reading
        all_names = sorted([s.name for s in symbols])
        
        for i, name in enumerate(all_names, 1):
            print(f"{i}. {name}")
            
        print(f"{'='*40}\nEND OF LIST\n{'='*40}")

def accountmanagement_manager(inv_id):
    """
    Updates accountmanagement.json field-by-field.
    Maintains 'Flex' look: lists on one line, dictionaries vertical.
    Handles both Default and Maximum risk management tables.
    """
    print(f"\n{'='*10} ⚙️ MANAGING ACCOUNT MANAGEMENT: {inv_id} {'='*10}")
    
    # 1. Setup Paths
    inv_folder = Path(INV_PATH) / inv_id
    inv_acc_mgmt_path = inv_folder / "accountmanagement.json"
    
    # 2. Identify Broker Template
    broker_cfg = usersdictionary.get(inv_id)
    if not broker_cfg:
        print(f" [!] Error: No broker config for {inv_id}")
        return False
    
    server = broker_cfg.get('SERVER', '')
    inv_broker_name = server.split('-')[0].split('.')[0].lower() if server else 'broker'
    template_filename = f"default{inv_broker_name}_accountmanagement.json"
    template_path = Path(DEFAULT_PATH) / template_filename

    if not template_path.exists():
        template_path = Path(DEFAULT_PATH) / "default_accountmanagement.json"
        if not template_path.exists(): return False

    # 3. Load Data
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template_data = json.load(f)
        if inv_acc_mgmt_path.exists():
            with open(inv_acc_mgmt_path, 'r', encoding='utf-8') as f:
                inv_data = json.load(f)
        else:
            inv_data = {}
    except Exception as e:
        print(f" [!] Load Error: {e}"); return False

    modified = False

    # --- FIELD-BY-FIELD FLEXIBLE LOGIC ---

    # selected_risk_reward
    curr_rr = inv_data.get("selected_risk_reward")
    if "selected_risk_reward" not in inv_data or curr_rr in [None, 0, [], [0], ""]:
        inv_data["selected_risk_reward"] = template_data.get("selected_risk_reward", [3])
        modified = True

    # symbols_dictionary
    if "symbols_dictionary" not in inv_data or not inv_data.get("symbols_dictionary"):
        inv_data["symbols_dictionary"] = template_data.get("symbols_dictionary", {})
        modified = True

    # settings (Sub-field check)
    template_settings = template_data.get("settings", {})
    if "settings" not in inv_data:
        inv_data["settings"] = template_settings
        modified = True
    else:
        for key, value in template_settings.items():
            if key not in inv_data["settings"]:
                inv_data["settings"][key] = value
                modified = True

    # account_balance_default_risk_management
    def_risk_key = "account_balance_default_risk_management"
    if def_risk_key not in inv_data or not inv_data.get(def_risk_key):
        inv_data[def_risk_key] = template_data.get(def_risk_key, {})
        modified = True

    # account_balance_maximum_risk_management (NEW)
    max_risk_key = "account_balance_maximum_risk_management"
    if max_risk_key not in inv_data or not inv_data.get(max_risk_key):
        inv_data[max_risk_key] = template_data.get(max_risk_key, {})
        modified = True
        print(f" └─ ✅ Added/Updated Maximum Risk Management table")

    # 4. Save with "Flex" Formatting
    if modified:
        try:
            # Generate standard JSON
            json_string = json.dumps(inv_data, indent=4)
            
            # Logic to flatten ONLY lists [ ... ] to keep them 'flex'
            # This regex captures lists but leaves large dictionaries vertical
            flex_format = re.sub(
                r'\[\s+([^\]\{\}]+?)\s+\]', 
                lambda m: "[ " + re.sub(r'\s+', ' ', m.group(1)).strip() + " ]", 
                json_string
            )

            with open(inv_acc_mgmt_path, 'w', encoding='utf-8') as f:
                f.write(flex_format)
                
            print(f" └─ 💾 {inv_id} accountmanagement.json synced successfully.")
            return True
        except Exception as e:
            print(f" └─ ❌ Save Error: {e}"); return False
    
    print(f" └─ 🔘 {inv_id} already contains all required fields.")
    return True

def get_normalized_symbol(record_symbol, risk_keys=None):
    """
    Standardizes symbols with a 'Broker-First' priority.
    If 'US OIL' is passed, it finds the USOIL family, then checks if the broker
    uses USOUSD, USOIL, or WTI.
    """
    if not record_symbol: return None

    NORM_PATH = Path(r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json")
    
    def clean(s): 
        return str(s).replace(" ", "").replace("_", "").replace("/", "").replace(".", "").upper()

    search_term = clean(record_symbol)
    
    # 1. Load Normalization Map
    norm_data = {}
    if NORM_PATH.exists():
        try:
            with open(NORM_PATH, 'r', encoding='utf-8') as f:
                norm_data = json.load(f).get("NORMALIZATION", {})
        except: pass

    # 2. Find the "Family"
    target_family_key = None
    all_family_variants = []
    
    for std_key, synonyms in norm_data.items():
        family_variants = [clean(std_key)] + [clean(s) for s in synonyms]
        if any(search_term == v or search_term.startswith(v) or v.startswith(search_term) for v in family_variants):
            target_family_key = std_key
            all_family_variants = family_variants
            break

    # 3. IF RISK_KEYS ARE PROVIDED (For Risk Enforcement)
    if risk_keys:
        clean_risk_map = {clean(k): k for k in risk_keys}
        if target_family_key and clean(target_family_key) in clean_risk_map:
            return clean_risk_map[clean(target_family_key)]
        for v in all_family_variants:
            if v in clean_risk_map: return clean_risk_map[v]

    # 4. IF NO RISK_KEYS (For Populating Order Fields / MT5 Specs)
    # Check what the broker actually has in MarketWatch
    all_symbols = mt5.symbols_get()
    if all_symbols:
        broker_symbols = {clean(s.name): s.name for s in all_symbols}
        
        # Try to find which variant the broker uses
        for v in all_family_variants:
            if v in broker_symbols:
                return broker_symbols[v]
            # Handle suffixes (e.g., USOIL.m)
            for b_clean, b_raw in broker_symbols.items():
                if b_clean.startswith(v):
                    return b_raw

    # Fallback
    return target_family_key if target_family_key else record_symbol.upper()

def deduplicate_orders(inv_id=None):
    """
    Scans all pending_orders/limit_orders.json, pending_orders/limit_orders_backup.json, 
    and pending_orders/signals.json files and removes duplicate orders based on: 
    Symbol, Timeframe, Order Type, and Entry Price.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any duplicates were removed, False otherwise
    """
    print(f"\n{'='*10} 🧹 DEDUPLICATING ORDERS {'='*10}")
    
    total_files_cleaned = 0
    total_duplicates_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_backup_files_cleaned = 0
    total_limit_duplicates = 0
    total_signal_duplicates = 0
    total_limit_backup_duplicates = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found for deduplication.")
        return False

    any_duplicates_removed = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Checking for duplicate entries...")

        # 2. Search for pending_orders folders
        pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
        
        investor_limit_duplicates = 0
        investor_signal_duplicates = 0
        investor_limit_backup_duplicates = 0
        investor_limit_files_cleaned = 0
        investor_signal_files_cleaned = 0
        investor_limit_backup_files_cleaned = 0

        for pending_folder in pending_orders_folders:
            # Process limit_orders.json
            limit_file = pending_folder / "limit_orders.json"
            if limit_file.exists():
                try:
                    with open(limit_file, 'r', encoding='utf-8') as f:
                        orders = json.load(f)

                    if orders:
                        original_count = len(orders)
                        seen_orders = set()
                        unique_orders = []

                        for order in orders:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(order.get("symbol", "")).strip(),
                                str(order.get("timeframe", "")).strip(),
                                str(order.get("order_type", "")).strip(),
                                float(order.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_orders.append(order)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_orders) < original_count:
                            removed = original_count - len(unique_orders)
                            with open(limit_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_orders, f, indent=4)
                            
                            investor_limit_duplicates += removed
                            investor_limit_files_cleaned += 1
                            total_limit_duplicates += removed
                            total_limit_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/limit_orders.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {limit_file}: {e}")

            # Process limit_orders_backup.json
            limit_backup_file = pending_folder / "limit_orders_backup.json"
            if limit_backup_file.exists():
                try:
                    with open(limit_backup_file, 'r', encoding='utf-8') as f:
                        backup_orders = json.load(f)

                    if backup_orders:
                        original_count = len(backup_orders)
                        seen_orders = set()
                        unique_backup_orders = []

                        for order in backup_orders:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(order.get("symbol", "")).strip(),
                                str(order.get("timeframe", "")).strip(),
                                str(order.get("order_type", "")).strip(),
                                float(order.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_backup_orders.append(order)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_backup_orders) < original_count:
                            removed = original_count - len(unique_backup_orders)
                            with open(limit_backup_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_backup_orders, f, indent=4)
                            
                            investor_limit_backup_duplicates += removed
                            investor_limit_backup_files_cleaned += 1
                            total_limit_backup_duplicates += removed
                            total_limit_backup_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/limit_orders_backup.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {limit_backup_file}: {e}")

            # Process signals.json
            signals_file = pending_folder / "signals.json"
            if signals_file.exists():
                try:
                    with open(signals_file, 'r', encoding='utf-8') as f:
                        signals = json.load(f)

                    if signals:
                        original_count = len(signals)
                        seen_orders = set()
                        unique_signals = []

                        for signal in signals:
                            # Create a unique key based on Symbol, Timeframe, Order Type, and Entry
                            unique_key = (
                                str(signal.get("symbol", "")).strip(),
                                str(signal.get("timeframe", "")).strip(),
                                str(signal.get("order_type", "")).strip(),
                                float(signal.get("entry", 0))
                            )

                            if unique_key not in seen_orders:
                                seen_orders.add(unique_key)
                                unique_signals.append(signal)
                        
                        # Only write back if duplicates were actually found
                        if len(unique_signals) < original_count:
                            removed = original_count - len(unique_signals)
                            with open(signals_file, 'w', encoding='utf-8') as f:
                                json.dump(unique_signals, f, indent=4)
                            
                            investor_signal_duplicates += removed
                            investor_signal_files_cleaned += 1
                            total_signal_duplicates += removed
                            total_signal_files_cleaned += 1
                            any_duplicates_removed = True
                            
                            folder_name = pending_folder.parent.name
                            print(f"  └─ 📄 {folder_name}/signals.json - Removed {removed} duplicates")

                except Exception as e:
                    print(f"  └─ ❌ Error processing {signals_file}: {e}")

        # Summary for the current investor
        if investor_limit_duplicates > 0 or investor_signal_duplicates > 0 or investor_limit_backup_duplicates > 0:
            print(f"\n  └─ ✨ Investor {current_inv_id} Cleanup Summary:")
            if investor_limit_duplicates > 0:
                print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_duplicates} duplicates")
            if investor_limit_backup_duplicates > 0:
                print(f"      • limit_orders_backup.json: Cleaned {investor_limit_backup_files_cleaned} files | Removed {investor_limit_backup_duplicates} duplicates")
            if investor_signal_duplicates > 0:
                print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_duplicates} duplicates")
        else:
            print(f"  └─ ✅ No duplicates found in any order files")

    # Final Global Summary
    print(f"\n{'='*10} DEDUPLICATION COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned + total_limit_backup_files_cleaned
    total_duplicates_removed = total_limit_duplicates + total_signal_duplicates + total_limit_backup_duplicates
    
    if total_duplicates_removed > 0:
        print(f" Total Duplicates Purged: {total_duplicates_removed}")
        print(f" Total Files Modified:    {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:        {total_limit_files_cleaned} files | {total_limit_duplicates} duplicates")
        print(f"   • limit_orders_backup.json: {total_limit_backup_files_cleaned} files | {total_limit_backup_duplicates} duplicates")
        print(f"   • signals.json:             {total_signal_files_cleaned} files | {total_signal_duplicates} duplicates")
    else:
        print(" ✅ Everything was already clean - no duplicates found!")
    print(f"{'='*33}\n")
    
    return any_duplicates_removed

def detect_unauthorized_action(inv_id=None):
    """
    Detects unauthorized trading activities and withdrawals for investors.
    Compares MT5 activity with tradeshistory.json from execution start date.
    Ensures NO other trades exist in MT5 history except those recorded in tradeshistory.json
    activities.json is located directly in INV_PATH/{investor_id}/ (new path structure)
    """
    
    def load_activities_config(inv_root):
        """Load activities.json directly from investor root folder"""
        activities_path = inv_root / "activities.json"
        if not activities_path.exists():
            print(f"    ⚠️  activities.json not found at {activities_path}")
            return None, None
        
        try:
            with open(activities_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config, activities_path
        except Exception as e:
            print(f"    ❌ Error loading activities.json: {e}")
            return None, None

    def load_trades_history(inv_root):
        """Load all trades from tradeshistory.json directly from investor root (the ONLY source of truth)"""
        history_path = inv_root / "tradeshistory.json"
        if not history_path.exists():
            print(f"    ⚠️  tradeshistory.json not found at {history_path}")
            return []
        
        try:
            with open(history_path, 'r', encoding='utf-8') as f:
                trades = json.load(f)
            return trades if isinstance(trades, list) else []
        except Exception as e:
            print(f"    ⚠️  Error loading tradeshistory.json: {e}")
            return []

    def get_mt5_activity_since(start_date, authorized_trades_list):
        """
        Get all MT5 trades since start_date
        Returns ONLY trades that are NOT in tradeshistory.json as unauthorized
        """
        # Convert start_date string to datetime
        try:
            # Try parsing "March 03, 2026" format
            start_datetime = datetime.strptime(start_date, "%B %d, %Y")
            # Set to beginning of the day
            start_datetime = start_datetime.replace(hour=0, minute=0, second=0)
        except:
            try:
                # Fallback to ISO format
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
                start_datetime = start_datetime.replace(hour=0, minute=0, second=0)
            except:
                print(f"    ❌ Invalid date format: {start_date}")
                return [], []

        print(f"    🔍 Checking MT5 history from: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Create lookup of authorized tickets from tradeshistory.json (the ONLY source)
        authorized_tickets = set()
        for trade in authorized_trades_list:
            if 'ticket' in trade and trade['ticket']:
                authorized_tickets.add(int(trade['ticket']))

        # Get ALL deals (completed trades) since start date
        all_deals = mt5.history_deals_get(start_datetime, datetime.now()) or []
        
        print(f"    📊 Total MT5 deals found: {len(all_deals)}")
        print(f"    📋 Authorized tickets in tradeshistory.json: {len(authorized_tickets)}")
        
        unauthorized_trades = []
        withdrawals = []
        
        # Track processed tickets to avoid duplicates
        processed_tickets = set()
        
        # Check all deals (completed and closed trades)
        for deal in all_deals:
            deal_ticket = deal.ticket
            
            # Skip if already processed
            if deal_ticket in processed_tickets:
                continue
            processed_tickets.add(deal_ticket)
            
            # Check if this is a withdrawal (balance operation)
            if deal.type == mt5.DEAL_TYPE_BALANCE:
                if deal.profit < 0:  # Withdrawal
                    withdrawals.append({
                        'ticket': deal_ticket,
                        'amount': abs(deal.profit),
                        'balance': deal.balance,
                        'time': datetime.fromtimestamp(deal.time).strftime("%Y-%m-%d %H:%M:%S"),
                        'timestamp': deal.time,
                        'comment': deal.comment or 'Unknown',
                        'reason': 'Funds withdrawn',
                        'detected_at': datetime.now().isoformat()
                    })
                continue
            
            # For regular trades, check if this deal is authorized
            # A trade is authorized ONLY if its ticket exists in tradeshistory.json
            if deal_ticket not in authorized_tickets:
                # This is an unauthorized trade - NOT in tradeshistory.json
                deal_time = datetime.fromtimestamp(deal.time).strftime("%Y-%m-%d %H:%M:%S")
                deal_type = "BUY" if deal.type == mt5.DEAL_TYPE_BUY else "SELL" if deal.type == mt5.DEAL_TYPE_SELL else "UNKNOWN"
                
                unauthorized_trades.append({
                    'ticket': deal_ticket,
                    'order': deal.order,
                    'symbol': deal.symbol,
                    'volume': deal.volume,
                    'price': deal.price,
                    'type': deal_type,
                    'time': deal_time,
                    'timestamp': deal.time,
                    'magic': deal.magic,
                    'commission': deal.commission,
                    'swap': deal.swap,
                    'profit': deal.profit,
                    'reason': f"Trade NOT in tradeshistory.json (Ticket: {deal_ticket})",
                    'detected_at': datetime.now().isoformat()
                })
                print(f"      ⚠️  Found unauthorized trade: Ticket {deal_ticket} ({deal.symbol}) not in tradeshistory.json")
        
        return unauthorized_trades, withdrawals

    # --- MAIN EXECUTION ---
    print("\n" + "="*80)
    print("🔍 DETECTING UNAUTHORIZED ACTIONS")
    print("="*80)
    print("📋 Checking that ONLY trades in tradeshistory.json have been executed")
    
    # Get investor IDs to check
    investor_ids = [inv_id] if inv_id else list(usersdictionary.keys())
    unauthorized_detected = False

    for user_brokerid in investor_ids:
        print(f"\n📋 INVESTOR: {user_brokerid}")
        print("-" * 60)
        
        # Setup paths - DIRECTLY in investor root folder (new path structure)
        inv_root = Path(INV_PATH) / user_brokerid
        
        if not inv_root.exists():
            print(f"  ❌ Path not found: {inv_root}")
            continue

        # Load activities.json directly from investor root
        config, activities_path = load_activities_config(inv_root)
        if not config:
            print(f"  ⚠️  No activities.json found at {inv_root / 'activities.json'}, skipping...")
            continue
        
        # Check if autotrading is activated
        if not config.get('activate_autotrading', False):
            print(f"  ⏭️  AutoTrading not activated, skipping...")
            continue
        
        # Get execution start date
        execution_start = config.get('execution_start_date')
        if not execution_start:
            print(f"  ⚠️  No execution start date found, using today")
            execution_start = datetime.now().strftime("%B %d, %Y")
        
        print(f"  📅 Checking activity since: {execution_start}")
        
        # Load trades history from tradeshistory.json (the ONLY authorized trades)
        trades_history = load_trades_history(inv_root)
        print(f"  📊 Authorized trades in tradeshistory.json: {len(trades_history)}")
        
        # Get MT5 activity since execution start
        unauthorized_trades, withdrawals = get_mt5_activity_since(
            execution_start, 
            trades_history
        )
        
        # Update config with findings
        config_updated = False
        
        # Format unauthorized trades for storage (using ticket numbers as keys)
        new_unauthorized_trades = {}
        for trade in unauthorized_trades:
            ticket = trade.get('ticket')
            if ticket:
                new_unauthorized_trades[f"ticket_{ticket}"] = trade
        
        if new_unauthorized_trades:
            print(f"  ⚠️  Found {len(unauthorized_trades)} UNAUTHORIZED trades!")
            print(f"  ⚠️  These trades exist in MT5 but NOT in tradeshistory.json")
            if new_unauthorized_trades != config.get('unauthorized_trades', {}):
                config['unauthorized_trades'] = new_unauthorized_trades
                config_updated = True
                unauthorized_detected = True
        else:
            if config.get('unauthorized_trades'):
                config['unauthorized_trades'] = {}
                config_updated = True
            print(f"  ✅ ALL trades in MT5 match tradeshistory.json - No unauthorized trades")
        
        # Format withdrawals for storage
        new_withdrawals = {}
        for wd in withdrawals:
            ticket = wd.get('ticket')
            if ticket:
                new_withdrawals[f"withdrawal_{ticket}"] = wd
        
        if new_withdrawals:
            print(f"  ⚠️  Found {len(withdrawals)} unauthorized withdrawals!")
            if new_withdrawals != config.get('unauthorized_withdrawals', {}):
                config['unauthorized_withdrawals'] = new_withdrawals
                config_updated = True
                unauthorized_detected = True
        else:
            if config.get('unauthorized_withdrawals'):
                config['unauthorized_withdrawals'] = {}
                config_updated = True
            print(f"  ✅ No unauthorized withdrawals detected")
        
        # Update detection flag
        new_detection_status = bool(unauthorized_trades or withdrawals)
        if new_detection_status != config.get('unauthorized_action_detected', False):
            config['unauthorized_action_detected'] = new_detection_status
            config_updated = True
        
        # Save updated config if changes were made
        if config_updated:
            try:
                with open(activities_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=4)
                print(f"  💾 Updated activities.json at: {activities_path}")
                
                # Print detailed summary of unauthorized activities
                if unauthorized_trades:
                    print(f"\n  🚨 UNAUTHORIZED TRADES DETAILS:")
                    for trade in unauthorized_trades:
                        print(f"      - Ticket: {trade['ticket']} | {trade.get('symbol', 'N/A')} | "
                              f"{trade.get('type', 'N/A')} | {trade.get('volume', 'N/A')} lots | "
                              f"Price: {trade.get('price', 'N/A')} | Profit: ${trade.get('profit', 0):.2f}")
                        print(f"        Time: {trade.get('time', 'N/A')}")
                        print(f"        Reason: {trade.get('reason', 'Unknown')}")
                        print()
                
                if withdrawals:
                    print(f"\n  🚨 UNAUTHORIZED WITHDRAWALS DETAILS:")
                    for wd in withdrawals:
                        print(f"      - Ticket: {wd['ticket']} | Amount: ${wd['amount']:.2f} | "
                              f"Time: {wd['time']} | Comment: {wd['comment']}")
                        print()
                        
            except Exception as e:
                print(f"  ❌ Failed to save activities.json: {e}")
        else:
            print(f"  ℹ️  No changes to activities.json")

    print("\n" + "="*80)
    if unauthorized_detected:
        print("⚠️  UNAUTHORIZED ACTIONS DETECTED!")
        print("⚠️  Some trades in MT5 are NOT recorded in tradeshistory.json")
        print("⚠️  Check activities.json for complete details")
    else:
        print("✅ NO UNAUTHORIZED ACTIONS DETECTED")
        print("✅ All MT5 trades match records in tradeshistory.json")
    print("="*80)
    
    return unauthorized_detected

def filter_unauthorized_symbols(inv_id=None):
    """
    Verifies and filters pending order files based on allowed symbols defined in accountmanagement.json.
    Now filters both limit_orders.json and signals.json files, removing any entries with unauthorized symbols.
    Matches sanitized versions of symbols to handle broker suffixes (e.g., EURUSDm vs EURUSD).
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any unauthorized symbols were removed, False otherwise
    """
    print(f"\n{'='*10} 🛡️  SYMBOL AUTHORIZATION FILTER {'='*10}")

    def sanitize(sym):
        if not sym: return ""
        # Remove non-alphanumeric, uppercase, and strip trailing M/PRO suffixes
        clean = re.sub(r'[^a-zA-Z0-9]', '', str(sym)).upper()
        return re.sub(r'(PRO|M)$', '', clean)

    if not os.path.exists(INV_PATH):
        print(f" [!] Error: Investor path {INV_PATH} not found.")
        return False

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    if not investor_ids:
        print(" └─ 🔘 No investor directories found for filtering.")
        return False

    total_files_cleaned = 0
    total_entries_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_removed = 0
    total_signal_removed = 0
    any_symbols_removed = False

    for current_inv_id in investor_ids:
        print(f"\n [{current_inv_id}] 🔍 Verifying symbol permissions...")
        inv_folder = Path(INV_PATH) / current_inv_id
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract and sanitize the list of allowed symbols
            sym_dict = config.get("symbols_dictionary", {})
            allowed_sanitized = {sanitize(s) for sublist in sym_dict.values() for s in sublist}
            
            if not allowed_sanitized:
                print(f"  └─ 🔘 No symbols defined in dictionary. Skipping filter.")
                continue

            print(f"  └─ ✅ Found {len(allowed_sanitized)} authorized symbols")

            # Search for pending_orders folders
            pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
            
            investor_limit_removed = 0
            investor_signal_removed = 0
            investor_limit_files_cleaned = 0
            investor_signal_files_cleaned = 0

            for pending_folder in pending_orders_folders:
                # Process limit_orders.json
                limit_file = pending_folder / "limit_orders.json"
                if limit_file.exists():
                    try:
                        with open(limit_file, 'r', encoding='utf-8') as f:
                            orders = json.load(f)

                        if orders and isinstance(orders, list):
                            original_count = len(orders)
                            
                            # Filter: Keep only if the sanitized symbol exists in our allowed set
                            filtered_orders = [
                                order for order in orders 
                                if sanitize(order.get("symbol", "")) in allowed_sanitized
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_orders) < original_count:
                                removed = original_count - len(filtered_orders)
                                with open(limit_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_orders, f, indent=4)
                                
                                investor_limit_removed += removed
                                investor_limit_files_cleaned += 1
                                total_limit_removed += removed
                                total_limit_files_cleaned += 1
                                any_symbols_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/limit_orders.json - Removed {removed} unauthorized entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/limit_orders.json - All symbols authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {limit_file}: {e}")

                # Process signals.json
                signals_file = pending_folder / "signals.json"
                if signals_file.exists():
                    try:
                        with open(signals_file, 'r', encoding='utf-8') as f:
                            signals = json.load(f)

                        if signals and isinstance(signals, list):
                            original_count = len(signals)
                            
                            # Filter: Keep only if the sanitized symbol exists in our allowed set
                            filtered_signals = [
                                signal for signal in signals 
                                if sanitize(signal.get("symbol", "")) in allowed_sanitized
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_signals) < original_count:
                                removed = original_count - len(filtered_signals)
                                with open(signals_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_signals, f, indent=4)
                                
                                investor_signal_removed += removed
                                investor_signal_files_cleaned += 1
                                total_signal_removed += removed
                                total_signal_files_cleaned += 1
                                any_symbols_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/signals.json - Removed {removed} unauthorized entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/signals.json - All symbols authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {signals_file}: {e}")

            # Summary for the current investor
            if investor_limit_removed > 0 or investor_signal_removed > 0:
                print(f"\n  └─ ✨ Investor {current_inv_id} Filter Summary:")
                if investor_limit_removed > 0:
                    print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_removed} unauthorized entries")
                if investor_signal_removed > 0:
                    print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_removed} unauthorized entries")
            else:
                # Check if any files were found at all
                if pending_orders_folders:
                    print(f"  └─ ✅ All symbols in order files are authorized")
                else:
                    print(f"  └─ 🔘 No pending_orders folders found")

        except Exception as e:
            print(f"  └─ ❌ Error processing {current_inv_id}: {e}")

    # Final Global Summary
    print(f"\n{'='*10} SYMBOL FILTERING COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned
    total_entries_removed = total_limit_removed + total_signal_removed
    
    if total_entries_removed > 0:
        print(f" Total Unauthorized Entries Removed: {total_entries_removed}")
        print(f" Total Files Modified:               {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:   {total_limit_files_cleaned} files | {total_limit_removed} entries removed")
        print(f"   • signals.json:        {total_signal_files_cleaned} files | {total_signal_removed} entries removed")
    else:
        if total_files_cleaned == 0:
            print(" ✅ No files needed filtering - all symbols were already authorized!")
        else:
            print(" ✅ All files checked and verified - no unauthorized symbols found!")
    print(f"{'='*39}\n")
    
    return any_symbols_removed

def filter_unauthorized_timeframes(inv_id=None):
    """
    Verifies and filters pending order files based on restricted timeframes defined in accountmanagement.json.
    Now filters both limit_orders.json and signals.json files, removing any entries with restricted timeframes.
    Matches the 'timeframe' key in order files against the 'restrict_order_from_timeframe' setting.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        This function does NOT require MT5.
    
    Returns:
        bool: True if any restricted timeframes were removed, False otherwise
    """
    print(f"\n{'='*10} 🛡️  TIMEFRAME AUTHORIZATION FILTER {'='*10}")

    def sanitize_tf(tf):
        if not tf: return ""
        # Ensure uniform comparison (lowercase, stripped)
        return str(tf).strip().lower()

    if not os.path.exists(INV_PATH):
        print(f" [!] Error: Investor path {INV_PATH} not found.")
        return False

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    if not investor_ids:
        print(" └─ 🔘 No investor directories found for filtering.")
        return False

    total_files_cleaned = 0
    total_entries_removed = 0
    total_limit_files_cleaned = 0
    total_signal_files_cleaned = 0
    total_limit_removed = 0
    total_signal_removed = 0
    any_timeframes_removed = False

    for current_inv_id in investor_ids:
        print(f"\n [{current_inv_id}] 🔍 Checking timeframe restrictions...")
        inv_folder = Path(INV_PATH) / current_inv_id
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract restriction setting
            # Supports: "5m" OR ["1m", "5m"]
            raw_restrictions = config.get("settings", {}).get("restrict_order_from_timeframe", [])
            
            if isinstance(raw_restrictions, str):
                # Handle comma separated strings or single strings
                restricted_list = [s.strip() for s in raw_restrictions.split(',')]
            elif isinstance(raw_restrictions, list):
                restricted_list = raw_restrictions
            else:
                restricted_list = []

            restricted_set = {sanitize_tf(t) for t in restricted_list if t}

            if not restricted_set:
                print(f"  └─ ✅ No timeframe restrictions active.")
                continue

            print(f"  └─ 🚫 Restricted timeframes: {', '.join(restricted_set)}")

            # Search for pending_orders folders
            pending_orders_folders = list(inv_folder.rglob("*/pending_orders/"))
            
            investor_limit_removed = 0
            investor_signal_removed = 0
            investor_limit_files_cleaned = 0
            investor_signal_files_cleaned = 0

            for pending_folder in pending_orders_folders:
                # Process limit_orders.json
                limit_file = pending_folder / "limit_orders.json"
                if limit_file.exists():
                    try:
                        with open(limit_file, 'r', encoding='utf-8') as f:
                            orders = json.load(f)

                        if orders and isinstance(orders, list):
                            original_count = len(orders)
                            
                            # Filter: Keep only if the entry's timeframe is NOT in the restricted set
                            filtered_orders = [
                                order for order in orders 
                                if sanitize_tf(order.get("timeframe")) not in restricted_set
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_orders) < original_count:
                                removed = original_count - len(filtered_orders)
                                with open(limit_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_orders, f, indent=4)
                                
                                investor_limit_removed += removed
                                investor_limit_files_cleaned += 1
                                total_limit_removed += removed
                                total_limit_files_cleaned += 1
                                any_timeframes_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/limit_orders.json - Removed {removed} restricted timeframe entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/limit_orders.json - All timeframes authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {limit_file}: {e}")

                # Process signals.json
                signals_file = pending_folder / "signals.json"
                if signals_file.exists():
                    try:
                        with open(signals_file, 'r', encoding='utf-8') as f:
                            signals = json.load(f)

                        if signals and isinstance(signals, list):
                            original_count = len(signals)
                            
                            # Filter: Keep only if the entry's timeframe is NOT in the restricted set
                            filtered_signals = [
                                signal for signal in signals 
                                if sanitize_tf(signal.get("timeframe")) not in restricted_set
                            ]
                            
                            # Only write back if entries were actually removed
                            if len(filtered_signals) < original_count:
                                removed = original_count - len(filtered_signals)
                                with open(signals_file, 'w', encoding='utf-8') as f:
                                    json.dump(filtered_signals, f, indent=4)
                                
                                investor_signal_removed += removed
                                investor_signal_files_cleaned += 1
                                total_signal_removed += removed
                                total_signal_files_cleaned += 1
                                any_timeframes_removed = True
                                
                                folder_name = pending_folder.parent.name
                                print(f"    └─ 📄 {folder_name}/signals.json - Removed {removed} restricted timeframe entries")
                            elif original_count > 0:
                                folder_name = pending_folder.parent.name
                                print(f"    └─ ✅ {folder_name}/signals.json - All timeframes authorized ({original_count} entries)")

                    except Exception as e:
                        print(f"    └─ ❌ Error processing {signals_file}: {e}")

            # Summary for the current investor
            if investor_limit_removed > 0 or investor_signal_removed > 0:
                print(f"\n  └─ ✨ Investor {current_inv_id} Filter Summary:")
                if investor_limit_removed > 0:
                    print(f"      • limit_orders.json: Cleaned {investor_limit_files_cleaned} files | Removed {investor_limit_removed} restricted entries")
                if investor_signal_removed > 0:
                    print(f"      • signals.json: Cleaned {investor_signal_files_cleaned} files | Removed {investor_signal_removed} restricted entries")
                print(f"     (Blocked timeframes: {', '.join(restricted_set)})")
            else:
                # Check if any files were found at all
                if pending_orders_folders:
                    print(f"  └─ ✅ All timeframes in order files are authorized")
                else:
                    print(f"  └─ 🔘 No pending_orders folders found")

        except Exception as e:
            print(f"  └─ ❌ Error processing {current_inv_id}: {e}")

    # Final Global Summary
    print(f"\n{'='*10} TIMEFRAME FILTERING COMPLETE {'='*10}")
    
    total_files_cleaned = total_limit_files_cleaned + total_signal_files_cleaned
    total_entries_removed = total_limit_removed + total_signal_removed
    
    if total_entries_removed > 0:
        print(f" Total Restricted Entries Removed: {total_entries_removed}")
        print(f" Total Files Modified:              {total_files_cleaned}")
        print(f"\n Breakdown by file type:")
        print(f"   • limit_orders.json:   {total_limit_files_cleaned} files | {total_limit_removed} entries removed")
        print(f"   • signals.json:        {total_signal_files_cleaned} files | {total_signal_removed} entries removed")
    else:
        if total_files_cleaned == 0:
            print(" ✅ No files needed filtering - no restricted timeframes found!")
        else:
            print(" ✅ All files checked and verified - no restricted timeframes found!")
    print(f"{'='*41}\n")
    
    return any_timeframes_removed

def backup_limit_orders(inv_id=None):
    """
    Finds all limit_orders.json files and creates a copy named 
    limit_orders_backup.json in the same directory.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. 
                               If None, processes all investors.
    """
    print(f"\n{'='*10} 📂 CREATING LIMIT ORDERS BACKUP {'='*10}")
    
    inv_base_path = Path(INV_PATH)
    total_backups_created = 0

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # 1. Determine which investors to process
    if inv_id:
        investor_folders = [inv_base_path / inv_id]
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]

    # 2. Loop through each investor folder
    for inv_folder in investor_folders:
        if not inv_folder.exists():
            continue
            
        print(f" [{inv_folder.name}] Scanning for limit_orders.json...")

        # 3. Find all limit_orders.json files (using rglob for subfolders)
        # Specifically targeting the 'pending_orders' subfolder pattern
        target_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))

        for source_path in target_files:
            # Define the backup path in the same directory
            backup_path = source_path.parent / "limit_orders_backup.json"
            
            try:
                # 4. Create the copy (overwrites existing backup)
                shutil.copy2(source_path, backup_path)
                
                print(f"  └─ ✅ Backed up: {source_path.parent.parent.name} -> limit_orders_backup.json")
                total_backups_created += 1
                
            except Exception as e:
                print(f"  └─ ❌ Error backing up {source_path}: {e}")

    print(f"\n{'='*10} BACKUP PROCESS COMPLETE {'='*10}")
    print(f" Total backups created: {total_backups_created}")
    return total_backups_created > 0

def populate_orders_missing_fields(inv_id=None, callback_function=None):
    print(f"\n{'='*10} 📊 POPULATING ORDER FIELDS {'='*10}")
    
    total_files_updated = 0
    total_orders_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f" [{current_inv_id}] 🔍 Processing orders...")

        # Local Cache for this investor to prevent redundant lookups
        # Format: { "raw_symbol": {"broker_sym": "normalized", "info": mt5_obj} }
        resolution_cache = {}

        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        if not order_files: continue
            
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg: continue
            
        server = broker_cfg.get('SERVER', '')
        broker_prefix = server.split('-')[0].split('.')[0].lower() if server else 'broker'
        v_field, ts_field, tv_field = f"{broker_prefix}_volume", f"{broker_prefix}_tick_size", f"{broker_prefix}_tick_value"

        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                if not orders: continue
                
                modified = False
                for order in orders:
                    raw_symbol = order.get("symbol")
                    if not raw_symbol: continue

                    # Check Cache First
                    if raw_symbol in resolution_cache:
                        res = resolution_cache[raw_symbol]
                        broker_symbol = res['broker_sym']
                        symbol_info = res['info']
                    else:
                        # Perform mapping only once
                        broker_symbol = get_normalized_symbol(raw_symbol)
                        symbol_info = mt5.symbol_info(broker_symbol)
                        
                        resolution_cache[raw_symbol] = {'broker_sym': broker_symbol, 'info': symbol_info}
                        
                        # Detailed Log only on first discovery
                        if symbol_info:
                            if broker_symbol != raw_symbol:
                                print(f"    └─ ✅ {raw_symbol} -> {broker_symbol} (Mapped & Cached)")
                                total_symbols_normalized += 1
                        else:
                            print(f"    └─ ❌ MT5: '{broker_symbol}' (from '{raw_symbol}') not found in MarketWatch")

                    if symbol_info:
                        order['symbol'] = broker_symbol
                        
                        # Cleanup and Update
                        for key in list(order.keys()):
                            if any(x in key.lower() for x in ['volume', 'tick_size', 'tick_value']) and key not in [v_field, ts_field, tv_field]:
                                del order[key]

                        order[v_field] = symbol_info.volume_min
                        order[ts_field] = symbol_info.trade_tick_size
                        order[tv_field] = symbol_info.trade_tick_value
                        total_orders_updated += 1
                        modified = True

                if modified:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(orders, f, indent=4)
                    total_files_updated += 1

            except Exception as e:
                print(f"  └─ ❌ Error: {e}")

    print(f"\n{'='*10} POPULATION COMPLETE {'='*10}")
    print(f" Total Orders Updated:      {total_orders_updated}")
    print(f" Total Symbols Normalized:  {total_symbols_normalized}")
    return True

def activate_usd_based_risk_on_empty_pricelevels(inv_id=None):
    print(f"\n{'='*10} 📊 INVESTOR EMPTY TARGET CHECK - USD RISK ENFORCEMENT {'='*10}")
    
    total_orders_processed = 0
    total_orders_enforced = 0
    total_files_updated = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Processing empty target check...")

        # Cache for risk mappings to avoid re-calculating family logic 1000s of times
        risk_map_cache = {}

        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        broker_name = broker_cfg.get('BROKER_NAME', '').lower() or \
                      broker_cfg.get('SERVER', 'default').split('-')[0].split('.')[0].lower()

        default_config_path = Path(DEFAULT_PATH) / f"{broker_name}_default_allowedsymbolsandvolumes.json"
        
        risk_lookup = {}
        if default_config_path.exists():
            try:
                with open(default_config_path, 'r', encoding='utf-8') as f:
                    default_config = json.load(f)
                    for category, items in default_config.items():
                        if not isinstance(items, list): continue
                        for item in items:
                            sym = str(item.get("symbol", "")).upper()
                            if sym:
                                risk_lookup[sym] = {
                                    k.replace("_specs", "").upper(): v.get("usd_risk", 0)
                                    for k, v in item.items() if k.endswith("_specs")
                                }
                print(f"  └─ ✅ Loaded risk config for {len(risk_lookup)} symbols")
            except Exception as e:
                print(f"  └─ ❌ Risk config error: {e}")
                continue

        known_risk_symbols = list(risk_lookup.keys())
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        signals_files = list(inv_folder.rglob("*/signals/signals.json"))
        
        for file_list, label in [(order_files, "LIMITS"), (signals_files, "SIGNALS")]:
            for file_path in file_list:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if not data: continue
                    modified = False
                    
                    for item in data:
                        if item.get('exit') in [0, "0", None, 0.0] and \
                           item.get('target') in [0, "0", None, 0.0]:
                            
                            total_orders_processed += 1
                            raw_sym = str(item.get('symbol', '')).upper()
                            raw_tf = str(item.get('timeframe', '')).upper()
                            
                            # Cache Logic for Risk Mapping
                            if raw_sym not in risk_map_cache:
                                matched_sym = get_normalized_symbol(raw_sym, risk_keys=known_risk_symbols)
                                risk_map_cache[raw_sym] = matched_sym
                                
                                # Print mapping only once per symbol type
                                if matched_sym not in risk_lookup:
                                    print(f"      ❌ [{label}] {raw_sym}: Not in risk config (Mapped as: {matched_sym})")
                            else:
                                matched_sym = risk_map_cache[raw_sym]
                            
                            if matched_sym in risk_lookup:
                                tf_risks = risk_lookup[matched_sym]
                                risk_value = tf_risks.get(raw_tf, 0)
                                
                                if risk_value > 0:
                                    item.update({
                                        'exit': 0, 'target': 0, 'usd_risk': risk_value,
                                        'usd_based_risk_only': True, 'symbol': matched_sym
                                    })
                                    modified = True
                                    total_orders_enforced += 1
                                    
                                    # Logic to avoid spamming the same enforcement 1000 times in logs
                                    # Only log the first time we enforce this symbol/tf pair for this file
                                    if f"{raw_sym}_{raw_tf}" not in risk_map_cache:
                                        print(f"      ✅ [{label}] {matched_sym} ({raw_sym}) {raw_tf}: Enforced ${risk_value} risk")
                                        risk_map_cache[f"{raw_sym}_{raw_tf}"] = True
                                
                    if modified:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        total_files_updated += 1

                except Exception as e:
                    print(f"    └─ ❌ Error processing {file_path.name}: {e}")

    print(f"\n{'='*10} ENFORCEMENT COMPLETE {'='*10}")
    print(f" Total Targetless Found:  {total_orders_processed}")
    print(f" Total Risk Enforced:    {total_orders_enforced}")
    print(f" Files Updated:          {total_files_updated}")
    
    return total_orders_enforced > 0

def enforce_investors_risk(inv_id=None):
    """
    Enforces risk rules for investors based on accountmanagement.json settings.
    Enhanced with Smart Normalization Caching and optimized lookup logic.
    """
    print(f"\n{'='*10} 📊 SMART INVESTOR RISK ENFORCEMENT {'='*10}")
    
    total_orders_processed = 0
    total_orders_enforced = 0
    total_files_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    investor_folders = [inv_base_path / inv_id] if inv_id else [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_enforced = False
    
    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        
        # --- INVESTOR LOCAL CACHE ---
        # Stores: { "RAW_SYM": {"matched": "NORM_SYM", "is_norm": True/False, "risk": {TF_DATA}} }
        resolution_cache = {}
        
        print(f"\n [{current_inv_id}] 🔍 Initializing smart enforcement...")

        # 1. Load accountmanagement.json
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found, skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            enforce_default = acc_mgmt_data.get("settings", {}).get("enforce_default_usd_risk", False)
            print(f"  └─ 🎯 Master Switch: {enforce_default}")
            
            if not enforce_default:
                print(f"  └─ ⏭️  Master switch is OFF - skipping")
                continue
        except Exception as e:
            print(f"  └─ ❌ Failed to load accountmanagement.json: {e}")
            continue

        # 2. Get Broker and Config Path
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            continue
        
        broker_name = broker_cfg.get('BROKER_NAME', '').lower() or \
                      broker_cfg.get('SERVER', 'default').split('-')[0].split('.')[0].lower()

        default_config_path = Path(DEFAULT_PATH) / f"{broker_name}_default_allowedsymbolsandvolumes.json"
        if not default_config_path.exists():
            print(f"  └─ ❌ Default config not found: {default_config_path.name}")
            continue
        
        # 3. Build Risk Lookup Table
        risk_lookup = {}
        try:
            with open(default_config_path, 'r', encoding='utf-8') as f:
                default_config = json.load(f)
                for category, items in default_config.items():
                    if not isinstance(items, list): continue
                    for item in items:
                        sym = str(item.get("symbol", "")).upper()
                        if sym:
                            risk_lookup[sym] = {
                                k.replace("_specs", "").upper(): {
                                    "volume": v.get("volume", 0.01),
                                    "usd_risk": v.get("usd_risk", 0)
                                } for k, v in item.items() if k.endswith("_specs")
                            }
            known_risk_symbols = list(risk_lookup.keys())
            print(f"  └─ ✅ Loaded risk config for {len(risk_lookup)} symbols")
        except Exception as e:
            print(f"  └─ ❌ Failed to parse default config: {e}")
            continue

        # 4. Gather Files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        signals_files = list(inv_folder.rglob("*/signals/signals.json"))
        
        investor_orders_enforced = 0
        investor_files_updated = 0
        
        # 5. Process Unified Pipeline
        for file_list, label in [(order_files, "LIMITS"), (signals_files, "SIGNALS")]:
            for file_path in file_list:
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    if not data: continue
                    
                    modified = False
                    for item in data:
                        total_orders_processed += 1
                        raw_sym = str(item.get('symbol', '')).upper()
                        raw_tf = str(item.get('timeframe', '')).upper()
                        
                        # --- SMART RESOLUTION LOGIC ---
                        if raw_sym not in resolution_cache:
                            # Helper does the heavy lifting: USOUSD -> USOIL
                            matched_sym = get_normalized_symbol(raw_sym, risk_keys=known_risk_symbols)
                            was_normalized = (matched_sym != raw_sym)
                            
                            # Cache the result
                            resolution_cache[raw_sym] = {
                                "matched": matched_sym,
                                "is_norm": was_normalized,
                                "risk_data": risk_lookup.get(matched_sym, {})
                            }
                            
                            # Log first-time discovery
                            if was_normalized and matched_sym in risk_lookup:
                                print(f"    └─ ✅ Normalized: {raw_sym} -> {matched_sym}")
                                total_symbols_normalized += 1
                        
                        res = resolution_cache[raw_sym]
                        matched_sym = res["matched"]
                        tf_data = res["risk_data"].get(raw_tf)

                        if tf_data and tf_data["usd_risk"] > 0:
                            # Apply Enforcement
                            item.update({
                                'exit': 0,
                                'target': 0,
                                'usd_risk': tf_data["usd_risk"],
                                'usd_based_risk_only': True,
                                'symbol': matched_sym
                            })
                            
                            # Update volume if specified
                            if tf_data["volume"] > 0:
                                for key in list(item.keys()):
                                    if 'volume' in key.lower():
                                        item[key] = tf_data["volume"]
                                        break
                                        
                            modified = True
                            investor_orders_enforced += 1
                            total_orders_enforced += 1
                        
                    if modified:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(data, f, indent=4)
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                except Exception as e:
                    print(f"    └─ ❌ Error in {file_path.name}: {e}")

        # Summary for this investor
        if investor_orders_enforced > 0:
            any_orders_enforced = True
            print(f"  └─ 📊 {current_inv_id} Complete: Enforced {investor_orders_enforced} orders across {investor_files_updated} files.")

    # Final Global Summary
    print(f"\n{'='*10} RISK ENFORCEMENT COMPLETE {'='*10}")
    print(f" Total Files Updated:   {total_files_updated}")
    print(f" Total Enforced:        {total_orders_enforced} / {total_orders_processed}")
    print(f" Symbols Normalized:    {total_symbols_normalized}")
    print(f"{'='*50}\n")
    
    return any_orders_enforced
    
def calculate_investor_symbols_orders(inv_id=None, callback_function=None):
    """
    Calculates Exit/Target prices for ALL orders in limit_orders.json files for investors.
    Uses strategy-specific risk_reward from strategies_risk_reward object in accountmanagement.json,
    falling back to selected_risk_reward if strategy not defined.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        callback_function (callable, optional): A function to call with the opened file data.
            The callback will receive (inv_id, file_path, orders_list, strategy_name, rr_ratio) parameters.
    
    Returns:
        bool: True if any orders were calculated, False otherwise
    """
    print(f"\n{'='*10} 📊 CALCULATING INVESTOR ORDER PRICES (Strategy-Specific R:R) {'='*10}")
    
    total_files_updated = 0
    total_orders_processed = 0
    total_orders_calculated = 0
    total_orders_skipped = 0
    total_symbols_normalized = 0
    strategies_used = {}  # Track which strategies used which R:R
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_calculated = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Processing orders with strategy-aware R:R...")

        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}

        # 1. Load accountmanagement.json to get risk reward configurations
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found for {current_inv_id}, skipping")
            continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_mgmt_data = json.load(f)
            
            # Get default selected_risk_reward
            selected_rr = acc_mgmt_data.get("selected_risk_reward", [1.0])
            if isinstance(selected_rr, list) and len(selected_rr) > 0:
                default_rr_ratio = float(selected_rr[0])
            else:
                default_rr_ratio = float(selected_rr) if selected_rr else 1.0
            
            # Get strategy-specific risk rewards
            strategies_rr = acc_mgmt_data.get("strategies_risk_reward", {})
            
            print(f"  └─ 📊 Default R:R ratio: {default_rr_ratio}")
            if strategies_rr:
                print(f"  └─ 📋 Strategy-specific R:R configured for: {', '.join(strategies_rr.keys())}")
            
        except Exception as e:
            print(f"  └─ ❌ Failed to load accountmanagement.json: {e}")
            continue

        # 2. Get broker config for potential symbol mapping context
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ⚠️  No broker config found for {current_inv_id}")
            # Continue anyway as normalization might still work

        # 3. Find all limit_orders.json files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        
        if not order_files:
            print(f"  └─ 🔘 No limit order files found")
            continue
            
        investor_files_updated = 0
        investor_orders_processed = 0
        investor_orders_calculated = 0
        investor_orders_skipped = 0
        
        # Process each file individually
        for file_path in order_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                
                if not orders:
                    continue
                
                # --- GET STRATEGY NAME FROM FOLDER STRUCTURE ---
                # Strategy folder is the parent of the pending_orders folder
                strategy_name = file_path.parent.parent.name
                
                # --- DETERMINE WHICH R:R RATIO TO USE FOR THIS STRATEGY ---
                # Check if this strategy has a specific R:R configured
                if strategy_name in strategies_rr:
                    rr_ratio = float(strategies_rr[strategy_name])
                    rr_source = f"strategy-specific ({strategy_name}: {rr_ratio})"
                else:
                    rr_ratio = default_rr_ratio
                    rr_source = f"default (selected_risk_reward: {rr_ratio})"
                
                # Track strategy usage
                if strategy_name not in strategies_used:
                    strategies_used[strategy_name] = {
                        'investor': current_inv_id,
                        'rr_ratio': rr_ratio,
                        'source': 'specific' if strategy_name in strategies_rr else 'default'
                    }
                
                print(f"  └─ 📂 Strategy: '{strategy_name}' using {rr_source}")
                
                # Call callback function if provided with the original data (now including strategy info)
                if callback_function:
                    try:
                        callback_function(current_inv_id, file_path, orders, strategy_name, rr_ratio)
                    except Exception as e:
                        print(f"    └─ ⚠️  Callback error for {file_path.name}: {e}")
                
                # Track original orders for this file
                original_count = len(orders)
                investor_orders_processed += original_count
                
                # Process each order in this file
                orders_updated = False
                file_orders_calculated = 0
                file_orders_skipped = 0
                
                for order in orders:
                    try:
                        # --- SYMBOL NORMALIZATION with Caching ---
                        raw_symbol = order.get("symbol", "")
                        if not raw_symbol:
                            file_orders_skipped += 1
                            continue
                        
                        # Check Cache First
                        if raw_symbol in resolution_cache:
                            normalized_symbol = resolution_cache[raw_symbol]
                        else:
                            # Perform mapping only once
                            normalized_symbol = get_normalized_symbol(raw_symbol)
                            resolution_cache[raw_symbol] = normalized_symbol
                            
                            # Log normalization on first discovery
                            if normalized_symbol != raw_symbol:
                                print(f"    └─ ✅ {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                                total_symbols_normalized += 1
                        
                        # Update the symbol in the order
                        if normalized_symbol:
                            order['symbol'] = normalized_symbol
                        
                        # --- CHECK FOR USD-BASED RISK FIRST (doesn't require volume) ---
                        if order.get("usd_based_risk_only") is True:
                            risk_val = float(order.get("usd_risk", 0))
                            
                            if risk_val > 0:
                                # For USD-based, we need volume but it might be named differently
                                # Try to find volume field
                                volume_value = None
                                for key, value in order.items():
                                    if 'volume' in key.lower() and isinstance(value, (int, float)):
                                        volume_value = float(value)
                                        break
                                
                                if volume_value is None or volume_value <= 0:
                                    print(f"      ⚠️  USD-based order missing volume for {order.get('symbol', 'Unknown')}, skipping")
                                    file_orders_skipped += 1
                                    continue
                                
                                # Find tick_size field
                                tick_size_value = None
                                for key, value in order.items():
                                    if 'tick_size' in key.lower() and isinstance(value, (int, float)):
                                        tick_size_value = float(value)
                                        break
                                
                                if tick_size_value is None or tick_size_value <= 0:
                                    tick_size_value = 0.00001
                                    print(f"      ⚠️  No tick_size found for {order.get('symbol', 'Unknown')}, using default")
                                
                                # Find tick_value field
                                tick_value_value = None
                                for key, value in order.items():
                                    if 'tick_value' in key.lower() and isinstance(value, (int, float)):
                                        tick_value_value = float(value)
                                        break
                                
                                if tick_value_value is None or tick_value_value <= 0:
                                    tick_value_value = 1.0
                                    print(f"      ⚠️  No tick_value found for {order.get('symbol', 'Unknown')}, using default")
                                
                                # Extract required order data
                                entry = float(order.get('entry', 0))
                                if entry == 0:
                                    file_orders_skipped += 1
                                    continue
                                    
                                order_type = str(order.get('order_type', '')).upper()
                                
                                # Calculate digits for rounding based on tick_size
                                if tick_size_value < 1:
                                    digits = len(str(tick_size_value).split('.')[-1])
                                else:
                                    digits = 0
                                
                                # Calculate using USD risk
                                sl_dist = (risk_val * tick_size_value) / (tick_value_value * volume_value)
                                tp_dist = sl_dist * rr_ratio
                                
                                if "BUY" in order_type:
                                    order["exit"] = round(entry - sl_dist, digits)
                                    order["target"] = round(entry + tp_dist, digits)
                                elif "SELL" in order_type:
                                    order["exit"] = round(entry + sl_dist, digits)
                                    order["target"] = round(entry - tp_dist, digits)
                                else:
                                    file_orders_skipped += 1
                                    continue
                                
                                file_orders_calculated += 1
                                any_orders_calculated = True
                                
                                # Update metadata with strategy info
                                order['risk_reward'] = rr_ratio
                                order['risk_reward_source'] = 'strategy_specific' if strategy_name in strategies_rr else 'default'
                                order['strategy_name'] = strategy_name
                                order['status'] = "Calculated"
                                order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                orders_updated = True
                                continue  # Skip the rest of the processing for this order
                            else:
                                file_orders_skipped += 1
                                continue
                        
                        # --- NON-USD BASED ORDERS (require volume) ---
                        # Check for required volume field
                        volume_field = None
                        volume_value = None
                        
                        for key, value in order.items():
                            if 'volume' in key.lower() and isinstance(value, (int, float)):
                                volume_field = key
                                volume_value = float(value)
                                break
                        
                        if volume_value is None or volume_value <= 0:
                            file_orders_skipped += 1
                            continue
                        
                        # Find tick_size field
                        tick_size_field = None
                        tick_size_value = None
                        
                        for key, value in order.items():
                            if 'tick_size' in key.lower() and isinstance(value, (int, float)):
                                tick_size_field = key
                                tick_size_value = float(value)
                                break
                        
                        if tick_size_value is None or tick_size_value <= 0:
                            tick_size_value = 0.00001
                            print(f"      ⚠️  No tick_size found for {order.get('symbol', 'Unknown')}, using default")
                        
                        # Find tick_value field
                        tick_value_field = None
                        tick_value_value = None
                        
                        for key, value in order.items():
                            if 'tick_value' in key.lower() and isinstance(value, (int, float)):
                                tick_value_field = key
                                tick_value_value = float(value)
                                break
                        
                        if tick_value_value is None or tick_value_value <= 0:
                            tick_value_value = 1.0
                            print(f"      ⚠️  No tick_value found for {order.get('symbol', 'Unknown')}, using default")
                        
                        # Extract required order data
                        entry = float(order.get('entry', 0))
                        if entry == 0:
                            file_orders_skipped += 1
                            continue
                            
                        order_type = str(order.get('order_type', '')).upper()
                        
                        # Calculate digits for rounding based on tick_size
                        if tick_size_value < 1:
                            digits = len(str(tick_size_value).split('.')[-1])
                        else:
                            digits = 0
                        
                        # Standard calculation based on exit or target
                        sl_price = float(order.get('exit', 0))
                        tp_price = float(order.get('target', 0))
                        
                        # Case 1: Target provided, need to calculate exit
                        if sl_price == 0 and tp_price > 0:
                            risk_dist = abs(tp_price - entry) / rr_ratio
                            if "BUY" in order_type:
                                order['exit'] = round(entry - risk_dist, digits)
                            elif "SELL" in order_type:
                                order['exit'] = round(entry + risk_dist, digits)
                            else:
                                file_orders_skipped += 1
                                continue
                            
                            file_orders_calculated += 1
                            any_orders_calculated = True
                        
                        # Case 2: Exit provided, need to calculate target
                        elif sl_price > 0:
                            risk_dist = abs(entry - sl_price)
                            if "BUY" in order_type:
                                order['target'] = round(entry + (risk_dist * rr_ratio), digits)
                            elif "SELL" in order_type:
                                order['target'] = round(entry - (risk_dist * rr_ratio), digits)
                            else:
                                file_orders_skipped += 1
                                continue
                            
                            file_orders_calculated += 1
                            any_orders_calculated = True
                            print(f"      ✅ [{strategy_name}] {order.get('symbol')} - Target calculated: {order['target']} (R:R={rr_ratio})")
                        
                        # Case 3: Neither exit nor target provided, skip
                        else:
                            file_orders_skipped += 1
                            continue
                        
                        # --- METADATA UPDATES with Strategy Info ---
                        order['risk_reward'] = rr_ratio
                        order['risk_reward_source'] = 'strategy_specific' if strategy_name in strategies_rr else 'default'
                        order['strategy_name'] = strategy_name
                        order['status'] = "Calculated"
                        order['calculated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        orders_updated = True
                        
                    except (ValueError, KeyError, TypeError, ZeroDivisionError) as e:
                        file_orders_skipped += 1
                        print(f"      ⚠️  Error processing order {order.get('symbol', 'Unknown')}: {e}")
                        continue
                
                # Save the updated orders back to the same file
                if orders_updated:
                    try:
                        with open(file_path, 'w', encoding='utf-8') as f:
                            json.dump(orders, f, indent=4)
                        
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                        # Update counters
                        investor_orders_calculated += file_orders_calculated
                        investor_orders_skipped += file_orders_skipped
                        
                        print(f"    └─ 📁 {strategy_name}/{file_path.parent.name}/limit_orders.json: "
                              f"Processed: {original_count}, Calculated: {file_orders_calculated}, "
                              f"Skipped: {file_orders_skipped} [R:R={rr_ratio}]")
                        
                    except Exception as e:
                        print(f"    └─ ❌ Failed to save {file_path}: {e}")
                
            except Exception as e:
                print(f"    └─ ❌ Error reading {file_path}: {e}")
                continue
        
        # Summary for current investor
        if investor_orders_processed > 0:
            total_orders_processed += investor_orders_processed
            total_orders_calculated += investor_orders_calculated
            total_orders_skipped += investor_orders_skipped
            
            print(f"\n  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"      Files updated: {investor_files_updated}")
            print(f"      Orders processed: {investor_orders_processed}")
            print(f"      Orders calculated: {investor_orders_calculated}")
            print(f"      Orders skipped: {investor_orders_skipped}")
            
            if investor_orders_processed > 0:
                calc_rate = (investor_orders_calculated / investor_orders_processed) * 100
                print(f"      Calculation rate: {calc_rate:.1f}%")
        else:
            print(f"  └─ ⚠️  No orders processed for {current_inv_id}")

    # Final Global Summary with Strategy Breakdown
    print(f"\n{'='*10} INVESTOR CALCULATION COMPLETE {'='*10}")
    if total_orders_processed > 0:
        print(f" Total Files Modified:    {total_files_updated}")
        print(f" Total Orders Processed:  {total_orders_processed}")
        print(f" Total Orders Calculated: {total_orders_calculated}")
        print(f" Total Orders Skipped:    {total_orders_skipped}")
        print(f" Symbols Normalized:      {total_symbols_normalized}")
        
        if total_orders_processed > 0:
            overall_rate = (total_orders_calculated / total_orders_processed) * 100
            print(f" Overall Calculation Rate: {overall_rate:.1f}%")
        
        # Show strategy R:R usage breakdown
        if strategies_used:
            print(f"\n {'='*10} STRATEGY R:R USAGE {'='*10}")
            for strategy, info in strategies_used.items():
                source_indicator = "🎯" if info['source'] == 'specific' else "📋"
                print(f" {source_indicator} {strategy}: R:R={info['rr_ratio']} ({info['source']})")
    else:
        print(" No orders were processed.")
    
    return any_orders_calculated

def padding_tight_usd_risk(inv_id=None):
    """
    Ranks orders, adjusts 'too tight' risk to 50% of the next order in line,
    and saves results back to the original limit_orders.json.
    """
    print(f"\n{'='*10} ⚖️ DYNAMIC RISK RANKING & SPACING {'='*10}")
    
    inv_base_path = Path(INV_PATH) 
    if not inv_base_path.exists(): 
        return False

    investor_folders = [inv_base_path / inv_id] if inv_id and (inv_base_path / inv_id).exists() else [f for f in inv_base_path.iterdir() if f.is_dir()]

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        # Target the standard limit_orders.json files
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        
        for file_path in order_files:
            try:
                # 1. Load the original file
                with open(file_path, 'r', encoding='utf-8') as f:
                    orders = json.load(f)
                if not orders: 
                    continue

                # 2. Group by timeframe and extract agnostic broker data
                timeframe_groups = {}
                for order in orders:
                    tf = order.get("timeframe", "Unknown")
                    if tf not in timeframe_groups: 
                        timeframe_groups[tf] = []
                    
                    # Agnostic extraction (suffix-based)
                    vol = next((v for k, v in order.items() if k.endswith("_volume")), 0)
                    t_size = next((v for k, v in order.items() if k.endswith("_tick_size")), 0)
                    t_val = next((v for k, v in order.items() if k.endswith("_tick_value")), 0)
                    entry = order.get("entry", 0)
                    exit_p = order.get("exit", 0)

                    # Initial risk calc
                    if vol > 0 and t_size > 0 and entry > 0 and exit_p > 0:
                        ticks = abs(entry - exit_p) / t_size
                        order["live_risk_usd"] = round(vol * ticks * t_val, 2)
                    else:
                        order["live_risk_usd"] = 0.0
                    
                    timeframe_groups[tf].append(order)

                rank_words = ["first", "second", "third", "fourth", "fifth"]
                updated_orders_list = []

                # 3. Process each group for the 50% spacing rule
                for tf, group in timeframe_groups.items():
                    # Sort by risk lowest -> highest
                    group.sort(key=lambda x: x.get("live_risk_usd", 0))
                    
                    for i in range(len(group)):
                        current = group[i]
                        
                        # Apply spacing if there's a higher risk order next in line
                        if i + 1 < len(group):
                            next_order = group[i+1]
                            curr_risk = current.get("live_risk_usd", 0)
                            next_risk = next_order.get("live_risk_usd", 0)
                            
                            threshold = round(next_risk / 2, 2)
                            
                            if curr_risk < threshold and threshold > 0:
                                vol = next((v for k, v in current.items() if k.endswith("_volume")), 0)
                                t_size = next((v for k, v in current.items() if k.endswith("_tick_size")), 0)
                                t_val = next((v for k, v in current.items() if k.endswith("_tick_value")), 0)
                                entry = current.get("entry")
                                
                                if vol > 0 and t_val > 0:
                                    new_dist = (threshold * t_size) / (vol * t_val)
                                    if "buy" in current.get("order_type", "").lower():
                                        current["exit"] = round(entry - new_dist, 5)
                                    else:
                                        current["exit"] = round(entry + new_dist, 5)
                                    
                                    current["live_risk_usd"] = threshold
                                    current["adjustment_note"] = f"Spaced to 50% of rank {i+2} (${threshold})"

                        # Remove old rank flags
                        for k in list(current.keys()):
                            if "_usd_risk" in k and any(w in k for w in rank_words):
                                del current[k]

                        # Apply current ranking flag (only for True values as requested)
                        if i < len(rank_words):
                            flag_name = f"{rank_words[i]}_lowest_{tf}_usd_risk"
                            current[flag_name] = True
                        
                        current["risk_calculated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        updated_orders_list.append(current)

                # 4. Save back to original limit_orders.json
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(updated_orders_list, f, indent=4)
                
                # 5. Cleanup: Remove the order_risks.json file if it exists
                legacy_risks_file = file_path.parent / "order_risks.json"
                if legacy_risks_file.exists():
                    os.remove(legacy_risks_file)
                
                print(f"  └─ [{current_inv_id}] Updated {file_path.name} and cleaned legacy files.")

            except Exception as e:
                print(f"  └─ ❌ Error processing {file_path}: {e}")

    return True

def live_usd_risk_and_scaling(inv_id=None, callback_function=None):
    """
    Calculates and populates the live USD risk for all orders in pending_orders/limit_orders.json files.
    Deduplicates orders first, scales volume, and SPLITS orders if they exceed max volume.
    """
    print(f"\n{'='*10} 💰 CALCULATING LIVE USD RISK WITH DEDUPLICATION & SPLITTING {'='*10}")
    
    total_files_updated = 0
    total_orders_updated = 0
    total_risk_usd = 0.0
    total_signals_created = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_processed = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Initializing pre-process cleanup and risk scaling...")

        resolution_cache = {}

        # 1. Load account management data
        account_mgmt_path = inv_folder / "accountmanagement.json"
        risk_ranges = {}
        account_balance = None
        
        if account_mgmt_path.exists():
            try:
                with open(account_mgmt_path, 'r', encoding='utf-8') as f:
                    account_data = json.load(f)
                risk_ranges = account_data.get('account_balance_default_risk_management', {})
                print(f"  └─ 📊 Loaded risk management ranges: {len(risk_ranges)} ranges")
            except Exception as e:
                print(f"  └─ ⚠️  Could not load accountmanagement.json: {e}")
        else:
            print(f"  └─ ⚠️  No accountmanagement.json found, skipping risk-based scaling")
            continue

        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {current_inv_id}")
            continue
        
        account_info = mt5.account_info()
        if account_info:
            account_balance = account_info.balance
            print(f"  └─ 💵 Live account balance: ${account_balance:,.2f}")
        else:
            print(f"  └─ ⚠️  Could not fetch account balance from broker")
            continue
        
        required_risk = 0
        tolerance_min = 0
        tolerance_max = 0
        
        for range_str, risk_value in risk_ranges.items():
            try:
                if '_risk' in range_str:
                    range_part = range_str.replace('_risk', '')
                    if '-' in range_part:
                        min_val, max_val = map(float, range_part.split('-'))
                        if min_val <= account_balance <= max_val:
                            required_risk = float(risk_value)
                            tolerance_min = required_risk
                            tolerance_max = required_risk + 0.99
                            print(f"  └─ 🎯 Balance ${account_balance:,.2f} falls in range {range_part}")
                            print(f"  └─ 🎯 Required risk: ${required_risk:.2f} (tolerance: ${tolerance_min:.2f} - ${tolerance_max:.2f})")
                            break
            except Exception as e:
                continue
        
        if required_risk == 0:
            print(f"  └─ ⚠️  No matching risk range found for balance ${account_balance:,.2f}")
            continue
        
        order_files = list(inv_folder.rglob("*/pending_orders/limit_orders.json"))
        
        if not order_files:
            print(f"  └─ 🔘 No limit order files found")
            continue
            
        investor_files_updated = 0
        investor_orders_updated = 0
        investor_risk_usd = 0.0
        investor_signals_count = 0
        
        broker_prefix = broker_cfg.get('BROKER_NAME', '').lower()
        if not broker_prefix:
            server = broker_cfg.get('SERVER', '')
            broker_prefix = server.split('-')[0].split('.')[0].lower() if server else 'broker'
        
        print(f"  └─ 🏷️  Using broker prefix: '{broker_prefix}' for field names")
        
        for file_path in order_files:
            try:
                # --- PRE-PROCESS DEDUPLICATION ---
                with open(file_path, 'r', encoding='utf-8') as f:
                    raw_orders = json.load(f)
                
                if not raw_orders:
                    continue

                # Deduplicate based on symbol, entry, and exit
                unique_orders = []
                seen_keys = set()
                for o in raw_orders:
                    key = (o.get('symbol'), o.get('entry'), o.get('exit'))
                    if key not in seen_keys:
                        unique_orders.append(o)
                        seen_keys.add(key)
                
                if len(unique_orders) < len(raw_orders):
                    print(f"    └─ 🧹 Cleaned {len(raw_orders) - len(unique_orders)} duplicate orders from {file_path.name}")
                
                orders = unique_orders

                # Clear signals.json for this specific folder to prevent stale data mixing with splits
                signals_path = file_path.parent / "signals.json"
                if signals_path.exists():
                    with open(signals_path, 'w', encoding='utf-8') as f:
                        json.dump([], f)
                    print(f"    └─ 🚿 Cleared existing signals.json for fresh split generation")

                # --- START PROCESSING ---
                if callback_function:
                    try:
                        callback_function(current_inv_id, file_path, orders)
                    except Exception as e:
                        print(f"    └─ ⚠️  Callback error: {e}")
                
                orders_modified = False
                file_risk_total = 0.0
                file_signals = []
                
                for order in orders:
                    raw_symbol = order.get("symbol", "")
                    if not raw_symbol:
                        continue
                    
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                        if normalized_symbol != raw_symbol:
                            print(f"    └─ ✅ {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                            total_symbols_normalized += 1
                    
                    symbol = normalized_symbol if normalized_symbol else raw_symbol
                    order['symbol'] = symbol
                    
                    volume_field = f"{broker_prefix}_volume"
                    tick_size_field = f"{broker_prefix}_tick_size"
                    tick_value_field = f"{broker_prefix}_tick_value"
                    
                    current_volume = order.get(volume_field)
                    tick_size = order.get(tick_size_field)
                    tick_value = order.get(tick_value_field)
                    
                    if None in (current_volume, tick_size, tick_value):
                        print(f"    └─ ⚠️  Missing broker fields for {symbol}, skipping")
                        continue
                    
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info:
                        print(f"    └─ ⚠️  Could not fetch current price for {symbol}, skipping")
                        continue
                    
                    entry_price = order.get("entry")
                    exit_price = order.get("exit")
                    
                    if not entry_price or not exit_price:
                        continue
                    
                    stop_distance_pips = abs(entry_price - exit_price)
                    ticks_in_stop = stop_distance_pips / tick_size if tick_size > 0 else 0
                    
                    volume_step = symbol_info.volume_step
                    min_volume = symbol_info.volume_min
                    max_volume = symbol_info.volume_max
                    
                    best_volume = current_volume
                    best_risk = 0
                    test_volume = current_volume if current_volume >= min_volume else min_volume
                    test_risk = test_volume * ticks_in_stop * tick_value
                    
                    print(f"    └─ 📈 {symbol}: Starting volume {test_volume} -> risk ${test_risk:.2f}")
                    
                    if test_risk > tolerance_max:
                        print(f"      └─ ⬇️  Risk too high (${test_risk:.2f} > ${tolerance_max:.2f}), scaling down...")
                        while test_risk > tolerance_max and test_volume > min_volume:
                            test_volume = max(min_volume, test_volume - volume_step)
                            test_risk = test_volume * ticks_in_stop * tick_value
                        best_volume = test_volume
                        best_risk = test_risk
                    
                    elif test_risk < tolerance_min:
                        print(f"      └─ ⬆️  Risk too low (${test_risk:.2f} < ${tolerance_min:.2f}), scaling up...")
                        while test_risk < tolerance_min:
                            previous_volume = test_volume
                            previous_risk = test_risk
                            test_volume = test_volume + volume_step
                            test_risk = test_volume * ticks_in_stop * tick_value
                            
                            if test_risk > tolerance_max:
                                best_volume = previous_volume
                                best_risk = previous_risk
                                print(f"      └─ ✅ Using volume: {best_volume:.3f} (risk ${best_risk:.2f}) to avoid overshoot")
                                break
                        else:
                            best_volume = test_volume
                            best_risk = test_risk
                    else:
                        best_volume = test_volume
                        best_risk = test_risk
                        print(f"      └─ ✅ Already within tolerance: volume {best_volume:.3f} (risk ${best_risk:.2f})")

                    order[volume_field] = round(best_volume, 2)
                    order["risk_in_usd"] = round(best_risk, 2)
                    order["risk_calculated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    order["required_risk_target"] = required_risk
                    order["risk_tolerance_min"] = round(tolerance_min, 2)
                    order["risk_tolerance_max"] = round(tolerance_max, 2)
                    order["account_balance_at_calc"] = round(account_balance, 2)
                    order["current_bid"] = round(symbol_info.bid, 6) if hasattr(symbol_info, 'bid') else None
                    order["current_ask"] = round(symbol_info.ask, 6) if hasattr(symbol_info, 'ask') else None
                    
                    orders_modified = True
                    investor_orders_updated += 1
                    total_orders_updated += 1
                    file_risk_total += best_risk
                    
                    # --- SPLITTING LOGIC ---
                    if best_risk >= tolerance_min * 0.5 and best_risk > 0:
                        remaining_volume = best_volume
                        is_split = best_volume > max_volume
                        
                        while remaining_volume > 0.0001:
                            chunk_volume = min(remaining_volume, max_volume)
                            if chunk_volume < min_volume and remaining_volume != best_volume:
                                break
                                
                            signal_order = order.copy()
                            signal_order[volume_field] = round(chunk_volume, 2)
                            signal_order["split_order"] = is_split
                            signal_order["moved_to_signals_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            
                            file_signals.append(signal_order)
                            investor_signals_count += 1
                            total_signals_created += 1
                            remaining_volume -= chunk_volume

                        if is_split:
                            print(f"      └─ 🟢 Qualified & SPLIT (Total Vol: {best_volume:.2f}, Max Vol: {max_volume})")
                        else:
                            print(f"      └─ 🟢 Qualified for signals.json (risk ${best_risk:.2f})")
                
                # Save cleaned and updated limit orders
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(orders, f, indent=4)
                
                investor_files_updated += 1
                total_files_updated += 1
                investor_risk_usd += file_risk_total
                total_risk_usd += file_risk_total
                any_orders_processed = True
                
                if file_signals:
                    try:
                        # We already cleared signals.json at the start, so we just write the new ones
                        with open(signals_path, 'w', encoding='utf-8') as f:
                            json.dump(file_signals, f, indent=4)
                        
                        print(f"  └─ 📊 signals.json: Created {len(file_signals)} clean signals (splits included)")
                    except Exception as e:
                        print(f"  └─ ❌ Error writing signals.json: {e}")
                
            except Exception as e:
                print(f"  └─ ❌ Error processing {file_path}: {e}")
                continue
        
        # Summary for current investor
        if investor_orders_updated > 0:
            print(f"\n  └─ {'='*40}")
            print(f"  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"  └─     Files Processed:    {investor_files_updated}")
            print(f"  └─     Orders Risk-Scaled: {investor_orders_updated}")
            print(f"  └─     Total Risk:         ${investor_risk_usd:,.2f}")
            print(f"  └─     Signals Generated:  {investor_signals_count}")
            print(f"  └─ {'='*40}")

    print(f"\n{'='*10} USD RISK CALCULATION COMPLETE {'='*10}")
    return any_orders_processed

def apply_default_prices(inv_id=None, callback_function=None):
    """
    Applies default prices from limit_orders_backup.json to signals.json when default_price is true.
    Copies exit/target prices from backup to matching orders in signals.json, handling symbol normalization.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        callback_function (callable, optional): A function to call with the opened file data.
            The callback will receive (inv_id, backup_file_path, signals_file_path, modifications) parameters.
    
    Returns:
        bool: True if any orders were modified, False otherwise
    """
    print(f"\n{'='*10} 💰 APPLYING DEFAULT PRICES FROM BACKUP {'='*10}")
    
    total_orders_modified = 0
    total_files_updated = 0
    total_symbols_normalized = 0
    inv_base_path = Path(INV_PATH)

    if not inv_base_path.exists():
        print(f" [!] Error: Investor path {INV_PATH} does not exist.")
        return False

    # Determine which investors to process
    if inv_id:
        inv_folder = inv_base_path / inv_id
        investor_folders = [inv_folder] if inv_folder.exists() else []
        if not investor_folders:
            print(f" [!] Error: Investor folder {inv_id} does not exist.")
            return False
    else:
        investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    any_orders_modified = False

    for inv_folder in investor_folders:
        current_inv_id = inv_folder.name
        print(f"\n [{current_inv_id}] 🔍 Checking default price setting...")

        # --- INVESTOR LOCAL CACHE for symbol normalization ---
        resolution_cache = {}

        # 1. Load accountmanagement.json to check default_price setting
        account_mgmt_path = inv_folder / "accountmanagement.json"
        if not account_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found, skipping")
            continue

        try:
            with open(account_mgmt_path, 'r', encoding='utf-8') as f:
                account_data = json.load(f)
            
            settings = account_data.get('settings', {})
            default_price_enabled = settings.get('default_price', False)
            
            if not default_price_enabled:
                print(f"  └─ ⏭️  default_price is FALSE - skipping investor (set to true to apply default prices)")
                continue
                
            print(f"  └─ ✅ default_price is TRUE - will apply prices from backup")
            
        except Exception as e:
            print(f"  └─ ❌ Error reading accountmanagement.json: {e}")
            continue

        # 2. Load broker config for symbol handling
        broker_cfg = usersdictionary.get(current_inv_id)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {current_inv_id}")
            continue

        # 3. Find all limit_orders_backup.json files
        backup_files = list(inv_folder.rglob("*/pending_orders/limit_orders_backup.json"))
        
        if not backup_files:
            print(f"  └─ 🔘 No limit_orders_backup.json files found")
            continue
        
        print(f"  └─ 📁 Found {len(backup_files)} backup files to process")

        investor_orders_modified = 0
        investor_files_updated = 0
        investor_symbols_normalized = 0

        # 4. Process each backup file
        for backup_path in backup_files:
            folder_path = backup_path.parent.parent  # Gets the strategy folder (e.g., double-levels)
            signals_path = backup_path.parent / "signals.json"  # Same directory as backup
            
            # Check if signals.json exists
            if not signals_path.exists():
                print(f"  └─ ⚠️  No signals.json found in {backup_path.parent} (same folder as backup), skipping")
                continue
            
            print(f"\n  └─ 📂 Processing folder: {folder_path.name}")
            print(f"      ├─ Backup: {backup_path.name}")
            print(f"      └─ Signals: {signals_path.name}")
            
            try:
                # Load backup orders
                with open(backup_path, 'r', encoding='utf-8') as f:
                    backup_orders = json.load(f)
                
                # Load signals
                with open(signals_path, 'r', encoding='utf-8') as f:
                    signals = json.load(f)
                
                if not backup_orders:
                    print(f"    └─ ⚠️  Empty backup file")
                    continue
                    
                if not signals:
                    print(f"    └─ ⚠️  Empty signals file")
                    continue
                
                # Create lookup dictionaries for backup orders with multiple matching strategies
                backup_lookup = {}  # (symbol, timeframe, order_type) -> order
                
                print(f"    └─ 📊 Processing {len(backup_orders)} backup orders and {len(signals)} signals")
                
                # First, let's analyze what's in the backup for AUDCAD 15m sell_limit
                audcad_backups = []
                for order in backup_orders:
                    # --- SYMBOL NORMALIZATION for backup symbols ---
                    raw_symbol = str(order.get('symbol', '')).upper()
                    if not raw_symbol:
                        continue
                    
                    # Check Cache First for backup symbol
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        # Perform mapping only once
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                        
                        # Log normalization on first discovery
                        if normalized_symbol != raw_symbol:
                            print(f"      └─ ✅ Backup: {raw_symbol} -> {normalized_symbol} (Mapped & Cached)")
                            investor_symbols_normalized += 1
                            total_symbols_normalized += 1
                    
                    # Use normalized symbol for lookup
                    symbol = normalized_symbol if normalized_symbol else raw_symbol
                    timeframe = str(order.get('timeframe', '')).upper()
                    order_type = str(order.get('order_type', '')).lower()
                    
                    if symbol == 'AUDCAD' and timeframe == '15M' and order_type == 'sell_limit':
                        audcad_backups.append(order)
                        print(f"      └─ 📌 Found AUDCAD 15M sell_limit in backup with exit: {order.get('exit')}")
                    
                    if symbol and timeframe and order_type:
                        # Store with normalized symbol
                        key = (symbol, timeframe, order_type)
                        backup_lookup[key] = order
                
                print(f"    └─ 📊 Created lookup for {len(backup_lookup)} backup orders")
                
                # Process signals and apply default prices
                modified = False
                signals_modified_count = 0
                modifications_log = []
                
                for signal in signals:
                    # --- SYMBOL NORMALIZATION for signal symbols ---
                    raw_signal_symbol = str(signal.get('symbol', '')).upper()
                    if not raw_signal_symbol:
                        continue
                    
                    # Check Cache First for signal symbol
                    if raw_signal_symbol in resolution_cache:
                        signal_symbol = resolution_cache[raw_signal_symbol]
                    else:
                        # Perform mapping only once
                        signal_symbol = get_normalized_symbol(raw_signal_symbol)
                        resolution_cache[raw_signal_symbol] = signal_symbol
                        
                        # Log normalization on first discovery
                        if signal_symbol != raw_signal_symbol:
                            print(f"      └─ ✅ Signal: {raw_signal_symbol} -> {signal_symbol} (Mapped & Cached)")
                            investor_symbols_normalized += 1
                            total_symbols_normalized += 1
                    
                    signal_timeframe = str(signal.get('timeframe', '')).upper()
                    signal_type = str(signal.get('order_type', '')).lower()
                    
                    if not all([signal_symbol, signal_timeframe, signal_type]):
                        print(f"      └─ ⚠️  Signal missing required fields: {signal}")
                        continue
                    
                    # Special debug for AUDCAD+ 15M (now normalized to AUDCAD)
                    if raw_signal_symbol == 'AUDCAD+' and signal_timeframe == '15M' and signal_type == 'sell_limit':
                        print(f"      └─ 🔍 DEBUG: Processing AUDCAD+ 15M sell_limit signal (normalized to {signal_symbol})")
                        print(f"          Current exit: {signal.get('exit')}, target: {signal.get('target')}")
                    
                    # Try to find matching backup order
                    matched_backup = None
                    match_method = None
                    
                    # Method 1: Direct symbol match with normalized symbols
                    backup_key = (signal_symbol, signal_timeframe, signal_type)
                    if backup_key in backup_lookup:
                        matched_backup = backup_lookup[backup_key]
                        match_method = "direct match"
                        if raw_signal_symbol == 'AUDCAD+':
                            print(f"      └─ ✓ Found direct match for normalized AUDCAD")
                    
                    if matched_backup:
                        # Check if we need to update any prices
                        updates_made = False
                        
                        # Get backup values
                        backup_exit = matched_backup.get('exit', 0)
                        backup_target = matched_backup.get('target', 0)
                        
                        # Current signal values
                        current_exit = signal.get('exit', 0)
                        current_target = signal.get('target', 0)
                        
                        update_details = []
                        
                        # Apply backup exit if not zero and different from current
                        if backup_exit != 0 and backup_exit != current_exit:
                            signal['exit'] = backup_exit
                            updates_made = True
                            update_details.append(f"exit: {current_exit} -> {backup_exit}")
                        
                        # Apply backup target if not zero and different from current
                        if backup_target != 0 and backup_target != current_target:
                            signal['target'] = backup_target
                            updates_made = True
                            update_details.append(f"target: {current_target} -> {backup_target}")
                        
                        if updates_made:
                            # Add metadata about the update
                            signal['price_updated_from_backup'] = True
                            signal['price_updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            signal['backup_match_method'] = match_method
                            signal['original_symbol'] = raw_signal_symbol
                            
                            signals_modified_count += 1
                            investor_orders_modified += 1
                            total_orders_modified += 1
                            any_orders_modified = True
                            modified = True
                            
                            modifications_log.append({
                                'symbol': raw_signal_symbol,
                                'normalized_symbol': signal_symbol,
                                'timeframe': signal_timeframe,
                                'type': signal_type,
                                'updates': {
                                    'exit': backup_exit if backup_exit != 0 else None,
                                    'target': backup_target if backup_target != 0 else None
                                },
                                'match_method': match_method,
                                'update_details': ', '.join(update_details)
                            })
                            
                            print(f"      └─ 🔄 [{raw_signal_symbol} -> {signal_symbol}] {', '.join(update_details)} [{match_method}]")
                        else:
                            if raw_signal_symbol == 'AUDCAD+':
                                print(f"      └─ ✓ AUDCAD+ already has correct prices (exit={current_exit}, target={current_target})")
                    else:
                        # Debug: Show unmatched signals with more detail
                        if raw_signal_symbol == 'AUDCAD+':
                            print(f"      └─ ❌ FAILED to find match for AUDCAD+ 15M sell_limit (normalized to {signal_symbol})")
                            print(f"          Looking for backup_key: ({signal_symbol}, {signal_timeframe}, {signal_type})")
                            
                            # Show all available backup keys
                            print(f"          Available backup keys:")
                            for (bsym, btf, btype) in list(backup_lookup.keys())[:10]:
                                if btf == signal_timeframe and btype == signal_type:
                                    print(f"            • ({bsym}, {btf}, {btype})")
                        else:
                            print(f"      └─ ⚠️  No backup match for: {raw_signal_symbol} -> {signal_symbol} ({signal_timeframe}, {signal_type})")
                
                # Save modified signals file
                if modified:
                    try:
                        with open(signals_path, 'w', encoding='utf-8') as f:
                            json.dump(signals, f, indent=4)
                        
                        investor_files_updated += 1
                        total_files_updated += 1
                        
                        print(f"    └─ 📝 Updated {signals_modified_count} orders in signals.json")
                        
                        # Call callback if provided
                        if callback_function:
                            try:
                                callback_function(current_inv_id, backup_path, signals_path, modifications_log)
                            except Exception as e:
                                print(f"    └─ ⚠️  Callback error: {e}")
                        
                        # Show summary of modifications
                        if modifications_log:
                            print(f"    └─ 📋 Modification Summary:")
                            for mod in modifications_log[:5]:  # Show first 5
                                norm_info = f" -> {mod['normalized_symbol']}" if mod['symbol'] != mod['normalized_symbol'] else ""
                                print(f"      • {mod['symbol']}{norm_info} ({mod['timeframe']}): {mod['update_details']} [{mod['match_method']}]")
                            if len(modifications_log) > 5:
                                print(f"      • ... and {len(modifications_log) - 5} more")
                    
                    except Exception as e:
                        print(f"    └─ ❌ Error saving signals.json: {e}")
                else:
                    print(f"    └─ ✓ No price updates needed for signals in {folder_path.name}")
                
            except Exception as e:
                print(f"  └─ ❌ Error processing {backup_path}: {e}")
                continue

        # Investor summary
        if investor_orders_modified > 0:
            print(f"\n  └─ {'='*40}")
            print(f"  └─ ✨ Investor {current_inv_id} Summary:")
            print(f"  └─    Folders Processed:   {len(backup_files)}")
            print(f"  └─    Signals Files Updated: {investor_files_updated}")
            print(f"  └─    Orders Modified:     {investor_orders_modified}")
            if investor_symbols_normalized > 0:
                print(f"  └─    Symbols Normalized:   {investor_symbols_normalized}")
            print(f"  └─ {'='*40}")
        else:
            print(f"\n  └─ ⚠️  No modifications made for {current_inv_id}")

    # Final Global Summary
    print(f"\n{'='*10} DEFAULT PRICE APPLICATION COMPLETE {'='*10}")
    if total_orders_modified > 0:
        print(f" Total Files Updated:       {total_files_updated}")
        print(f" Total Orders Modified:     {total_orders_modified}")
        if total_symbols_normalized > 0:
            print(f" Total Symbols Normalized:  {total_symbols_normalized}")
        print(f"\n ✓ Default prices successfully applied from backup files")
    else:
        print(" No orders were modified.")
        print(" └─ Possible reasons:")
        print("    • default_price is false in accountmanagement.json")
        print("    • No matching orders found between backup and signals")
        print("    • All exit/target prices already match backup values")
        print("    • No limit_orders_backup.json files found")
    
    return any_orders_modified

def place_usd_orders(inv_id=None):
    """
    Places pending orders from signals.json files for investors.
    
    ENHANCED FEATURES:
    1. Detailed tradeshistory.json with complete order information
    2. Per-order cache to prevent duplicate placement in same run
    3. Proximity risk check against existing positions
    4. Order regulation - always cancels unauthorized orders
    5. Dynamic order types: buy_stop, sell_stop, buy_limit, sell_limit, instant_buy, instant_sell
    6. No conversion logic - places exactly what signals.json specifies
    7. Uses 'volume' field from signals (ignores deriv_ prefix)
    8. Recursively finds signals.json in ALL strategy subfolders
    """
    
    # --- SUB-FUNCTION 1: CHECK AUTHORIZATION STATUS ---
    def check_authorization_status(investor_root):
        """Check activities.json for unauthorized actions and bypass status"""
        activities_path = investor_root / "activities.json"
        if not activities_path.exists():
            print(f"    ✅ No activities.json found - proceeding with order placement")
            return True, None
        
        try:
            with open(activities_path, 'r', encoding='utf-8') as f:
                activities = json.load(f)
            unauthorized_detected = activities.get('unauthorized_action_detected', False)
            bypass_active = activities.get('bypass_restriction', False)
            autotrading_active = activities.get('activate_autotrading', False)
            
            if unauthorized_detected:
                if bypass_active and autotrading_active:
                    print(f"    ⚠️  Unauthorized actions detected but BYPASS ACTIVE - proceeding")
                    return True, activities
                else:
                    print(f"    🚫 Unauthorized actions detected - ORDER PLACEMENT BLOCKED")
                    if not bypass_active: print(f"       - Bypass restriction: DISABLED")
                    if not autotrading_active: print(f"       - Auto-trading: DISABLED")
                    return False, activities
            print(f"    ✅ No unauthorized actions detected - proceeding")
            return True, activities
        except Exception as e:
            print(f"    ⚠️  Error reading activities.json: {e}")
            return True, None

    # --- SUB-FUNCTION 2: GET ORDER TYPE CONSTANTS ---
    def get_mt5_order_type(order_type_str):
        """Convert order type string to MT5 constant"""
        order_type_map = {
            'buy_stop': mt5.ORDER_TYPE_BUY_STOP,
            'sell_stop': mt5.ORDER_TYPE_SELL_STOP,
            'buy_limit': mt5.ORDER_TYPE_BUY_LIMIT,
            'sell_limit': mt5.ORDER_TYPE_SELL_LIMIT,
            'instant_buy': mt5.ORDER_TYPE_BUY,
            'instant_sell': mt5.ORDER_TYPE_SELL
        }
        return order_type_map.get(order_type_str.lower())

    # --- SUB-FUNCTION 3: GET VOLUME FROM SIGNAL ---
    def get_volume_from_signal(order_data):
        """
        Extract volume from signal data.
        Looks for 'volume' field first, regardless of broker prefix.
        Returns default 0.01 if not found.
        """
        # First try direct 'volume' field
        if 'volume' in order_data:
            return float(order_data['volume'])
        
        # If not found, look for any key that ends with 'volume' (like deriv_volume, broker_volume, etc.)
        for key, value in order_data.items():
            if key.endswith('volume'):
                try:
                    return float(value)
                except (ValueError, TypeError):
                    continue
        
        # Default volume
        return 0.01

    # --- SUB-FUNCTION 4: CHECK PROXIMITY RISK ---
    def check_proximity_risk(order, existing_positions):
        """
        Check if order is too close to existing positions using risk-based calculation.
        Returns (is_risk, closest_position, risk_amount, threshold)
        """
        symbol = order.get('symbol')
        order_type = order.get('order_type', '').lower()
        entry_price = float(order.get('entry', 0))
        volume = get_volume_from_signal(order)
        
        is_buy = 'buy' in order_type
        is_sell = 'sell' in order_type
        
        if not is_buy and not is_sell:
            return False, None, 0, 0
        
        # Filter positions for this symbol
        symbol_positions = [p for p in existing_positions if p.symbol == symbol]
        if not symbol_positions:
            return False, None, 0, 0
        
        for position in symbol_positions:
            position_type = position.type
            position_entry = position.price_open
            position_volume = position.volume
            position_sl = position.sl
            position_ticket = position.ticket
            
            is_position_buy = (position_type == mt5.ORDER_TYPE_BUY)
            is_position_sell = (position_type == mt5.ORDER_TYPE_SELL)
            
            # Calculate position risk from SL
            position_risk = 0
            if position_sl and position_sl > 0:
                if is_position_buy:
                    risk_profit = mt5.order_calc_profit(
                        mt5.ORDER_TYPE_BUY, symbol, position_volume,
                        position_entry, position_sl
                    )
                else:
                    risk_profit = mt5.order_calc_profit(
                        mt5.ORDER_TYPE_SELL, symbol, position_volume,
                        position_entry, position_sl
                    )
                if risk_profit:
                    position_risk = abs(risk_profit)
            
            if position_risk == 0:
                continue
            
            risk_threshold = position_risk / 2  # 50% threshold
            
            # SAME DIRECTION CHECK
            if (is_buy and is_position_buy) or (is_sell and is_position_sell):
                # Calculate potential risk if order triggers
                if is_sell and is_position_sell:
                    if entry_price < position_entry:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        ) or 0)
                    else:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        ) or 0)
                elif is_buy and is_position_buy:
                    if entry_price > position_entry:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        ) or 0)
                    else:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        ) or 0)
                else:
                    continue
                
                if potential_risk and potential_risk < risk_threshold:
                    return True, position, potential_risk, risk_threshold
            
            # OPPOSITE DIRECTION CHECK
            elif (is_buy and is_position_sell) or (is_sell and is_position_buy):
                if is_buy and is_position_sell:
                    if entry_price > position_entry:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        ) or 0)
                    else:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        ) or 0)
                elif is_sell and is_position_buy:
                    if entry_price < position_entry:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        ) or 0)
                    else:
                        potential_risk = abs(mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        ) or 0)
                else:
                    continue
                
                if potential_risk and potential_risk < risk_threshold:
                    return True, position, potential_risk, risk_threshold
        
        return False, None, 0, 0

    # --- SUB-FUNCTION 5: REGULATE ORDERS (CANCEL UNAUTHORIZED) ---
    def regulate_orders(investor_root, authorized_keys):
        """
        Cancel ALL pending orders that are NOT in the authorized list.
        Always runs to keep account clean.
        """
        print(f"    🔍 Regulating orders - cancelling unauthorized pending orders...")
        try:
            # Load tradeshistory.json to get authorized tickets/magics
            history_path = investor_root / "tradeshistory.json"
            authorized_tickets = set()
            authorized_magics = set()
            
            if history_path.exists():
                with open(history_path, 'r', encoding='utf-8') as f:
                    history = json.load(f)
                    for trade in history:
                        if trade.get('ticket'):
                            authorized_tickets.add(int(trade['ticket']))
                        if trade.get('magic'):
                            authorized_magics.add(int(trade['magic']))
            
            pending_orders = mt5.orders_get() or []
            cancelled_count = 0
            
            for order in pending_orders:
                is_authorized = False
                
                # Check by ticket
                if order.ticket in authorized_tickets:
                    is_authorized = True
                # Check by magic
                elif order.magic in authorized_magics:
                    is_authorized = True
                # Check by symbol+type+price+volume (from authorized_keys)
                else:
                    order_key = f"{order.symbol}_{order.type}_{round(order.price, 5)}_{round(order.volume_current, 2)}"
                    if order_key in authorized_keys:
                        is_authorized = True
                
                if not is_authorized:
                    request = {
                        "action": mt5.TRADE_ACTION_REMOVE,
                        "order": order.ticket,
                        "comment": "Cancelled by regulation - unauthorized"
                    }
                    result = mt5.order_send(request)
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"        ✅ Cancelled unauthorized order: #{order.ticket} ({order.symbol})")
                        cancelled_count += 1
            
            if cancelled_count > 0:
                print(f"    ✅ Regulation complete: Cancelled {cancelled_count} unauthorized orders")
            else:
                print(f"    ✅ No unauthorized orders found")
                
            return cancelled_count
            
        except Exception as e:
            print(f"    ⚠️  Error during regulation: {e}")
            return 0

    # --- SUB-FUNCTION 6: SYNC & SAVE DETAILED HISTORY ---
    def sync_and_save_detailed_history(investor_root, new_trade=None):
        """
        Synchronizes tradeshistory.json with MT5 terminal.
        Stores COMPLETE order information including all signal fields.
        """
        try:
            history_path = investor_root / "tradeshistory.json"
            
            print(f"      📂 Tradeshistory path: {history_path}")
            
            history = []
            if history_path.exists():
                try:
                    with open(history_path, 'r', encoding='utf-8') as f:
                        history = json.load(f)
                    print(f"      📋 Loaded {len(history)} existing trades")
                except Exception as e:
                    print(f"      ⚠️  Error reading tradeshistory.json: {e}")
                    history = []

            # Add new trade if provided
            if new_trade:
                existing_ticket = any(t.get('ticket') == new_trade.get('ticket') for t in history)
                if not existing_ticket:
                    history.append(new_trade)
                    print(f"      ➕ Added new trade: Ticket {new_trade.get('ticket')}")
                else:
                    print(f"      ℹ️  Trade Ticket {new_trade.get('ticket')} already exists")

            # Sync all records with MT5
            active_orders = {o.ticket: o for o in (mt5.orders_get() or [])}
            active_positions = {p.ticket: p for p in (mt5.positions_get() or [])}
            
            # Fetch history deals
            from_date = datetime.now() - timedelta(days=7)
            history_deals = mt5.history_deals_get(from_date, datetime.now()) or []
            deals_by_order = {deal.order: deal for deal in history_deals}
            
            updated_count = 0
            for trade in history:
                ticket = trade.get('ticket')
                if not ticket:
                    continue
                
                old_status = trade.get('status', 'unknown')
                
                if ticket in active_orders or ticket in active_positions:
                    trade['status'] = 'pending'
                    if old_status != 'pending':
                        print(f"      🔄 Trade {ticket}: {old_status} → pending")
                        updated_count += 1
                
                elif ticket in deals_by_order:
                    deal = deals_by_order[ticket]
                    trade['status'] = 'closed'
                    trade['profit'] = deal.profit
                    trade['commission'] = deal.commission
                    trade['swap'] = deal.swap
                    trade['close_price'] = deal.price
                    trade['close_time'] = datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d %H:%M:%S')
                    if old_status != 'closed':
                        print(f"      🔄 Trade {ticket}: {old_status} → closed (profit: ${deal.profit:.2f})")
                        updated_count += 1
                
                else:
                    if trade.get('status') != 'closed':
                        trade['status'] = 'closed'
                        trade['close_reason'] = 'expired_or_not_found'
                        print(f"      🔄 Trade {ticket}: {old_status} → closed (expired)")
                        updated_count += 1

            # Save updated history
            with open(history_path, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=4)
            
            if new_trade:
                print(f"      ✅ Saved new trade to tradeshistory.json")
            elif updated_count > 0:
                print(f"      ✅ Updated {updated_count} trades in tradeshistory.json")
            
            # Save backup
            backup_path = investor_root / "tradeshistory_backup.json"
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(history, f, indent=4)
            
            return True
            
        except Exception as e:
            print(f"      ❌ Error in sync_and_save_history: {e}")
            return False

    # --- SUB-FUNCTION 7: COLLECT ORDERS FROM SIGNALS (UPDATED) ---
    def collect_orders_from_signals(investor_root, resolution_cache):
        """
        Collect all orders from signals.json files in ALL strategy subfolders.
        Path structure: investors/{investor_id}/{strategy_name}/signals.json
        """
        entries_with_paths = []
        
        # Find ALL signals.json files in any subfolder of investor_root
        signals_files = list(investor_root.rglob("signals.json"))
        
        if not signals_files:
            print(f"    ℹ️  No signals.json files found in {investor_root} or its subfolders")
            return []
        
        print(f"    📁 Found {len(signals_files)} signals.json files in strategy folders:")
        
        for signals_path in signals_files:
            # Extract strategy name from parent folder
            strategy_name = signals_path.parent.name
            print(f"       • Strategy: {strategy_name} - {signals_path}")
            
            try:
                with open(signals_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not data:
                    print(f"         ⚠️  Empty signals.json file")
                    continue
                
                print(f"         📡 Found {len(data)} signals")
                
                for entry in data:
                    # Normalize symbol
                    raw_symbol = entry.get("symbol", "")
                    if raw_symbol in resolution_cache:
                        normalized_symbol = resolution_cache[raw_symbol]
                    else:
                        normalized_symbol = get_normalized_symbol(raw_symbol)
                        resolution_cache[raw_symbol] = normalized_symbol
                    
                    entry['symbol'] = normalized_symbol
                    entry['strategy_name'] = strategy_name  # Add strategy name to entry
                    
                    entries_with_paths.append({
                        'data': entry,
                        'path': signals_path,
                        'strategy': strategy_name
                    })
                    
            except json.JSONDecodeError as e:
                print(f"         ❌ Invalid JSON in {signals_path}: {e}")
                continue
            except Exception as e:
                print(f"         ⚠️  Error reading {signals_path}: {e}")
                continue
        
        print(f"    📡 Total signals collected: {len(entries_with_paths)}")
        return entries_with_paths

    # --- SUB-FUNCTION 8: EXECUTE SINGLE ORDER ---
    def execute_order(order_data, investor_root, per_order_cache, existing_positions):
        """
        Execute a single order based on its order_type.
        Supports: buy_stop, sell_stop, buy_limit, sell_limit, instant_buy, instant_sell
        """
        symbol = order_data.get('symbol')
        order_type = order_data.get('order_type', '').lower()
        entry_price = float(order_data.get('entry', 0))
        exit_price = float(order_data.get('exit', 0)) if order_data.get('exit') else 0
        target_price = float(order_data.get('target', 0)) if order_data.get('target') else 0
        volume = get_volume_from_signal(order_data)
        magic_number = int(order_data.get('magic', int(investor_root.name) if investor_root.name.isdigit() else 123456))
        strategy_name = order_data.get('strategy_name', 'unknown')
        
        # Get symbol info
        if not mt5.symbol_select(symbol, True):
            return False, None, f"Failed to select symbol {symbol}"
        
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info:
            return False, None, f"Cannot get symbol info for {symbol}"
        
        # Round values to symbol digits
        entry_price = round(entry_price, symbol_info.digits)
        exit_price = round(exit_price, symbol_info.digits) if exit_price else 0
        target_price = round(target_price, symbol_info.digits) if target_price else 0
        volume = max(symbol_info.volume_min, min(symbol_info.volume_max, round(volume, 2)))
        
        # Get MT5 order type constant
        mt5_order_type = get_mt5_order_type(order_type)
        if mt5_order_type is None:
            return False, None, f"Invalid order type: {order_type}"
        
        # Generate cache key
        cache_key = f"{symbol}_{mt5_order_type}_{entry_price}_{volume}"
        
        # Check per-order cache
        if cache_key in per_order_cache:
            print(f"        ⏭️  SKIP - already placed in this run: {order_type} @ {entry_price}")
            return False, None, "Already placed in this run", cache_key
        
        # Check proximity risk
        is_risk, risk_position, risk_amount, risk_threshold = check_proximity_risk(order_data, existing_positions)
        if is_risk:
            print(f"        ⏭️  SKIP - too close to position #{risk_position.ticket} (risk: ${risk_amount:.2f} < ${risk_threshold:.2f})")
            return False, None, f"Too close to position #{risk_position.ticket}", cache_key
        
        # Prepare request based on order type
        if order_type in ['instant_buy', 'instant_sell']:
            # Market order (instant execution)
            tick = mt5.symbol_info_tick(symbol)
            if not tick:
                return False, None, f"Cannot get tick for {symbol}"
            
            price = tick.ask if order_type == 'instant_buy' else tick.bid
            
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": volume,
                "type": mt5_order_type,
                "price": price,
                "deviation": 20,
                "magic": magic_number,
                "comment": f"{strategy_name[:20]} RR{order_data.get('risk_reward', '?')}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            if exit_price:
                request["sl"] = exit_price
            if target_price:
                request["tp"] = target_price
                
        else:
            # Pending order
            request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": volume,
                "type": mt5_order_type,
                "price": entry_price,
                "deviation": 20,
                "magic": magic_number,
                "comment": f"{strategy_name[:20]} RR{order_data.get('risk_reward', '?')}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_RETURN,
            }
            
            if exit_price:
                request["sl"] = exit_price
            if target_price:
                request["tp"] = target_price
        
        # Send order
        result = mt5.order_send(request)
        
        if result is None:
            return False, None, f"Order send failed: {mt5.last_error()}", cache_key
        
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            return False, None, f"Order failed: {result.comment} (code: {result.retcode})", cache_key
        
        # Create detailed trade record
        trade_record = order_data.copy()
        trade_record.update({
            'ticket': result.order,
            'magic': magic_number,
            'placed_timestamp': datetime.now().isoformat(),
            'status': 'pending',
            'mt5_retcode': result.retcode,
            'mt5_comment': result.comment,
            'placed_price': entry_price if order_type not in ['instant_buy', 'instant_sell'] else (request['price'] if 'price' in request else entry_price),
            'placed_volume': volume,
            'placed_order_type': order_type,
            'strategy_name': strategy_name
        })
        
        # Save to history
        sync_and_save_detailed_history(investor_root, trade_record)
        
        print(f"        ✅ SUCCESS: {order_type.upper()} {symbol} @ {entry_price} (Ticket: {result.order}) [Strategy: {strategy_name}]")
        return True, result, None, cache_key

    # --- MAIN EXECUTION FLOW ---
    print("\n" + "="*80)
    print("🚀 STARTING ENHANCED ORDER PLACEMENT ENGINE")
    print("="*80)
    
    investor_ids = [inv_id] if inv_id else list(usersdictionary.keys())
    any_orders_placed = False
    global_stats = {
        'investors_processed': 0,
        'investors_blocked': 0,
        'total_signals_found': 0,
        'total_orders_placed': 0,
        'total_orders_skipped_duplicate': 0,
        'total_orders_skipped_risk': 0,
        'total_orders_failed': 0,
        'total_orders_cancelled_regulation': 0
    }

    for user_brokerid in investor_ids:
        print(f"\n{'='*60}")
        print(f"📋 INVESTOR: {user_brokerid}")
        print(f"{'='*60}")
        
        resolution_cache = {}
        investor_root = Path(INV_PATH) / user_brokerid
        
        if not investor_root.exists():
            print(f"  ❌ Investor root not found: {investor_root}")
            continue
        
        # STEP 1: Check authorization status
        can_proceed, activities = check_authorization_status(investor_root)
        
        if not can_proceed:
            print(f"  🚫 INVESTOR BLOCKED - unauthorized actions detected without bypass")
            global_stats['investors_blocked'] += 1
            continue
        
        # STEP 2: Collect signals from ALL strategy folders
        all_signals = collect_orders_from_signals(investor_root, resolution_cache)
        
        if not all_signals:
            print(f"  ℹ️  No signals found for {user_brokerid}")
            continue
        
        global_stats['investors_processed'] += 1
        global_stats['total_signals_found'] += len(all_signals)
        
        # STEP 3: Sync existing tradeshistory.json first
        print(f"\n  🔄 Syncing tradeshistory.json with MT5...")
        sync_and_save_detailed_history(investor_root)
        
        # STEP 4: Get existing positions for risk check
        existing_positions = mt5.positions_get() or []
        print(f"  📊 Found {len(existing_positions)} existing open positions")
        
        # STEP 5: Per-order cache for this run
        per_order_cache = set()
        
        # STEP 6: Build authorized keys for regulation
        authorized_keys = set()
        
        # STEP 7: Process each signal
        print(f"\n  📝 Processing {len(all_signals)} signals...")
        
        orders_placed = 0
        orders_skipped_duplicate = 0
        orders_skipped_risk = 0
        orders_failed = 0
        
        for signal_wrapper in all_signals:
            order_data = signal_wrapper['data']
            order_type = order_data.get('order_type', '').lower()
            symbol = order_data.get('symbol', '')
            entry = order_data.get('entry', 0)
            volume = get_volume_from_signal(order_data)
            strategy = signal_wrapper.get('strategy', 'unknown')
            
            print(f"\n    🔹 Processing: {order_type} {symbol} @ {entry} (Volume: {volume}) [Strategy: {strategy}]")
            
            # Execute order
            success, result, error, cache_key = execute_order(
                order_data, investor_root, per_order_cache, existing_positions
            )
            
            if cache_key:
                if success:
                    per_order_cache.add(cache_key)
                    authorized_keys.add(cache_key)
                    orders_placed += 1
                elif error == "Already placed in this run":
                    orders_skipped_duplicate += 1
                elif "too close" in str(error).lower():
                    orders_skipped_risk += 1
                else:
                    orders_failed += 1
                    print(f"        ❌ FAILED: {error}")
        
        # STEP 8: Regulate orders (cancel unauthorized)
        print(f"\n  🧹 Running order regulation...")
        cancelled_count = regulate_orders(investor_root, authorized_keys)
        
        # Update global stats
        global_stats['total_orders_placed'] += orders_placed
        global_stats['total_orders_skipped_duplicate'] += orders_skipped_duplicate
        global_stats['total_orders_skipped_risk'] += orders_skipped_risk
        global_stats['total_orders_failed'] += orders_failed
        global_stats['total_orders_cancelled_regulation'] += cancelled_count
        
        # Print investor summary
        print(f"\n  📊 INVESTOR SUMMARY:")
        print(f"     • Signals processed: {len(all_signals)}")
        print(f"     • Orders placed: {orders_placed}")
        print(f"     • Skipped (duplicate): {orders_skipped_duplicate}")
        print(f"     • Skipped (proximity risk): {orders_skipped_risk}")
        print(f"     • Failed: {orders_failed}")
        print(f"     • Cancelled (regulation): {cancelled_count}")
        
        if orders_placed > 0:
            any_orders_placed = True
    
    # Print global summary
    print("\n" + "="*80)
    print("📊 GLOBAL SUMMARY")
    print("="*80)
    print(f"  • Investors processed: {global_stats['investors_processed']}")
    print(f"  • Investors blocked: {global_stats['investors_blocked']}")
    print(f"  • Total signals found: {global_stats['total_signals_found']}")
    print(f"  • Total orders placed: {global_stats['total_orders_placed']}")
    print(f"  • Orders skipped (duplicate): {global_stats['total_orders_skipped_duplicate']}")
    print(f"  • Orders skipped (risk): {global_stats['total_orders_skipped_risk']}")
    print(f"  • Orders failed: {global_stats['total_orders_failed']}")
    print(f"  • Orders cancelled (regulation): {global_stats['total_orders_cancelled_regulation']}")
    print("="*80)
    
    return any_orders_placed
 
def check_pending_orders_risk(inv_id=None):
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
     VERSION: Uses the EXACT account initialization logic from place_usd_orders_for_accounts()
    Only removes orders with risk HIGHER than allowed (lower risk orders are kept).
    
    NOW CHECKS: ALL pending orders (LIMIT, STOP, STOP-LIMIT)
    
    RISK CONFIGURATION LOGIC:
    - If enable_maximum_account_balance_management = true -> use account_balance_maximum_risk_management
    - Else if enable_default_account_balance_management = true -> use account_balance_default_risk_management
    - Else (both false) -> default to account_balance_default_risk_management
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 🛡️ LIVE RISK AUDIT: ALL PENDING ORDERS (LIMIT + STOP)  {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # --- DATA INITIALIZATION ---
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "orders_checked": 0,
        "orders_removed": 0,
        "orders_kept_lower": 0,
        "orders_kept_in_range": 0,
        "risk_config_used": None,
        "processing_success": False
    }
    
    try:
        if not os.path.exists(NORMALIZE_SYMBOLS_PATH):
            print(" [!] CRITICAL ERROR: Normalization map path missing.")
            return stats
        with open(NORMALIZE_SYMBOLS_PATH, 'r') as f:
            norm_map = json.load(f)
    except Exception as e:
        print(f" [!] CRITICAL ERROR: Normalization map load failed: {e}")
        return stats

    # Define MT5 order types for better readability
    ORDER_TYPES = {
        mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT",
        mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
        mt5.ORDER_TYPE_BUY_STOP: "BUY STOP",
        mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
        mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY STOP-LIMIT",
        mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL STOP-LIMIT"
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Auditing live risk limits...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND DETERMINE RISK CONFIGURATION TO USE ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Get settings flags
            settings = config.get("settings", {})
            enable_default = settings.get("enable_default_account_balance_management", False)
            enable_maximum = settings.get("enable_maximum_account_balance_management", False)
            
            print(f"  └─ ⚙️  Risk Configuration Settings:")
            print(f"      • enable_default_account_balance_management: {enable_default}")
            print(f"      • enable_maximum_account_balance_management: {enable_maximum}")
            
            # Determine which risk config to use
            risk_map = None
            risk_config_used = None
            
            # LOGIC: If maximum is enabled, use maximum (even if default is also enabled)
            if enable_maximum:
                risk_map = config.get("account_balance_maximum_risk_management", {})
                risk_config_used = "maximum"
                print(f"      📋 USING: account_balance_maximum_risk_management (maximum enabled)")
            
            # Else if default is enabled (and maximum is false), use default
            elif enable_default:
                risk_map = config.get("account_balance_default_risk_management", {})
                risk_config_used = "default"
                print(f"      📋 USING: account_balance_default_risk_management (default enabled)")
            
            # Else (both false), default to default risk management
            else:
                risk_map = config.get("account_balance_default_risk_management", {})
                risk_config_used = "default (fallback)"
                print(f"      📋 USING: account_balance_default_risk_management (fallback - both flags false)")
            
            if not risk_map:
                print(f"  └─ ⚠️  Selected risk configuration is empty or missing")
                continue
                
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            continue

        # --- ACCOUNT CONNECTION CHECK (NO INIT/SHUTDOWN) ---
        print(f"  └─ 🔌 Checking account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Check if already logged into correct account
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"  └─ ❌ Not logged into the correct account. Expected: {login_id}, Found: {acc.login if acc else 'None'}")
            continue
        else:
            print(f"      ✅ Connected to account: {acc.login}")

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            continue
            
        balance = acc_info.balance

        # Get terminal info for additional details
        term_info = mt5.terminal_info()
        
        print(f"\n  └─ 📊 Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

        # Determine Primary Risk Value based on selected risk map
        primary_risk = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = float(r_val)
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f} in selected config")
            continue

        print(f"\n  └─ 💰 Target Risk (from {risk_config_used} config): ${primary_risk:.2f}")
        
        # Store which config was used in stats
        stats["risk_config_used"] = risk_config_used

        # Check ALL Live Pending Orders (LIMIT, STOP, STOP-LIMIT)
        pending_orders = mt5.orders_get()
        investor_orders_checked = 0
        investor_orders_removed = 0
        investor_orders_kept_lower = 0
        investor_orders_kept_in_range = 0

        if pending_orders:
            print(f"  └─ 🔍 Scanning {len(pending_orders)} pending orders (ALL types)...")
            
            for order in pending_orders:
                # Skip if not a pending order type
                if order.type not in ORDER_TYPES.keys():
                    continue

                investor_orders_checked += 1
                stats["orders_checked"] += 1
                
                order_type_name = ORDER_TYPES.get(order.type, f"Unknown Type {order.type}")
                
                # Determine order direction for calculations
                is_buy = order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT]
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                
                # Calculate risk (stop loss distance in money)
                if order.sl == 0:
                    print(f"    └─ ⚠️  Order #{order.ticket} | {order_type_name} | {order.symbol} - No SL set, skipping risk check")
                    continue
                
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                  order.price_open, order.sl)
                
                if sl_profit is not None:
                    order_risk_usd = round(abs(sl_profit), 2)
                    
                    # Use a percentage-based threshold instead of absolute dollar difference
                    # For small balances, absolute differences can be misleading
                    risk_difference = order_risk_usd - primary_risk
                    
                    # For very small balances (like $2), a difference of $0.50 is significant
                    # Use a relative threshold: 20% of primary risk or $0.50, whichever is smaller
                    relative_threshold = max(0.50, primary_risk * 0.2)
                    
                    print(f"    └─ 📋 Order #{order.ticket} | {order_type_name} | {order.symbol}")
                    print(f"       Risk: ${order_risk_usd:.2f} | Target Risk: ${primary_risk:.2f}")
                    
                    # Only remove if risk is significantly higher than allowed
                    if risk_difference > relative_threshold: 
                        print(f"       🗑️ PURGING: Risk too high")
                        print(f"       Risk: ${order_risk_usd:.2f} > Allowed: ${primary_risk:.2f} (Δ: ${risk_difference:.2f})")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        result = mt5.order_send(cancel_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_orders_removed += 1
                            stats["orders_removed"] += 1
                            print(f"       ✅ Order removed successfully")
                        else:
                            error_msg = result.comment if result else "No response"
                            print(f"       ❌ Cancel failed: {error_msg}")
                    
                    elif order_risk_usd < primary_risk - relative_threshold:
                        # Lower risk - keep it (good for the account)
                        investor_orders_kept_lower += 1
                        stats["orders_kept_lower"] += 1
                        print(f"       ✅ KEEPING: Lower risk than allowed")
                        print(f"       Risk: ${order_risk_usd:.2f} < Allowed: ${primary_risk:.2f} (Δ: ${primary_risk - order_risk_usd:.2f})")
                    
                    else:
                        # Within tolerance - keep it
                        investor_orders_kept_in_range += 1
                        stats["orders_kept_in_range"] += 1
                        print(f"       ✅ KEEPING: Risk within tolerance")
                        print(f"       Risk: ${order_risk_usd:.2f} vs Allowed: ${primary_risk:.2f} (Δ: ${abs(risk_difference):.2f})")
                else:
                    print(f"    └─ ⚠️  Order #{order.ticket} - Could not calculate risk")

        # Investor final summary
        if investor_orders_checked > 0:
            print(f"\n  └─ 📊 Audit Results for {user_brokerid}:")
            print(f"       • Risk config used: {risk_config_used}")
            print(f"       • Orders checked: {investor_orders_checked}")
            if investor_orders_kept_lower > 0:
                print(f"       • Kept (lower risk): {investor_orders_kept_lower}")
            if investor_orders_kept_in_range > 0:
                print(f"       • Kept (in tolerance): {investor_orders_kept_in_range}")
            if investor_orders_removed > 0:
                print(f"       • Removed (too high): {investor_orders_removed}")
            else:
                print(f"       ✅ No orders needed removal")
            stats["processing_success"] = True
        else:
            print(f"  └─ 🔘 No pending orders found.")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 RISK AUDIT SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Risk config used: {stats['risk_config_used']}")
    print(f"   Orders checked: {stats['orders_checked']}")
    print(f"   Orders removed: {stats['orders_removed']}")
    print(f"   Orders kept (lower risk): {stats['orders_kept_lower']}")
    print(f"   Orders kept (in tolerance): {stats['orders_kept_in_range']}")
    
    if stats['orders_checked'] > 0:
        removal_rate = (stats['orders_removed'] / stats['orders_checked']) * 100
        print(f"   Removal rate: {removal_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 RISK AUDIT COMPLETE {'='*10}\n")
    return stats

def orders_risk_correction_old(inv_id=None):
    """
    Function: Checks both live pending orders AND open positions (LIMIT, STOP, and MARKET)
    and adjusts their take profit levels based on the selected risk-reward ratio from
    accountmanagement.json. Only executes if risk_reward_correction setting is True.
    
     VERSION: Uses the EXACT account initialization logic from place_usd_orders_for_accounts()
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 📐 RISK-REWARD CORRECTION: ALL POSITIONS & PENDING ORDERS (MARKET + LIMIT + STOP)  {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "orders_checked": 0,
        "orders_adjusted": 0,
        "orders_skipped": 0,
        "orders_error": 0,
        "positions_checked": 0,
        "positions_adjusted": 0,
        "processing_success": False
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking risk-reward configurations...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check if risk_reward_correction is enabled
            settings = config.get("settings", {})
            if not settings.get("risk_reward_correction", False):
                print(f"  └─ ⏭️  Risk-reward correction disabled in settings. Skipping.")
                continue
            
            # Get selected risk-reward ratios
            selected_rr = config.get("selected_risk_reward", [2])
            if not selected_rr:
                print(f"  └─ ⚠️  No risk-reward ratios selected. Using default [2]")
                selected_rr = [2]
            
            # Use the first ratio in the list (typically the preferred one)
            target_rr_ratio = float(selected_rr[0])
            print(f"  └─ ✅ Target R:R Ratio: 1:{target_rr_ratio}")
            
            # Get risk management mapping for balance-based risk
            risk_map = config.get("account_balance_default_risk_management", {})
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            stats["orders_error"] += 1
            continue

        # --- ACCOUNT INITIALIZATION (EXACT COPY FROM place_usd_orders_for_accounts) ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌  login failed: {error}")
                stats["orders_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")
        # --- END EXACT INITIALIZATION COPY ---

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            stats["orders_error"] += 1
            continue
            
        balance = acc_info.balance

        # Get terminal info for additional details
        term_info = mt5.terminal_info()
        
        print(f"\n  └─ 📊 Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

        # --- DETERMINE PRIMARY RISK VALUE BASED ON BALANCE ---
        primary_risk = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = float(r_val)
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            stats["orders_skipped"] += 1
            continue

        print(f"\n  └─ 💰 Balance: ${balance:,.2f} | Base Risk: ${primary_risk:.2f} | Target R:R: 1:{target_rr_ratio}")

        # --- CHECK AND ADJUST ALL POSITIONS (OPEN MARKET ORDERS) ---
        positions = mt5.positions_get()
        investor_positions_checked = 0
        investor_positions_adjusted = 0
        investor_positions_skipped = 0
        investor_positions_error = 0

        # Define MT5 position/order types for better readability
        POSITION_TYPES = {
            mt5.POSITION_TYPE_BUY: "BUY (MARKET)",
            mt5.POSITION_TYPE_SELL: "SELL (MARKET)"
        }
        
        ORDER_TYPES = {
            mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT",
            mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
            mt5.ORDER_TYPE_BUY_STOP: "BUY STOP",
            mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
            mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY STOP-LIMIT",
            mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL STOP-LIMIT"
        }

        # Process OPEN POSITIONS first
        if positions:
            print(f"\n  └─ 🔍 Scanning {len(positions)} open positions (MARKET)...")
            
            for position in positions:
                investor_positions_checked += 1
                stats["positions_checked"] += 1
                
                position_type_name = POSITION_TYPES.get(position.type, f"Unknown Type {position.type}")
                
                # Get symbol info
                symbol_info = mt5.symbol_info(position.symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {position.symbol}")
                    investor_positions_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                # Determine position direction
                is_buy = position.type == mt5.POSITION_TYPE_BUY
                
                print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                
                # Calculate current risk (stop loss distance in money)
                if position.sl == 0:
                    print(f"       ⚠️  No SL set - cannot calculate risk. Skipping TP adjustment.")
                    investor_positions_skipped += 1
                    stats["orders_skipped"] += 1
                    continue
                
                # For positions, risk is from current price to SL (or entry to SL if price moved favorably)
                # We use the more conservative approach: risk based on original entry to SL
                # This ensures we don't reduce TP if price has moved in our favor
                if is_buy:
                    # For BUY: entry price is position.price_open
                    risk_price = position.price_open - position.sl
                else:
                    # For SELL: entry price is position.price_open
                    risk_price = position.sl - position.price_open
                
                # Calculate risk in money
                risk_points = abs(risk_price) / symbol_info.point
                point_value = symbol_info.trade_tick_value / symbol_info.trade_tick_size * symbol_info.point
                current_risk_usd = round(risk_points * point_value * position.volume, 2)
                
                # Alternative: calculate using MT5 profit calculator for accuracy
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                sl_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                  position.price_open, position.sl)
                
                if sl_profit is not None:
                    current_risk_usd = round(abs(sl_profit), 2)
                
                # Calculate required take profit based on risk and target R:R ratio
                target_profit_usd = current_risk_usd * target_rr_ratio
                
                print(f"       Risk (from entry): ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f}")
                
                # Calculate the take profit price that would achieve this profit
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    # Calculate how many ticks we need to move to achieve target profit
                    ticks_needed = target_profit_usd / (position.volume * tick_value)
                    
                    # Convert ticks to price movement
                    price_move_needed = ticks_needed * tick_size
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    price_move_needed = round(price_move_needed, digits)
                    
                    # Calculate new take profit price based on position type (from entry price, not current price)
                    if is_buy:
                        # For BUY positions: TP above entry price
                        new_tp = round(position.price_open + price_move_needed, digits)
                    else:
                        # For SELL positions: TP below entry price
                        new_tp = round(position.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    if position.tp == 0:
                        target_move = abs(new_tp - position.price_open)
                        print(f"       📝 No TP currently set")
                        print(f"       Target TP: {new_tp:.{digits}f} (Move from entry: {target_move:.{digits}f})")
                        should_adjust = True
                    else:
                        current_move = abs(position.tp - position.price_open)
                        target_move = abs(new_tp - position.price_open)
                        
                        # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                        pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                        
                        if abs(current_move - target_move) > pip_threshold:
                            print(f"       📐 TP needs adjustment")
                            print(f"       Current TP: {position.tp:.{digits}f} (Move from entry: {current_move:.{digits}f})")
                            print(f"       Target TP:  {new_tp:.{digits}f} (Move from entry: {target_move:.{digits}f})")
                            should_adjust = True
                        else:
                            print(f"       ✅ TP already correct")
                            print(f"       TP: {position.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                            investor_positions_skipped += 1
                            stats["orders_skipped"] += 1
                            continue
                    
                    if should_adjust:
                        # Prepare modification request for position
                        modify_request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": position.ticket,
                            "sl": position.sl,
                            "tp": new_tp,
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_positions_adjusted += 1
                            stats["positions_adjusted"] += 1
                            stats["orders_adjusted"] += 1
                            print(f"       ✅ TP adjusted successfully to {new_tp:.{digits}f}")
                        else:
                            investor_positions_error += 1
                            stats["orders_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid tick values - using fallback calculation")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        test_move = symbol_info.point * 10
                        if is_buy:
                            test_price = position.price_open + test_move
                        else:
                            test_price = position.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                            position.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            point_value = abs(test_profit) / 10
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy:
                                new_tp = round(position.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(position.price_open - price_move_needed, digits)
                            
                            print(f"       Using fallback calculation")
                            
                            if position.tp == 0 or abs(position.tp - new_tp) > symbol_info.point * 20:
                                modify_request = {
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "position": position.ticket,
                                    "sl": position.sl,
                                    "tp": new_tp,
                                }
                                
                                result = mt5.order_send(modify_request)
                                
                                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                    investor_positions_adjusted += 1
                                    stats["positions_adjusted"] += 1
                                    stats["orders_adjusted"] += 1
                                    print(f"       ✅ TP adjusted using fallback method to {new_tp:.{digits}f}")
                                else:
                                    investor_positions_error += 1
                                    stats["orders_error"] += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                investor_positions_skipped += 1
                                stats["orders_skipped"] += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            investor_positions_skipped += 1
                            stats["orders_skipped"] += 1
                            print(f"       ⚠️  Cannot calculate using fallback method")
                    except Exception as e:
                        investor_positions_error += 1
                        stats["orders_error"] += 1
                        print(f"       ❌ Fallback calculation error: {e}")
        
        # --- CHECK AND ADJUST ALL PENDING ORDERS (LIMIT AND STOP) ---
        pending_orders = mt5.orders_get()
        investor_orders_checked = 0
        investor_orders_adjusted = 0
        investor_orders_skipped = 0
        investor_orders_error = 0

        if pending_orders:
            print(f"\n  └─ 🔍 Scanning {len(pending_orders)} pending orders (LIMIT & STOP)...")
            
            for order in pending_orders:
                # Skip if not a pending order (only process pending order types)
                if order.type not in ORDER_TYPES.keys():
                    continue

                investor_orders_checked += 1
                stats["orders_checked"] += 1
                
                order_type_name = ORDER_TYPES.get(order.type, f"Unknown Type {order.type}")
                
                # Get symbol info
                symbol_info = mt5.symbol_info(order.symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {order.symbol}")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                # Determine order direction for calculations
                is_buy = order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT]
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                
                print(f"\n    └─ 📋 Order #{order.ticket} | {order_type_name} | {order.symbol}")
                
                # Calculate current risk (stop loss distance in money)
                if order.sl == 0:
                    print(f"       ⚠️  No SL set - cannot calculate risk. Skipping TP adjustment.")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue
                    
                # For pending orders, risk is from entry to SL
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                  order.price_open, order.sl)
                
                if sl_profit is None:
                    print(f"       ⚠️  Cannot calculate risk. Skipping.")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                current_risk_usd = round(abs(sl_profit), 2)
                
                # Calculate required take profit based on risk and target R:R ratio
                target_profit_usd = current_risk_usd * target_rr_ratio
                
                print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f}")
                
                # Calculate the take profit price that would achieve this profit
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    # Calculate how many ticks we need to move to achieve target profit
                    ticks_needed = target_profit_usd / (order.volume_initial * tick_value)
                    
                    # Convert ticks to price movement
                    price_move_needed = ticks_needed * tick_size
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    price_move_needed = round(price_move_needed, digits)
                    
                    # Calculate new take profit price based on order type
                    if is_buy:
                        # For BUY orders: TP above entry
                        new_tp = round(order.price_open + price_move_needed, digits)
                    else:
                        # For SELL orders: TP below entry
                        new_tp = round(order.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    current_move = abs(order.tp - order.price_open) if order.tp != 0 else 0
                    target_move = abs(new_tp - order.price_open)
                    
                    # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                    pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                    
                    should_adjust = False
                    
                    if order.tp == 0:
                        print(f"       📝 No TP currently set")
                        print(f"       Target TP: {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        should_adjust = True
                    elif abs(current_move - target_move) > pip_threshold:
                        print(f"       📐 TP needs adjustment")
                        print(f"       Current TP: {order.tp:.{digits}f} (Move: {current_move:.{digits}f})")
                        print(f"       Target TP:  {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        should_adjust = True
                    else:
                        print(f"       ✅ TP already correct")
                        print(f"       TP: {order.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                        investor_orders_skipped += 1
                        stats["orders_skipped"] += 1
                        continue
                    
                    if should_adjust:
                        # Prepare modification request
                        modify_request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": order.ticket,
                            "price": order.price_open,
                            "sl": order.sl,
                            "tp": new_tp,
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_orders_adjusted += 1
                            stats["orders_adjusted"] += 1
                            print(f"       ✅ TP adjusted successfully to {new_tp:.{digits}f}")
                        else:
                            investor_orders_error += 1
                            stats["orders_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid tick values - using fallback calculation")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        test_move = symbol_info.point * 10
                        if is_buy:
                            test_price = order.price_open + test_move
                        else:
                            test_price = order.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                            order.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            point_value = abs(test_profit) / 10
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy:
                                new_tp = round(order.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(order.price_open - price_move_needed, digits)
                            
                            print(f"       Using fallback calculation")
                            
                            if order.tp == 0 or abs(order.tp - new_tp) > symbol_info.point * 20:
                                modify_request = {
                                    "action": mt5.TRADE_ACTION_MODIFY,
                                    "order": order.ticket,
                                    "price": order.price_open,
                                    "sl": order.sl,
                                    "tp": new_tp,
                                }
                                
                                result = mt5.order_send(modify_request)
                                
                                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                    investor_orders_adjusted += 1
                                    stats["orders_adjusted"] += 1
                                    print(f"       ✅ TP adjusted using fallback method to {new_tp:.{digits}f}")
                                else:
                                    investor_orders_error += 1
                                    stats["orders_error"] += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                investor_orders_skipped += 1
                                stats["orders_skipped"] += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            investor_orders_skipped += 1
                            stats["orders_skipped"] += 1
                            print(f"       ⚠️  Cannot calculate using fallback method")
                    except Exception as e:
                        investor_orders_error += 1
                        stats["orders_error"] += 1
                        print(f"       ❌ Fallback calculation error: {e}")

        # --- INVESTOR SUMMARY ---
        total_checked = investor_positions_checked + investor_orders_checked
        total_adjusted = investor_positions_adjusted + investor_orders_adjusted
        
        if total_checked > 0:
            print(f"\n  └─ 📊 Risk-Reward Correction Results for {user_brokerid}:")
            if investor_positions_checked > 0:
                print(f"       • Positions checked: {investor_positions_checked}")
                print(f"       • Positions adjusted: {investor_positions_adjusted}")
                print(f"       • Positions skipped: {investor_positions_skipped}")
            if investor_orders_checked > 0:
                print(f"       • Pending orders checked: {investor_orders_checked}")
                print(f"       • Pending orders adjusted: {investor_orders_adjusted}")
                print(f"       • Pending orders skipped: {investor_orders_skipped}")
            if investor_positions_error + investor_orders_error > 0:
                print(f"       • Errors: {investor_positions_error + investor_orders_error}")
            else:
                print(f"       ✅ All adjustments completed successfully")
            stats["processing_success"] = True
        else:
            print(f"  └─ 🔘 No positions or pending orders found.")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 RISK-REWARD CORRECTION SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Positions checked: {stats['positions_checked']}")
    print(f"   Positions adjusted: {stats['positions_adjusted']}")
    print(f"   Pending orders checked: {stats['orders_checked']}")
    print(f"   Pending orders adjusted: {stats['orders_adjusted']}")
    print(f"   Total checked: {stats['positions_checked'] + stats['orders_checked']}")
    print(f"   Total adjusted: {stats['positions_adjusted'] + stats['orders_adjusted']}")
    print(f"   Orders skipped: {stats['orders_skipped']}")
    print(f"   Errors: {stats['orders_error']}")
    
    total_checked = stats['positions_checked'] + stats['orders_checked']
    total_adjusted = stats['positions_adjusted'] + stats['orders_adjusted']
    if total_checked > 0:
        success_rate = (total_adjusted / total_checked) * 100
        print(f"   Adjustment success rate: {success_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 POSITIONS & PENDING ORDERS RISK-REWARD CORRECTION COMPLETE {'='*10}\n")
    return stats

def orders_risk_correction(inv_id=None):
    """
    Function: Checks both live pending orders AND open positions (LIMIT, STOP, and MARKET)
    and adjusts their take profit levels based on the NEAREST MATCHING strategy risk-reward ratio.
    
    INTELLIGENT APPROACH:
    1. Calculate current R:R from order's exit/target prices
    2. Compare with strategy-specific R:R values from accountmanagement.json
    3. Find the nearest matching R:R (next higher value) and use that
    4. Fall back to default selected_risk_reward if no match found
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 📐 INTELLIGENT R:R CORRECTION: FINDING NEAREST STRATEGY MATCH {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "orders_checked": 0,
        "orders_adjusted": 0,
        "orders_skipped": 0,
        "orders_error": 0,
        "positions_checked": 0,
        "positions_adjusted": 0,
        "rr_matches": {},  # Track which R:R ratios were used
        "rr_mismatches": 0,  # Track orders that didn't match any strategy
        "processing_success": False
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Loading R:R configurations...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND EXTRACT ALL AVAILABLE R:R VALUES ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check if risk_reward_correction is enabled
            settings = config.get("settings", {})
            if not settings.get("risk_reward_correction", False):
                print(f"  └─ ⏭️  Risk-reward correction disabled in settings. Skipping.")
                continue
            
            # Get ALL available R:R values (both default and strategy-specific)
            all_rr_values = []
            
            # Add default selected_risk_reward
            selected_rr = config.get("selected_risk_reward", [2])
            if isinstance(selected_rr, list) and selected_rr:
                default_rr = float(selected_rr[0])
                all_rr_values.append(default_rr)
            else:
                default_rr = 2.0
                all_rr_values.append(default_rr)
            
            # Add all strategy-specific R:R values
            strategies_rr = config.get("strategies_risk_reward", {})
            strategy_rr_values = []
            for strategy, rr_value in strategies_rr.items():
                try:
                    rr_float = float(rr_value)
                    strategy_rr_values.append(rr_float)
                    all_rr_values.append(rr_float)
                except (ValueError, TypeError):
                    continue
            
            # Sort and deduplicate all available R:R values
            all_rr_values = sorted(set(all_rr_values))
            
            print(f"  └─ 📊 Default R:R: 1:{default_rr}")
            if strategy_rr_values:
                print(f"  └─ 📋 Strategy R:R values: {', '.join([f'1:{v}' for v in sorted(set(strategy_rr_values))])}")
            print(f"  └─ 🎯 All available R:R targets: {', '.join([f'1:{v}' for v in all_rr_values])}")
            
            # Get risk management mapping for balance-based risk
            risk_map = config.get("account_balance_default_risk_management", {})
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            stats["orders_error"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌  login failed: {error}")
                stats["orders_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            stats["orders_error"] += 1
            continue
            
        balance = acc_info.balance

        # Get terminal info for additional details
        term_info = mt5.terminal_info()
        
        print(f"\n  └─ 📊 Account Details:")
        print(f"      • Balance: ${acc_info.balance:,.2f}")
        print(f"      • Equity: ${acc_info.equity:,.2f}")
        print(f"      • Free Margin: ${acc_info.margin_free:,.2f}")
        print(f"      • Margin Level: {acc_info.margin_level:.2f}%" if acc_info.margin_level else "      • Margin Level: N/A")
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else '❌ DISABLED'}")

        # --- DETERMINE PRIMARY RISK VALUE BASED ON BALANCE ---
        primary_risk = None
        for range_str, r_val in risk_map.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    primary_risk = float(r_val)
                    break
            except Exception as e:
                print(f"  └─ ⚠️  Error parsing range '{range_str}': {e}")
                continue

        if primary_risk is None:
            print(f"  └─ ⚠️  No risk mapping for balance ${balance:,.2f}")
            stats["orders_skipped"] += 1
            continue

        print(f"\n  └─ 💰 Balance: ${balance:,.2f} | Base Risk: ${primary_risk:.2f}")

        # --- HELPER FUNCTION: Find nearest matching R:R ---
        def find_nearest_rr(current_rr, available_rr_values):
            """
            Find the nearest matching R:R value from available options.
            Prefers next higher value, but if none exists, uses the closest.
            """
            if not available_rr_values:
                return None, "none"
            
            # Sort available values
            sorted_values = sorted(available_rr_values)
            
            # Find the next higher value (preferred)
            next_higher = None
            for val in sorted_values:
                if val >= current_rr:
                    next_higher = val
                    break
            
            if next_higher is not None:
                return next_higher, "next_higher"
            
            # If no higher value, use the closest (should be the maximum)
            closest = min(sorted_values, key=lambda x: abs(x - current_rr))
            return closest, "closest"

        # --- CHECK AND ADJUST ALL POSITIONS (OPEN MARKET ORDERS) ---
        positions = mt5.positions_get()
        investor_positions_checked = 0
        investor_positions_adjusted = 0
        investor_positions_skipped = 0
        investor_positions_error = 0

        # Define MT5 position/order types for better readability
        POSITION_TYPES = {
            mt5.POSITION_TYPE_BUY: "BUY (MARKET)",
            mt5.POSITION_TYPE_SELL: "SELL (MARKET)"
        }
        
        ORDER_TYPES = {
            mt5.ORDER_TYPE_BUY_LIMIT: "BUY LIMIT",
            mt5.ORDER_TYPE_SELL_LIMIT: "SELL LIMIT",
            mt5.ORDER_TYPE_BUY_STOP: "BUY STOP",
            mt5.ORDER_TYPE_SELL_STOP: "SELL STOP",
            mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY STOP-LIMIT",
            mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL STOP-LIMIT"
        }

        # Process OPEN POSITIONS first
        if positions:
            print(f"\n  └─ 🔍 Scanning {len(positions)} open positions (MARKET)...")
            
            for position in positions:
                investor_positions_checked += 1
                stats["positions_checked"] += 1
                
                position_type_name = POSITION_TYPES.get(position.type, f"Unknown Type {position.type}")
                
                # Get symbol info
                symbol_info = mt5.symbol_info(position.symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {position.symbol}")
                    investor_positions_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                # Determine position direction
                is_buy = position.type == mt5.POSITION_TYPE_BUY
                
                print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                
                # Calculate current risk (stop loss distance in money)
                if position.sl == 0:
                    print(f"       ⚠️  No SL set - cannot calculate risk. Skipping TP adjustment.")
                    investor_positions_skipped += 1
                    stats["orders_skipped"] += 1
                    continue
                
                # For positions, risk is from entry to SL
                if is_buy:
                    risk_distance = position.price_open - position.sl
                else:
                    risk_distance = position.sl - position.price_open
                
                # Calculate risk in money using MT5 profit calculator for accuracy
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                sl_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                  position.price_open, position.sl)
                
                if sl_profit is not None:
                    current_risk_usd = round(abs(sl_profit), 2)
                else:
                    # Fallback calculation
                    risk_points = abs(risk_distance) / symbol_info.point
                    point_value = symbol_info.trade_tick_value / symbol_info.trade_tick_size * symbol_info.point
                    current_risk_usd = round(risk_points * point_value * position.volume, 2)
                
                # Calculate current R:R if TP exists
                current_rr = None
                if position.tp != 0:
                    if is_buy:
                        tp_distance = position.tp - position.price_open
                    else:
                        tp_distance = position.price_open - position.tp
                    
                    if risk_distance > 0:
                        current_rr = round(tp_distance / risk_distance, 2)
                        print(f"       Current R:R: 1:{current_rr}")
                    else:
                        current_rr = None
                
                # Find target R:R based on current value
                if current_rr is not None:
                    target_rr, match_type = find_nearest_rr(current_rr, all_rr_values)
                    
                    if match_type == "next_higher":
                        print(f"       🔍 Using next higher R:R: 1:{target_rr} (from 1:{current_rr})")
                    elif match_type == "closest":
                        print(f"       🔍 Using closest R:R: 1:{target_rr} (from 1:{current_rr}) - no higher value")
                    else:
                        target_rr = default_rr
                        print(f"       ℹ️  Using default R:R: 1:{target_rr}")
                    
                    # Track R:R usage
                    rr_key = str(target_rr)
                    if rr_key not in stats["rr_matches"]:
                        stats["rr_matches"][rr_key] = 0
                    stats["rr_matches"][rr_key] += 1
                else:
                    # If no current R:R, use default
                    target_rr = default_rr
                    print(f"       ℹ️  No current R:R found, using default: 1:{target_rr}")
                    stats["rr_mismatches"] += 1
                
                # Calculate required take profit based on risk and target R:R ratio
                target_profit_usd = current_risk_usd * target_rr
                
                print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f} (1:{target_rr})")
                
                # Calculate the take profit price that would achieve this profit
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    # Calculate how many ticks we need to move to achieve target profit
                    ticks_needed = target_profit_usd / (position.volume * tick_value)
                    
                    # Convert ticks to price movement
                    price_move_needed = ticks_needed * tick_size
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    price_move_needed = round(price_move_needed, digits)
                    
                    # Calculate new take profit price based on position type (from entry price)
                    if is_buy:
                        new_tp = round(position.price_open + price_move_needed, digits)
                    else:
                        new_tp = round(position.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    if position.tp == 0:
                        target_move = abs(new_tp - position.price_open)
                        print(f"       📝 No TP currently set")
                        print(f"       Target TP: {new_tp:.{digits}f} (Move from entry: {target_move:.{digits}f})")
                        should_adjust = True
                    else:
                        current_move = abs(position.tp - position.price_open)
                        target_move = abs(new_tp - position.price_open)
                        
                        # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                        pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                        
                        if abs(current_move - target_move) > pip_threshold:
                            print(f"       📐 TP needs adjustment")
                            print(f"       Current TP: {position.tp:.{digits}f} (Move from entry: {current_move:.{digits}f})")
                            print(f"       Target TP:  {new_tp:.{digits}f} (Move from entry: {target_move:.{digits}f})")
                            should_adjust = True
                        else:
                            print(f"       ✅ TP already correct")
                            print(f"       TP: {position.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                            investor_positions_skipped += 1
                            stats["orders_skipped"] += 1
                            continue
                    
                    if should_adjust:
                        # Prepare modification request for position
                        modify_request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": position.ticket,
                            "sl": position.sl,
                            "tp": new_tp,
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_positions_adjusted += 1
                            stats["positions_adjusted"] += 1
                            stats["orders_adjusted"] += 1
                            print(f"       ✅ TP adjusted successfully to {new_tp:.{digits}f} (Target R:R: 1:{target_rr})")
                        else:
                            investor_positions_error += 1
                            stats["orders_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid tick values - using fallback calculation")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        test_move = symbol_info.point * 10
                        if is_buy:
                            test_price = position.price_open + test_move
                        else:
                            test_price = position.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                            position.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            point_value = abs(test_profit) / 10
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy:
                                new_tp = round(position.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(position.price_open - price_move_needed, digits)
                            
                            print(f"       Using fallback calculation")
                            
                            if position.tp == 0 or abs(position.tp - new_tp) > symbol_info.point * 20:
                                modify_request = {
                                    "action": mt5.TRADE_ACTION_SLTP,
                                    "position": position.ticket,
                                    "sl": position.sl,
                                    "tp": new_tp,
                                }
                                
                                result = mt5.order_send(modify_request)
                                
                                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                    investor_positions_adjusted += 1
                                    stats["positions_adjusted"] += 1
                                    stats["orders_adjusted"] += 1
                                    print(f"       ✅ TP adjusted using fallback method to {new_tp:.{digits}f}")
                                else:
                                    investor_positions_error += 1
                                    stats["orders_error"] += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                investor_positions_skipped += 1
                                stats["orders_skipped"] += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            investor_positions_skipped += 1
                            stats["orders_skipped"] += 1
                            print(f"       ⚠️  Cannot calculate using fallback method")
                    except Exception as e:
                        investor_positions_error += 1
                        stats["orders_error"] += 1
                        print(f"       ❌ Fallback calculation error: {e}")
        
        # --- CHECK AND ADJUST ALL PENDING ORDERS (LIMIT AND STOP) ---
        pending_orders = mt5.orders_get()
        investor_orders_checked = 0
        investor_orders_adjusted = 0
        investor_orders_skipped = 0
        investor_orders_error = 0

        if pending_orders:
            print(f"\n  └─ 🔍 Scanning {len(pending_orders)} pending orders (LIMIT & STOP)...")
            
            for order in pending_orders:
                # Skip if not a pending order (only process pending order types)
                if order.type not in ORDER_TYPES.keys():
                    continue

                investor_orders_checked += 1
                stats["orders_checked"] += 1
                
                order_type_name = ORDER_TYPES.get(order.type, f"Unknown Type {order.type}")
                
                # Get symbol info
                symbol_info = mt5.symbol_info(order.symbol)
                if not symbol_info:
                    print(f"    └─ ⚠️  Cannot get symbol info for {order.symbol}")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                # Determine order direction for calculations
                is_buy = order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP, mt5.ORDER_TYPE_BUY_STOP_LIMIT]
                calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                
                print(f"\n    └─ 📋 Order #{order.ticket} | {order_type_name} | {order.symbol}")
                
                # Calculate current risk (stop loss distance in money)
                if order.sl == 0:
                    print(f"       ⚠️  No SL set - cannot calculate risk. Skipping TP adjustment.")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue
                    
                # For pending orders, risk is from entry to SL
                sl_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                  order.price_open, order.sl)
                
                if sl_profit is None:
                    print(f"       ⚠️  Cannot calculate risk. Skipping.")
                    investor_orders_skipped += 1
                    stats["orders_skipped"] += 1
                    continue

                current_risk_usd = round(abs(sl_profit), 2)
                
                # Calculate current R:R if TP exists
                current_rr = None
                if order.tp != 0:
                    if is_buy:
                        tp_distance = order.tp - order.price_open
                    else:
                        tp_distance = order.price_open - order.tp
                    
                    risk_distance = abs(order.sl - order.price_open)
                    if risk_distance > 0:
                        current_rr = round(tp_distance / risk_distance, 2)
                        print(f"       Current R:R: 1:{current_rr}")
                    else:
                        current_rr = None
                
                # Find target R:R based on current value
                if current_rr is not None:
                    target_rr, match_type = find_nearest_rr(current_rr, all_rr_values)
                    
                    if match_type == "next_higher":
                        print(f"       🔍 Using next higher R:R: 1:{target_rr} (from 1:{current_rr})")
                    elif match_type == "closest":
                        print(f"       🔍 Using closest R:R: 1:{target_rr} (from 1:{current_rr}) - no higher value")
                    else:
                        target_rr = default_rr
                        print(f"       ℹ️  Using default R:R: 1:{target_rr}")
                    
                    # Track R:R usage
                    rr_key = str(target_rr)
                    if rr_key not in stats["rr_matches"]:
                        stats["rr_matches"][rr_key] = 0
                    stats["rr_matches"][rr_key] += 1
                else:
                    # If no current R:R, use default
                    target_rr = default_rr
                    print(f"       ℹ️  No current R:R found, using default: 1:{target_rr}")
                    stats["rr_mismatches"] += 1
                
                # Calculate required take profit based on risk and target R:R ratio
                target_profit_usd = current_risk_usd * target_rr
                
                print(f"       Risk: ${current_risk_usd:.2f} | Target Profit: ${target_profit_usd:.2f} (1:{target_rr})")
                
                # Calculate the take profit price that would achieve this profit
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    # Calculate how many ticks we need to move to achieve target profit
                    ticks_needed = target_profit_usd / (order.volume_initial * tick_value)
                    
                    # Convert ticks to price movement
                    price_move_needed = ticks_needed * tick_size
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    price_move_needed = round(price_move_needed, digits)
                    
                    # Calculate new take profit price based on order type
                    if is_buy:
                        new_tp = round(order.price_open + price_move_needed, digits)
                    else:
                        new_tp = round(order.price_open - price_move_needed, digits)
                    
                    # Check if current TP is significantly different from calculated TP
                    current_move = abs(order.tp - order.price_open) if order.tp != 0 else 0
                    target_move = abs(new_tp - order.price_open)
                    
                    # Calculate threshold (10% of target move or 2 pips, whichever is larger)
                    pip_threshold = max(target_move * 0.1, symbol_info.point * 20)
                    
                    should_adjust = False
                    
                    if order.tp == 0:
                        print(f"       📝 No TP currently set")
                        print(f"       Target TP: {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        should_adjust = True
                    elif abs(current_move - target_move) > pip_threshold:
                        print(f"       📐 TP needs adjustment")
                        print(f"       Current TP: {order.tp:.{digits}f} (Move: {current_move:.{digits}f})")
                        print(f"       Target TP:  {new_tp:.{digits}f} (Move: {target_move:.{digits}f})")
                        should_adjust = True
                    else:
                        print(f"       ✅ TP already correct")
                        print(f"       TP: {order.tp:.{digits}f} | Risk: ${current_risk_usd:.2f}")
                        investor_orders_skipped += 1
                        stats["orders_skipped"] += 1
                        continue
                    
                    if should_adjust:
                        # Prepare modification request
                        modify_request = {
                            "action": mt5.TRADE_ACTION_MODIFY,
                            "order": order.ticket,
                            "price": order.price_open,
                            "sl": order.sl,
                            "tp": new_tp,
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_orders_adjusted += 1
                            stats["orders_adjusted"] += 1
                            print(f"       ✅ TP adjusted successfully to {new_tp:.{digits}f} (Target R:R: 1:{target_rr})")
                        else:
                            investor_orders_error += 1
                            stats["orders_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid tick values - using fallback calculation")
                    # Fallback method using profit calculation for a small price movement
                    try:
                        test_move = symbol_info.point * 10
                        if is_buy:
                            test_price = order.price_open + test_move
                        else:
                            test_price = order.price_open - test_move
                            
                        test_profit = mt5.order_calc_profit(calc_type, order.symbol, order.volume_initial, 
                                                            order.price_open, test_price)
                        
                        if test_profit and test_profit != 0:
                            point_value = abs(test_profit) / 10
                            price_move_needed = target_profit_usd / point_value * symbol_info.point
                            
                            digits = symbol_info.digits
                            price_move_needed = round(price_move_needed, digits)
                            
                            if is_buy:
                                new_tp = round(order.price_open + price_move_needed, digits)
                            else:
                                new_tp = round(order.price_open - price_move_needed, digits)
                            
                            print(f"       Using fallback calculation")
                            
                            if order.tp == 0 or abs(order.tp - new_tp) > symbol_info.point * 20:
                                modify_request = {
                                    "action": mt5.TRADE_ACTION_MODIFY,
                                    "order": order.ticket,
                                    "price": order.price_open,
                                    "sl": order.sl,
                                    "tp": new_tp,
                                }
                                
                                result = mt5.order_send(modify_request)
                                
                                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                                    investor_orders_adjusted += 1
                                    stats["orders_adjusted"] += 1
                                    print(f"       ✅ TP adjusted using fallback method to {new_tp:.{digits}f}")
                                else:
                                    investor_orders_error += 1
                                    stats["orders_error"] += 1
                                    print(f"       ❌ Fallback modification failed")
                            else:
                                investor_orders_skipped += 1
                                stats["orders_skipped"] += 1
                                print(f"       ✅ TP already correct (fallback check)")
                        else:
                            investor_orders_skipped += 1
                            stats["orders_skipped"] += 1
                            print(f"       ⚠️  Cannot calculate using fallback method")
                    except Exception as e:
                        investor_orders_error += 1
                        stats["orders_error"] += 1
                        print(f"       ❌ Fallback calculation error: {e}")

        # --- INVESTOR SUMMARY ---
        total_checked = investor_positions_checked + investor_orders_checked
        total_adjusted = investor_positions_adjusted + investor_orders_adjusted
        
        if total_checked > 0:
            print(f"\n  └─ 📊 Intelligent R:R Correction Results for {user_brokerid}:")
            if investor_positions_checked > 0:
                print(f"       • Positions checked: {investor_positions_checked}")
                print(f"       • Positions adjusted: {investor_positions_adjusted}")
                print(f"       • Positions skipped: {investor_positions_skipped}")
            if investor_orders_checked > 0:
                print(f"       • Pending orders checked: {investor_orders_checked}")
                print(f"       • Pending orders adjusted: {investor_orders_adjusted}")
                print(f"       • Pending orders skipped: {investor_orders_skipped}")
            if investor_positions_error + investor_orders_error > 0:
                print(f"       • Errors: {investor_positions_error + investor_orders_error}")
            else:
                print(f"       ✅ All adjustments completed successfully")
            stats["processing_success"] = True
        else:
            print(f"  └─ 🔘 No positions or pending orders found.")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 INTELLIGENT R:R CORRECTION SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Positions checked: {stats['positions_checked']}")
    print(f"   Positions adjusted: {stats['positions_adjusted']}")
    print(f"   Pending orders checked: {stats['orders_checked']}")
    print(f"   Pending orders adjusted: {stats['orders_adjusted']}")
    print(f"   Total checked: {stats['positions_checked'] + stats['orders_checked']}")
    print(f"   Total adjusted: {stats['positions_adjusted'] + stats['orders_adjusted']}")
    print(f"   Orders skipped: {stats['orders_skipped']}")
    print(f"   Errors: {stats['orders_error']}")
    
    if stats["rr_matches"]:
        print(f"\n   📊 R:R Usage Breakdown:")
        for rr, count in sorted(stats["rr_matches"].items()):
            print(f"       • 1:{rr}: {count} orders")
    if stats["rr_mismatches"] > 0:
        print(f"   ⚠️  Orders using default R:R (no match): {stats['rr_mismatches']}")
    
    total_checked = stats['positions_checked'] + stats['orders_checked']
    total_adjusted = stats['positions_adjusted'] + stats['orders_adjusted']
    if total_checked > 0:
        success_rate = (total_adjusted / total_checked) * 100
        print(f"   Adjustment success rate: {success_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 INTELLIGENT R:R CORRECTION COMPLETE {'='*10}\n")
    return stats

def history_closed_orders_removal_in_pendingorders(inv_id=None):
    """
    Scans history for the last 48 hours. If a position was closed, 
    any pending limit orders with the same first 4 digits in the price 
    are cancelled to prevent re-entry.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were removed, False otherwise
    """
    from datetime import datetime, timedelta
    print(f"\n{'='*10} 📜 HISTORY AUDIT: PREVENTING RE-ENTRY {'='*10}")

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    any_orders_removed = False

    for user_brokerid in investor_ids:
        print(f" [{user_brokerid}] 🔍 Checking 48h history for duplicates...")
        
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {user_brokerid}")
            continue

        # 1. Define the 48-hour window
        from_date = datetime.now() - timedelta(hours=48)
        to_date = datetime.now()

        # 2. Get Closed Positions (Deals)
        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals is None:
            print(f"  └─ ⚠️ Could not access history for {user_brokerid}")
            continue

        # 3. Create a set of "Used Price Prefixes"
        # We store: (symbol, price_prefix)
        used_entries = set()
        for deal in history_deals:
            # Only look at actual trades (buy/sell) that were closed
            if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
                # Extract first 3 significant digits of the price
                # We remove the decimal to handle 0.856 and 1901 uniformly
                clean_price = str(deal.price).replace('.', '')[:4]
                used_entries.add((deal.symbol, clean_price))

        if not used_entries:
            print(f"  └─ ✅ No closed orders found in last 48h.")
            continue

        # 4. Check Current Pending Orders
        pending_orders = mt5.orders_get()
        removed_count = 0
        orders_checked = 0

        if pending_orders:
            for order in pending_orders:
                # Only target limit orders
                if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    orders_checked += 1
                    order_price_prefix = str(order.price_open).replace('.', '')[:4]
                    
                    # If this symbol + price prefix exists in history, kill the order
                    if (order.symbol, order_price_prefix) in used_entries:
                        print(f"  └─ 🚫 DUPLICATE FOUND: {order.symbol} at {order.price_open}")
                        print(f"     Match found in history (Prefix: {order_price_prefix}). Cancelling...")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        res = mt5.order_send(cancel_request)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            removed_count += 1
                            any_orders_removed = True
                            print(f"     ✅ Order #{order.ticket} cancelled successfully")
                        else:
                            error_msg = res.comment if res else f"Error code: {res.retcode if res else 'Unknown'}"
                            print(f"     ❌ Failed to cancel #{order.ticket}: {error_msg}")

        print(f"  └─ 📊 Cleanup Result: {removed_count} duplicate limit orders removed out of {orders_checked} checked.")

    print(f"\n{'='*10} 🏁 HISTORY AUDIT COMPLETE {'='*10}\n")
    return any_orders_removed

def apply_dynamic_breakeven(inv_id=None):
    """
    Function: Dynamically moves stop loss to breakeven or partial profit levels based on
    running profit reward multiples. Uses breakeven_dictionary from accountmanagement.json
    to determine at which reward levels to adjust SL.
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 🎯 DYNAMIC BREAKEVEN: MONITORING RUNNING PROFIT REWARDS {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "positions_checked": 0,
        "positions_adjusted": 0,
        "positions_skipped": 0,
        "positions_error": 0,
        "breakeven_events": 0,
        "processing_success": False
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking breakeven configurations...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check if breakeven is enabled
            settings = config.get("settings", {})
            if not settings.get("enable_breakeven", False):
                print(f"  └─ ⏭️  Breakeven disabled in settings. Skipping.")
                continue
            
            # Get breakeven dictionary
            breakeven_config = settings.get("breakeven_dictionary", [])
            if not breakeven_config:
                print(f"  └─ ⚠️  No breakeven configuration found. Using default.")
                # Default configuration if none provided
                breakeven_config = [
                    {"reward": 1, "breakeven_at_reward": 0.5},
                    {"reward": 2, "breakeven_at_reward": 1},
                    {"reward": 3, "breakeven_at_reward": 1.5}
                ]
            
            # Sort by reward level (ascending) to process in order
            breakeven_config.sort(key=lambda x: x["reward"])
            
            print(f"  └─ ✅ Breakeven enabled with {len(breakeven_config)} reward levels:")
            for level in breakeven_config:
                print(f"       • At {level['reward']}R profit → Move SL to {level['breakeven_at_reward']}R")
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            stats["positions_error"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌ login failed: {error}")
                stats["positions_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            stats["positions_error"] += 1
            continue
            
        balance = acc_info.balance
        print(f"\n  └─ 📊 Account Balance: ${balance:,.2f}")

        # --- CHECK ALL OPEN POSITIONS ---
        positions = mt5.positions_get()
        investor_positions_checked = 0
        investor_positions_adjusted = 0
        investor_positions_skipped = 0
        investor_positions_error = 0
        investor_breakeven_events = 0

        # Define position types for better readability
        POSITION_TYPES = {
            mt5.POSITION_TYPE_BUY: "BUY",
            mt5.POSITION_TYPE_SELL: "SELL"
        }

        if positions:
            print(f"\n  └─ 🔍 Scanning {len(positions)} open positions for breakeven opportunities...")
            
            for position in positions:
                investor_positions_checked += 1
                stats["positions_checked"] += 1
                
                position_type_name = POSITION_TYPES.get(position.type, f"Unknown Type {position.type}")
                
                # Skip positions without SL
                if position.sl == 0:
                    print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                    print(f"       ⚠️  No SL set - cannot manage breakeven. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Get symbol info
                symbol_info = mt5.symbol_info(position.symbol)
                if not symbol_info:
                    print(f"\n    └─ 📋 Position #{position.ticket} | {position.symbol}")
                    print(f"       ⚠️  Cannot get symbol info. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Determine position direction
                is_buy = position.type == mt5.POSITION_TYPE_BUY
                
                print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                
                # Calculate current risk (from entry to original SL)
                if is_buy:
                    risk_distance = position.price_open - position.sl
                else:
                    risk_distance = position.sl - position.price_open
                
                risk_points = abs(risk_distance) / symbol_info.point
                
                # Calculate point value
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    point_value = tick_value / tick_size * symbol_info.point
                    risk_usd = round(risk_points * point_value * position.volume, 2)
                else:
                    # Fallback: calculate risk using profit calculator
                    calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                    sl_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                      position.price_open, position.sl)
                    if sl_profit is not None:
                        risk_usd = round(abs(sl_profit), 2)
                    else:
                        print(f"       ⚠️  Cannot calculate risk. Skipping.")
                        investor_positions_skipped += 1
                        stats["positions_skipped"] += 1
                        continue

                # Calculate current profit in R multiples
                current_profit_usd = position.profit
                
                if risk_usd > 0:
                    current_r_multiple = current_profit_usd / risk_usd
                else:
                    print(f"       ⚠️  Invalid risk value. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                print(f"       • Risk: ${risk_usd:.2f} | Current P/L: ${current_profit_usd:.2f} ({current_r_multiple:.2f}R)")

                # Skip if position is not in profit
                if current_profit_usd <= 0:
                    print(f"       ⏭️  Position not in profit. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Find applicable breakeven rules
                applicable_rules = []
                for rule in breakeven_config:
                    reward_threshold = rule["reward"]
                    if current_r_multiple >= reward_threshold:
                        applicable_rules.append(rule)
                
                if not applicable_rules:
                    print(f"       ⏭️  No breakeven threshold reached (current: {current_r_multiple:.2f}R)")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Get the highest applicable rule (last in sorted list)
                highest_rule = applicable_rules[-1]
                target_reward = highest_rule["breakeven_at_reward"]
                
                print(f"       🎯 Reached {highest_rule['reward']}R threshold")
                print(f"       Target SL position: {target_reward}R")

                # Calculate target SL price based on target reward
                if target_reward >= 0:
                    # For positive target reward, we want SL at entry + (risk_distance * target_reward)
                    # But direction matters
                    if is_buy:
                        # For BUY: entry + (risk * target_reward)
                        target_sl_price = position.price_open + (risk_distance * target_reward)
                    else:
                        # For SELL: entry - (risk * target_reward)
                        target_sl_price = position.price_open - (risk_distance * target_reward)
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    target_sl_price = round(target_sl_price, digits)
                    
                    print(f"       Current SL: {position.sl:.{digits}f}")
                    print(f"       Target SL:  {target_sl_price:.{digits}f} ({target_reward}R)")
                    
                    # Check if SL needs adjustment
                    current_sl_distance = abs(position.sl - position.price_open) if position.sl != 0 else 0
                    target_sl_distance = abs(target_sl_price - position.price_open)
                    
                    # Calculate threshold (10% of target distance or 2 pips)
                    pip_threshold = max(target_sl_distance * 0.1, symbol_info.point * 20)
                    
                    should_adjust = False
                    
                    if position.sl == 0:
                        print(f"       📝 No SL currently set")
                        should_adjust = True
                    elif abs(current_sl_distance - target_sl_distance) > pip_threshold:
                        print(f"       📐 SL needs adjustment")
                        should_adjust = True
                    else:
                        # Check if we're moving in the right direction (should only move SL towards profit)
                        if is_buy and target_sl_price > position.sl:
                            print(f"       ✅ SL already at or beyond target")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        elif not is_buy and target_sl_price < position.sl:
                            print(f"       ✅ SL already at or beyond target")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        else:
                            should_adjust = True
                    
                    if should_adjust:
                        # Ensure we're only moving SL in the profit direction
                        if is_buy and target_sl_price <= position.sl:
                            print(f"       ⚠️  Target SL would not improve position. Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        elif not is_buy and target_sl_price >= position.sl:
                            print(f"       ⚠️  Target SL would not improve position. Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        
                        # Prepare modification request
                        modify_request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": position.ticket,
                            "sl": target_sl_price,
                            "tp": position.tp,  # Keep existing TP
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_positions_adjusted += 1
                            investor_breakeven_events += 1
                            stats["positions_adjusted"] += 1
                            stats["breakeven_events"] += 1
                            print(f"       ✅ SL adjusted successfully to {target_sl_price:.{digits}f} ({target_reward}R)")
                        else:
                            investor_positions_error += 1
                            stats["positions_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid target reward: {target_reward}")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1

        # --- INVESTOR SUMMARY ---
        if investor_positions_checked > 0:
            print(f"\n  └─ 📊 Breakeven Results for {user_brokerid}:")
            print(f"       • Positions checked: {investor_positions_checked}")
            print(f"       • Positions adjusted: {investor_positions_adjusted}")
            print(f"       • Breakeven events: {investor_breakeven_events}")
            print(f"       • Positions skipped: {investor_positions_skipped}")
            if investor_positions_error > 0:
                print(f"       • Errors: {investor_positions_error}")
            else:
                print(f"       ✅ All breakeven checks completed successfully")
            stats["processing_success"] = True
        else:
            print(f"  └─ 🔘 No open positions found.")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 DYNAMIC BREAKEVEN SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Positions checked: {stats['positions_checked']}")
    print(f"   Positions adjusted: {stats['positions_adjusted']}")
    print(f"   Breakeven events: {stats['breakeven_events']}")
    print(f"   Positions skipped: {stats['positions_skipped']}")
    print(f"   Errors: {stats['positions_error']}")
    
    if stats['positions_checked'] > 0:
        adjustment_rate = (stats['positions_adjusted'] / stats['positions_checked']) * 100
        print(f"   Adjustment rate: {adjustment_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 DYNAMIC BREAKEVEN MONITORING COMPLETE {'='*10}\n")
    return stats

def history_closed_orders_removal_in_pendingorders(inv_id=None):
    """
    Scans history for the last 48 hours. If a position was closed, 
    any pending limit orders with the same first 4 digits in the price 
    are cancelled to prevent re-entry.
    
    Args:
        inv_id (str, optional): Specific investor ID to process. If None, processes all investors.
        MT5 should already be initialized and logged in for this investor.
    
    Returns:
        bool: True if any orders were removed, False otherwise
    """
    from datetime import datetime, timedelta
    print(f"\n{'='*10} 📜 HISTORY AUDIT: PREVENTING RE-ENTRY {'='*10}")

    # Determine which investors to process
    if inv_id:
        investor_ids = [inv_id]
    else:
        investor_ids = list(usersdictionary.keys())
    
    if not investor_ids:
        print(" └─ 🔘 No investors found.")
        return False

    any_orders_removed = False

    for user_brokerid in investor_ids:
        print(f" [{user_brokerid}] 🔍 Checking 48h history for duplicates...")
        
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found for {user_brokerid}")
            continue

        # 1. Define the 48-hour window
        from_date = datetime.now() - timedelta(hours=48)
        to_date = datetime.now()

        # 2. Get Closed Positions (Deals)
        history_deals = mt5.history_deals_get(from_date, to_date)
        if history_deals is None:
            print(f"  └─ ⚠️ Could not access history for {user_brokerid}")
            continue

        # 3. Create a set of "Used Price Prefixes"
        # We store: (symbol, price_prefix)
        used_entries = set()
        for deal in history_deals:
            # Only look at actual trades (buy/sell) that were closed
            if deal.entry in [mt5.DEAL_ENTRY_OUT, mt5.DEAL_ENTRY_INOUT]:
                # Extract first 3 significant digits of the price
                # We remove the decimal to handle 0.856 and 1901 uniformly
                clean_price = str(deal.price).replace('.', '')[:4]
                used_entries.add((deal.symbol, clean_price))

        if not used_entries:
            print(f"  └─ ✅ No closed orders found in last 48h.")
            continue

        # 4. Check Current Pending Orders
        pending_orders = mt5.orders_get()
        removed_count = 0
        orders_checked = 0

        if pending_orders:
            for order in pending_orders:
                # Only target limit orders
                if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_SELL_LIMIT]:
                    orders_checked += 1
                    order_price_prefix = str(order.price_open).replace('.', '')[:4]
                    
                    # If this symbol + price prefix exists in history, kill the order
                    if (order.symbol, order_price_prefix) in used_entries:
                        print(f"  └─ 🚫 DUPLICATE FOUND: {order.symbol} at {order.price_open}")
                        print(f"     Match found in history (Prefix: {order_price_prefix}). Cancelling...")
                        
                        cancel_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        }
                        res = mt5.order_send(cancel_request)
                        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                            removed_count += 1
                            any_orders_removed = True
                            print(f"     ✅ Order #{order.ticket} cancelled successfully")
                        else:
                            error_msg = res.comment if res else f"Error code: {res.retcode if res else 'Unknown'}"
                            print(f"     ❌ Failed to cancel #{order.ticket}: {error_msg}")

        print(f"  └─ 📊 Cleanup Result: {removed_count} duplicate limit orders removed out of {orders_checked} checked.")

    print(f"\n{'='*10} 🏁 HISTORY AUDIT COMPLETE {'='*10}\n")
    return any_orders_removed

def apply_dynamic_breakeven(inv_id=None):
    """
    Function: Dynamically moves stop loss to breakeven or partial profit levels based on
    running profit reward multiples. Uses breakeven_dictionary from accountmanagement.json
    to determine at which reward levels to adjust SL.
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 🎯 DYNAMIC BREAKEVEN: MONITORING RUNNING PROFIT REWARDS {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "positions_checked": 0,
        "positions_adjusted": 0,
        "positions_skipped": 0,
        "positions_error": 0,
        "breakeven_events": 0,
        "processing_success": False
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking breakeven configurations...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─ ❌ No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check if breakeven is enabled
            settings = config.get("settings", {})
            if not settings.get("enable_breakeven", False):
                print(f"  └─ ⏭️  Breakeven disabled in settings. Skipping.")
                continue
            
            # Get breakeven dictionary
            breakeven_config = settings.get("breakeven_dictionary", [])
            if not breakeven_config:
                print(f"  └─ ⚠️  No breakeven configuration found. Using default.")
                # Default configuration if none provided
                breakeven_config = [
                    {"reward": 1, "breakeven_at_reward": 0.5},
                    {"reward": 2, "breakeven_at_reward": 1},
                    {"reward": 3, "breakeven_at_reward": 1.5}
                ]
            
            # Sort by reward level (ascending) to process in order
            breakeven_config.sort(key=lambda x: x["reward"])
            
            print(f"  └─ ✅ Breakeven enabled with {len(breakeven_config)} reward levels:")
            for level in breakeven_config:
                print(f"       • At {level['reward']}R profit → Move SL to {level['breakeven_at_reward']}R")
            
        except Exception as e:
            print(f"  └─ ❌ Failed to read config: {e}")
            stats["positions_error"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─ ❌ login failed: {error}")
                stats["positions_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─ ❌ Failed to get account info")
            stats["positions_error"] += 1
            continue
            
        balance = acc_info.balance
        print(f"\n  └─ 📊 Account Balance: ${balance:,.2f}")

        # --- CHECK ALL OPEN POSITIONS ---
        positions = mt5.positions_get()
        investor_positions_checked = 0
        investor_positions_adjusted = 0
        investor_positions_skipped = 0
        investor_positions_error = 0
        investor_breakeven_events = 0

        # Define position types for better readability
        POSITION_TYPES = {
            mt5.POSITION_TYPE_BUY: "BUY",
            mt5.POSITION_TYPE_SELL: "SELL"
        }

        if positions:
            print(f"\n  └─ 🔍 Scanning {len(positions)} open positions for breakeven opportunities...")
            
            for position in positions:
                investor_positions_checked += 1
                stats["positions_checked"] += 1
                
                position_type_name = POSITION_TYPES.get(position.type, f"Unknown Type {position.type}")
                
                # Skip positions without SL
                if position.sl == 0:
                    print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                    print(f"       ⚠️  No SL set - cannot manage breakeven. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Get symbol info
                symbol_info = mt5.symbol_info(position.symbol)
                if not symbol_info:
                    print(f"\n    └─ 📋 Position #{position.ticket} | {position.symbol}")
                    print(f"       ⚠️  Cannot get symbol info. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Determine position direction
                is_buy = position.type == mt5.POSITION_TYPE_BUY
                
                print(f"\n    └─ 📋 Position #{position.ticket} | {position_type_name} | {position.symbol}")
                
                # Calculate current risk (from entry to original SL)
                if is_buy:
                    risk_distance = position.price_open - position.sl
                else:
                    risk_distance = position.sl - position.price_open
                
                risk_points = abs(risk_distance) / symbol_info.point
                
                # Calculate point value
                tick_value = symbol_info.trade_tick_value
                tick_size = symbol_info.trade_tick_size
                
                if tick_value > 0 and tick_size > 0:
                    point_value = tick_value / tick_size * symbol_info.point
                    risk_usd = round(risk_points * point_value * position.volume, 2)
                else:
                    # Fallback: calculate risk using profit calculator
                    calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                    sl_profit = mt5.order_calc_profit(calc_type, position.symbol, position.volume, 
                                                      position.price_open, position.sl)
                    if sl_profit is not None:
                        risk_usd = round(abs(sl_profit), 2)
                    else:
                        print(f"       ⚠️  Cannot calculate risk. Skipping.")
                        investor_positions_skipped += 1
                        stats["positions_skipped"] += 1
                        continue

                # Calculate current profit in R multiples
                current_profit_usd = position.profit
                
                if risk_usd > 0:
                    current_r_multiple = current_profit_usd / risk_usd
                else:
                    print(f"       ⚠️  Invalid risk value. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                print(f"       • Risk: ${risk_usd:.2f} | Current P/L: ${current_profit_usd:.2f} ({current_r_multiple:.2f}R)")

                # Skip if position is not in profit
                if current_profit_usd <= 0:
                    print(f"       ⏭️  Position not in profit. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Find applicable breakeven rules
                applicable_rules = []
                for rule in breakeven_config:
                    reward_threshold = rule["reward"]
                    if current_r_multiple >= reward_threshold:
                        applicable_rules.append(rule)
                
                if not applicable_rules:
                    print(f"       ⏭️  No breakeven threshold reached (current: {current_r_multiple:.2f}R)")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Get the highest applicable rule (last in sorted list)
                highest_rule = applicable_rules[-1]
                target_reward = highest_rule["breakeven_at_reward"]
                
                print(f"       🎯 Reached {highest_rule['reward']}R threshold")
                print(f"       Target SL position: {target_reward}R")

                # Calculate target SL price based on target reward
                if target_reward >= 0:
                    # For positive target reward, we want SL at entry + (risk_distance * target_reward)
                    # But direction matters
                    if is_buy:
                        # For BUY: entry + (risk * target_reward)
                        target_sl_price = position.price_open + (risk_distance * target_reward)
                    else:
                        # For SELL: entry - (risk * target_reward)
                        target_sl_price = position.price_open - (risk_distance * target_reward)
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    target_sl_price = round(target_sl_price, digits)
                    
                    print(f"       Current SL: {position.sl:.{digits}f}")
                    print(f"       Target SL:  {target_sl_price:.{digits}f} ({target_reward}R)")
                    
                    # Check if SL needs adjustment
                    current_sl_distance = abs(position.sl - position.price_open) if position.sl != 0 else 0
                    target_sl_distance = abs(target_sl_price - position.price_open)
                    
                    # Calculate threshold (10% of target distance or 2 pips)
                    pip_threshold = max(target_sl_distance * 0.1, symbol_info.point * 20)
                    
                    should_adjust = False
                    
                    if position.sl == 0:
                        print(f"       📝 No SL currently set")
                        should_adjust = True
                    elif abs(current_sl_distance - target_sl_distance) > pip_threshold:
                        print(f"       📐 SL needs adjustment")
                        should_adjust = True
                    else:
                        # Check if we're moving in the right direction (should only move SL towards profit)
                        if is_buy and target_sl_price > position.sl:
                            print(f"       ✅ SL already at or beyond target")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        elif not is_buy and target_sl_price < position.sl:
                            print(f"       ✅ SL already at or beyond target")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        else:
                            should_adjust = True
                    
                    if should_adjust:
                        # Ensure we're only moving SL in the profit direction
                        if is_buy and target_sl_price <= position.sl:
                            print(f"       ⚠️  Target SL would not improve position. Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        elif not is_buy and target_sl_price >= position.sl:
                            print(f"       ⚠️  Target SL would not improve position. Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        
                        # Prepare modification request
                        modify_request = {
                            "action": mt5.TRADE_ACTION_SLTP,
                            "position": position.ticket,
                            "sl": target_sl_price,
                            "tp": position.tp,  # Keep existing TP
                        }
                        
                        # Send modification
                        result = mt5.order_send(modify_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            investor_positions_adjusted += 1
                            investor_breakeven_events += 1
                            stats["positions_adjusted"] += 1
                            stats["breakeven_events"] += 1
                            print(f"       ✅ SL adjusted successfully to {target_sl_price:.{digits}f} ({target_reward}R)")
                        else:
                            investor_positions_error += 1
                            stats["positions_error"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"       ❌ Modification failed: {error_msg}")
                else:
                    print(f"       ⚠️  Invalid target reward: {target_reward}")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1

        # --- INVESTOR SUMMARY ---
        if investor_positions_checked > 0:
            print(f"\n  └─ 📊 Breakeven Results for {user_brokerid}:")
            print(f"       • Positions checked: {investor_positions_checked}")
            print(f"       • Positions adjusted: {investor_positions_adjusted}")
            print(f"       • Breakeven events: {investor_breakeven_events}")
            print(f"       • Positions skipped: {investor_positions_skipped}")
            if investor_positions_error > 0:
                print(f"       • Errors: {investor_positions_error}")
            else:
                print(f"       ✅ All breakeven checks completed successfully")
            stats["processing_success"] = True
        else:
            print(f"  └─ 🔘 No open positions found.")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 DYNAMIC BREAKEVEN SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Positions checked: {stats['positions_checked']}")
    print(f"   Positions adjusted: {stats['positions_adjusted']}")
    print(f"   Breakeven events: {stats['breakeven_events']}")
    print(f"   Positions skipped: {stats['positions_skipped']}")
    print(f"   Errors: {stats['positions_error']}")
    
    if stats['positions_checked'] > 0:
        adjustment_rate = (stats['positions_adjusted'] / stats['positions_checked']) * 100
        print(f"   Adjustment rate: {adjustment_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 DYNAMIC BREAKEVEN MONITORING COMPLETE {'='*10}\n")
    return stats


# real accounts 
def process_single_investor(inv_id):
    """
    WORKER FUNCTION: Handles the entire pipeline for ONE investor ID.
    """
    account_stats = {"inv_id": inv_id, "success": False}
    
    try:
        with open(INVESTOR_USERS, 'r') as f:
            investor_users = json.load(f)
        broker_cfg = investor_users.get(inv_id)
    except Exception as e:
        print(f" [{inv_id}] ❌ JSON Read Error: {e}")
        return account_stats

    if not broker_cfg:
        return account_stats

    # Small jitter to prevent OS file-lock collisions when launching multiple .exe files
    time.sleep(random.uniform(0.1, 1.5)) 
    
    login_id = int(broker_cfg['LOGIN_ID'])
    mt5_path = broker_cfg["TERMINAL_PATH"]
    
    try:
        # Increase timeout slightly but keep it non-blocking for other processes
        # If this fails, it only kills this specific worker.
        if not mt5.initialize(path=mt5_path, timeout=60000): # 60 sec limit
            print(f" [{inv_id}] ❌ MT5 Init Timeout/Failed")
            
            # --- MOVE TO ISSUES_INVESTORS ON INIT FAILURE ---
            print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to MT5 initialization failure")
            
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Specific message for MT5 initialization failure
                    investor_data_to_move['MESSAGE'] = "invalid broker login please check your login details"
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
            
            mt5.shutdown()
            return account_stats

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f" [{inv_id}] ❌ Login failed")
            
            # --- MOVE TO ISSUES_INVESTORS ON LOGIN FAILURE ---
            print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to login failure")
            
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Specific message for login failure
                    investor_data_to_move['MESSAGE'] = "invalid broker login please check your login details"
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
            
            mt5.shutdown()
            return account_stats

        # --- EXECUTION PIPELINE ---
        # If any of these functions have internal delays, they won't affect other investors
        fetch_ohlc_data_for_investor(inv_id=inv_id)
        directional_bias(inv_id=inv_id)

        mt5.shutdown()
        account_stats["success"] = True
        
    except Exception as e:
        print(f" [{inv_id}] ❌ Pipeline Error: {e}")
        
        # --- MOVE TO ISSUES_INVESTORS ON ANY UNEXPECTED ERROR ---
        print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to unexpected error: {e}")
        
        try:
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Generic message for unexpected errors
                    investor_data_to_move['MESSAGE'] = f"unexpected error: {str(e)[:100]}"  # Truncate long errors
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
        except Exception as move_error:
            print(f"  ❌ Failed to move investor to issues_investors.json: {move_error}")
        
        mt5.shutdown()
    
    return account_stats

def process_single_invest(inv_id):
    """
    WORKER FUNCTION: Handles the entire pipeline for ONE investor ID.
    """
    account_stats = {"inv_id": inv_id, "success": False}
    
    try:
        with open(INVESTOR_USERS, 'r') as f:
            investor_users = json.load(f)
        broker_cfg = investor_users.get(inv_id)
    except Exception as e:
        print(f" [{inv_id}] ❌ JSON Read Error: {e}")
        return account_stats

    if not broker_cfg:
        return account_stats

    # Small jitter to prevent OS file-lock collisions when launching multiple .exe files
    time.sleep(random.uniform(0.1, 1.5)) 
    
    login_id = int(broker_cfg['LOGIN_ID'])
    mt5_path = broker_cfg["TERMINAL_PATH"]
    
    try:
        # Increase timeout slightly but keep it non-blocking for other processes
        # If this fails, it only kills this specific worker.
        if not mt5.initialize(path=mt5_path, timeout=60000): # 60 sec limit
            print(f" [{inv_id}] ❌ MT5 Init Timeout/Failed")
            
            # --- MOVE TO ISSUES_INVESTORS ON INIT FAILURE ---
            print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to MT5 initialization failure")
            
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Specific message for MT5 initialization failure
                    investor_data_to_move['MESSAGE'] = "invalid broker login please check your login details"
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
            
            mt5.shutdown()
            return account_stats

        if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
            print(f" [{inv_id}] ❌ Login failed")
            
            # --- MOVE TO ISSUES_INVESTORS ON LOGIN FAILURE ---
            print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to login failure")
            
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Specific message for login failure
                    investor_data_to_move['MESSAGE'] = "invalid broker login please check your login details"
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
            
            mt5.shutdown()
            return account_stats

        # --- EXECUTION PIPELINE ---
        # If any of these functions have internal delays, they won't affect other investors
        move_verified_investors()
        update_verified_investors_file()
        get_requirements(inv_id=inv_id)
        
        accountmanagement_manager(inv_id=inv_id)
        deduplicate_orders(inv_id=inv_id)
        filter_unauthorized_symbols(inv_id=inv_id)
        filter_unauthorized_timeframes(inv_id=inv_id)
        backup_limit_orders(inv_id=inv_id)
        populate_orders_missing_fields(inv_id=inv_id)
        activate_usd_based_risk_on_empty_pricelevels(inv_id=inv_id)
        enforce_investors_risk(inv_id=inv_id)
        calculate_investor_symbols_orders(inv_id=inv_id)
        padding_tight_usd_risk(inv_id=inv_id)
        live_usd_risk_and_scaling(inv_id=inv_id)
        apply_default_prices(inv_id=inv_id)
        place_usd_orders(inv_id=inv_id)
        orders_risk_correction(inv_id=inv_id)
        check_pending_orders_risk(inv_id=inv_id)
        history_closed_orders_removal_in_pendingorders(inv_id=inv_id)
        apply_dynamic_breakeven(inv_id=inv_id)

        update_verified_investors_file()
        check_and_record_authorized_actions(inv_id=inv_id)
        update_investor_info(inv_id=inv_id)

        mt5.shutdown()
        account_stats["success"] = True
        print(f" [{inv_id}] ✅ Processed Successfully")
        
    except Exception as e:
        print(f" [{inv_id}] ❌ Pipeline Error: {e}")
        
        # --- MOVE TO ISSUES_INVESTORS ON ANY UNEXPECTED ERROR ---
        print(f"  ❌ Moving investor {inv_id} to issues_investors.json due to unexpected error: {e}")
        
        try:
            if os.path.exists(INVESTOR_USERS):
                with open(INVESTOR_USERS, 'r', encoding='utf-8') as f:
                    investors_data = json.load(f)
                
                investor_data_to_move = None
                if isinstance(investors_data, list):
                    for i, inv in enumerate(investors_data):
                        if inv_id in inv:
                            investor_data_to_move = inv[inv_id]
                            investors_data.pop(i)
                            break
                else:
                    if inv_id in investors_data:
                        investor_data_to_move = investors_data[inv_id]
                        del investors_data[inv_id]
                
                if investor_data_to_move:
                    # Generic message for unexpected errors
                    investor_data_to_move['MESSAGE'] = f"unexpected error: {str(e)[:100]}"  # Truncate long errors
                    
                    with open(INVESTOR_USERS, 'w', encoding='utf-8') as f:
                        json.dump(investors_data, f, indent=4)
                    
                    issues_data = {}
                    if os.path.exists(ISSUES_INVESTORS):
                        try:
                            with open(ISSUES_INVESTORS, 'r', encoding='utf-8') as f:
                                issues_data = json.load(f)
                        except: issues_data = {}
                    
                    issues_data[inv_id] = investor_data_to_move
                    with open(ISSUES_INVESTORS, 'w', encoding='utf-8') as f:
                        json.dump(issues_data, f, indent=4)
                    
                    print(f"  ✅ Successfully moved investor {inv_id} to issues_investors.json")
                else:
                    print(f"  ⚠️  Investor {inv_id} not found in investors.json")
        except Exception as move_error:
            print(f"  ❌ Failed to move investor to issues_investors.json: {move_error}")
        
        mt5.shutdown()
    
    return account_stats

def place_orders_parallel():
    """
    ORCHESTRATOR: Spawns processes. If one hangs, others continue.
    """
    print(f"\n{'='*10} 🚀 MULTIPROCESSING ENGINE START {'='*10}")
    
    try:
        with open(INVESTOR_USERS, 'r') as f:
            investor_users = json.load(f)
    except Exception as e:
        print(f" ❌ Could not load JSON: {e}")
        return False

    investor_ids = list(investor_users.keys())
    if not investor_ids:
        return False

    # Define number of workers (max cores or number of accounts)
    num_processes = len(investor_ids)
    
    # Using a context manager ensures the pool is cleaned up properly
    with mp.Pool(processes=num_processes) as pool:
        # map() is blocking until all are done, but the executions are parallel.
        # This means the script waits for the SLOWEST investor to finish 
        # before printing the final summary.
        results = pool.map(process_single_investor, investor_ids)

    successful = sum(1 for r in results if r.get("success"))
    print(f"\n{'='*10} ALL TASKS FINISHED {'='*10}")
    print(f" Success: {successful} | Failed: {len(investor_ids)-successful}")
    
    return successful > 0


if __name__ == "__main__":
    place_orders_parallel()


