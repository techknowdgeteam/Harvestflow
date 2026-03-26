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
import multiprocessing as mp
import time

INVESTOR_USERS = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\demoinvestors.json"
INV_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors"
NORMALIZE_SYMBOLS_PATH = r"C:\xampp\htdocs\synapse\synarex\symbols_normalization.json"
DEFAULT_ACCOUNTMANAGEMENT = r"C:\xampp\htdocs\synapse\synarex\default_accountmanagement.json"
VERIFIED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\verified_investors.json"
UPDATED_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\updated_investors.json"
ISSUES_INVESTORS = r"C:\xampp\htdocs\synapse\synarex\issues_investors.json"

def load_investors_dictionary():
    BROKERS_JSON_PATH = r"C:\xampp\htdocs\synapse\synarex\usersdata\investors\demoinvestors.json"
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
        print(f"Invalid JSON in demoinvestors.json: {e}", "CRITICAL")
        return {}
    except Exception as e:
        print(f"Failed to load demoinvestors.json: {e}", "CRITICAL")
        return {}
usersdictionary = load_investors_dictionary()

#----
def move_verified_investors():
    """
    Moves verified investors from verified_investors.json to:
    Step 1: investors.json (with limited fields: LOGIN_ID, PASSWORD, SERVER, INVESTED_WITH, TERMINAL_PATH)
    Step 2: Create activities.json directly in investor root folder (NEW PATH STRUCTURE)
    
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
    
    processed_summary = []
    skipped_summary = []
    created_summary = []
    updated_summary = []
    
    for inv_id, investor_data in verified_data.items():
        # Case-insensitive lookup
        investor_data_upper = {k.upper(): v for k, v in investor_data.items()}
        
        invested_with = investor_data_upper.get('INVESTED_WITH', '').strip()
        execution_start = investor_data_upper.get('EXECUTION_START_DATE', '').strip()
        contract_days = investor_data_upper.get('CONTRACT_DAYS_LEFT', '').strip()
        terminal_path = investor_data_upper.get('TERMINAL_PATH', '').strip()
        
        # Skip if missing required fields
        if not all([invested_with, execution_start, contract_days, terminal_path]):
            skipped_summary.append(inv_id)
            continue
        
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
        
        # Format execution start date
        formatted_start_date = execution_start
        try:
            date_obj = datetime.strptime(execution_start, "%Y-%m-%d")
            formatted_start_date = date_obj.strftime("%B %d, %Y")
        except:
            try:
                date_obj = datetime.strptime(execution_start, "%B %d, %Y")
                formatted_start_date = execution_start
            except:
                pass
        
        # Calculate contract duration and expiry
        contract_duration_val = 30
        expiry_date_str = ""
        try:
            contract_duration_val = int(contract_days)
            try:
                start_date = datetime.strptime(execution_start, "%Y-%m-%d")
            except:
                start_date = datetime.strptime(formatted_start_date, "%B %d, %Y")
            expiry_date = start_date + timedelta(days=contract_duration_val)
            expiry_date_str = expiry_date.strftime("%B %d, %Y")
        except:
            pass
        
        # Create investor root folder if it doesn't exist
        inv_root = Path(INV_PATH) / inv_id
        try:
            inv_root.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"   Could not create investor folder: {e}")
            continue
        
        # NEW PATH: activities.json directly in investor root
        activities_path = inv_root / "activities.json"
        
        # Load existing activities.json if it exists
        existing_activities = {}
        is_new = False
        if activities_path.exists():
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    existing_activities = json.load(f)
                print(f"  📄 Existing activities.json found")
            except:
                existing_activities = {}
                is_new = True
        else:
            is_new = True
        
        # Prepare activities data (merge with existing)
        activities_data = DEFAULT_ACTIVITIES.copy()
        activities_data.update(existing_activities)
        
        # Update with new values
        changed = False
        
        # Update strategies list
        if activities_data.get("strategies") != strategy_names:
            activities_data["strategies"] = strategy_names
            changed = True
            print(f"  📋 Strategies: {', '.join(strategy_names)}")
        
        # Update execution_start_date
        if activities_data.get("execution_start_date") != formatted_start_date:
            activities_data["execution_start_date"] = formatted_start_date
            changed = True
        
        # Update contract_duration
        if activities_data.get("contract_duration") != contract_duration_val:
            activities_data["contract_duration"] = contract_duration_val
            changed = True
        
        # Update contract_expiry_date
        if activities_data.get("contract_expiry_date") != expiry_date_str:
            activities_data["contract_expiry_date"] = expiry_date_str
            changed = True
        
        # Update terminal_path if provided
        if terminal_path and activities_data.get("terminal_path") != terminal_path:
            activities_data["terminal_path"] = terminal_path
            changed = True
        
        # Ensure default values for other fields
        for field, default_value in DEFAULT_ACTIVITIES.items():
            if field not in activities_data or activities_data[field] is None:
                activities_data[field] = default_value
                changed = True
        
        # Save activities.json
        try:
            with open(activities_path, 'w', encoding='utf-8') as f:
                json.dump(activities_data, f, indent=4)
            
            if is_new:
                created_summary.append(inv_id)
                print(f"  ✅ Created new activities.json")
            elif changed:
                updated_summary.append(inv_id)
                print(f"  ✅ Updated existing activities.json")
            else:
                print(f"  ℹ️  No changes needed")
            
            processed_summary.append(inv_id)
            
        except Exception as e:
            print(f"   Failed to save activities.json: {e}")
    
    # ============================================
    # STEP 3: Summary
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
    
    print(f"\n🔹 STEP 2 - ACTIVITIES.JSON CREATION (NEW PATH):")
    print(f"   ✅ Created: {len(created_summary)} new activities.json files")
    if created_summary:
        for inv in created_summary[:5]:
            print(f"      • {inv}")
        if len(created_summary) > 5:
            print(f"      ... and {len(created_summary)-5} more")
    
    print(f"   🔄 Updated: {len(updated_summary)} existing activities.json files")
    if updated_summary:
        for inv in updated_summary[:3]:
            print(f"      • {inv}")
        if len(updated_summary) > 3:
            print(f"      ... and {len(updated_summary)-3} more")
    
    if skipped_summary:
        print(f"   ⏭️  Skipped (missing fields): {len(skipped_summary)}")
        for inv in skipped_summary[:3]:
            print(f"      • {inv}")
    
    print(f"\n🔹 STEP 3 - VERIFIED LIST:")
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
    investors_to_move_to_issues = []
    
    for inv_id, investor_data in verified_investors.items():
        print(f"\n📋 Checking investor: {inv_id}")
        print("-" * 40)
        
        # Check if investor folder exists at new path
        inv_folder = Path(INV_PATH) / inv_id
        
        if not inv_folder.exists():
            print(f"   Investor folder not found at: {inv_folder}")
            investors_to_remove.append(inv_id)
            
            # Add to issues_investors with reason
            if inv_id not in issues_investors:
                investor_data_copy = investor_data.copy()
                investor_data_copy['MESSAGE'] = f"Investor folder missing at {inv_folder}"
                investor_data_copy['verified_status'] = 'folder_missing'
                issues_investors[inv_id] = investor_data_copy
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
            print(f"  ⚠️  Missing required files: {', '.join(missing_files)}")
            
            # Check if investor is already in updated_investors or issues_investors
            if inv_id not in updated_investors and inv_id not in issues_investors:
                investor_data_copy = investor_data.copy()
                investor_data_copy['MESSAGE'] = f"Missing required files: {', '.join(missing_files)}"
                investor_data_copy['verified_status'] = 'missing_files'
                issues_investors[inv_id] = investor_data_copy
                print(f"  ⚠️  Added to issues_investors.json (missing files)")
                investors_to_remove.append(inv_id)
            else:
                print(f"  ℹ️  Investor already in updated/issues, skipping removal")
        else:
            print(f"  ✅ All required files present: {', '.join(required_files)}")
            
            # Check if activities.json has required data
            activities_path = inv_folder / "activities.json"
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    activities = json.load(f)
                
                # Validate critical fields
                execution_start_date = activities.get('execution_start_date')
                if not execution_start_date:
                    print(f"  ⚠️  Missing execution_start_date in activities.json")
                    
                    if inv_id not in updated_investors and inv_id not in issues_investors:
                        investor_data_copy = investor_data.copy()
                        investor_data_copy['MESSAGE'] = "Missing execution_start_date in activities.json"
                        investor_data_copy['verified_status'] = 'missing_start_date'
                        issues_investors[inv_id] = investor_data_copy
                        print(f"  ⚠️  Added to issues_investors.json (missing start date)")
                        investors_to_remove.append(inv_id)
                    continue
                
                # Check if tradeshistory.json has data
                tradeshistory_path = inv_folder / "tradeshistory.json"
                if tradeshistory_path.exists():
                    with open(tradeshistory_path, 'r', encoding='utf-8') as f:
                        tradeshistory = json.load(f)
                    
                    if tradeshistory:
                        print(f"  ✅ Found {len(tradeshistory)} authorized trades in tradeshistory.json")
                    else:
                        print(f"  ℹ️  tradeshistory.json is empty")
                
                # Remove MESSAGE field if it exists
                if 'MESSAGE' in investor_data:
                    print(f"  🧹 Removing MESSAGE field for investor {inv_id}")
                    del investor_data['MESSAGE']
                    updated = True
                
                # Add verification status
                investor_data['verified_status'] = 'verified'
                
            except Exception as e:
                print(f"   Error reading activities.json: {e}")
                if inv_id not in updated_investors and inv_id not in issues_investors:
                    investor_data_copy = investor_data.copy()
                    investor_data_copy['MESSAGE'] = f"Error reading activities.json: {str(e)}"
                    investor_data_copy['verified_status'] = 'read_error'
                    issues_investors[inv_id] = investor_data_copy
                    investors_to_remove.append(inv_id)
    
    # Remove investors from verified_investors that were moved to issues
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
            print(f"✅ Updated verified_investors.json - removed {len(investors_to_remove)} investors and cleaned MESSAGE fields")
        else:
            print(f"ℹ️  No changes made to verified_investors.json")
        
        if issues_investors:
            print(f"⚠️  Issues investors file updated with {len(issues_investors)} investors")
        print("="*80)
        
    except Exception as e:
        print(f" Error saving files: {e}")
        return False
    
    return True
#---

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

def symbols_grid_prices(inv_id=None):
    """
    Collect current prices for all symbols in symbols_dictionary from accountmanagement.json
    for  accounts. ASSUMES MT5 IS ALREADY INITIALIZED AND LOGGED IN.
    Can process a single investor or all investors when called from orchestrator.
    Saves all symbols' data to a single symbols_prices.json file with symbol names as top-level keys.
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the processing
    """
    print(f"\n{'='*10} 💰  SYMBOL PRICE COLLECTION {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols": 0,
        "successful_symbols": 0,
        "failed_symbols": 0,
        "total_categories": 0,
        "symbols_mapped": 0,
        "symbols_unchanged": 0,
        "signals_generated": False
    }

    def clean(s): 
        """Clean symbol string by removing special chars and converting to uppercase"""
        if s is None:
            return ""
        return str(s).replace(" ", "").replace("_", "").replace("/", "").replace(".", "").upper()
    
    def get_grid_configuration(config):
        """
        Extract grid prices setup and risk reward configuration from account management.
        
        Args:
            config: Loaded account management configuration
            
        Returns:
            tuple: (bid_order_type, ask_order_type, risk_reward_value)
        """
        grid_prices_setup = config.get("grid_prices_setup", {})
        selected_risk_reward = config.get("selected_risk_reward", [2])
        
        bid_order_type = grid_prices_setup.get("bid_prices_order_type", "buy_stop")
        ask_order_type = grid_prices_setup.get("ask_prices_order_type", "sell_stop")
        
        risk_reward_value = selected_risk_reward[0] if isinstance(selected_risk_reward, list) and selected_risk_reward else 2
        
        print(f"  📋 Grid Orders Configuration:")
        print(f"    • Bid Prices Order Type: {bid_order_type}")
        print(f"    • Ask Prices Order Type: {ask_order_type}")
        print(f"    • Selected Risk/Reward: {risk_reward_value}")
        
        return bid_order_type, ask_order_type, risk_reward_value
    
    def get_risk_requirement(config, account_balance):
        """
        Get risk requirement based on account balance from account_balance_default_risk_management.
        
        Args:
            config: Loaded account management configuration
            account_balance: Current account balance
            
        Returns:
            tuple: (target_risk_min, target_risk_max, target_risk_value)
        """
        risk_management = config.get("account_balance_default_risk_management", {})
        
        if not risk_management:
            print(f"        ⚠️  No risk management configuration found - using default 10 USD risk")
            return 10.0, 10.99, 10.0  # Default fallback
        
        # Find matching range for account balance
        target_risk_value = None
        target_risk_min = None
        target_risk_max = None
        
        for range_str, risk_value in risk_management.items():
            if "_risk" in range_str:
                # Parse range like "100-109.99_risk"
                range_part = range_str.replace("_risk", "")
                if "-" in range_part:
                    min_val, max_val = range_part.split("-")
                    try:
                        min_balance = float(min_val)
                        max_balance = float(max_val)
                        
                        if min_balance <= account_balance <= max_balance:
                            target_risk_value = float(risk_value)
                            target_risk_min = float(risk_value)
                            target_risk_max = float(risk_value) + 0.99  # Add 99 cents tolerance
                            break
                    except ValueError:
                        continue
        
        if target_risk_value is None:
            print(f"        ⚠️  No matching risk range found for balance {account_balance} - using default")
            return 10.0, 10.99, 10.0  # Default fallback
        
        print(f"        📊 Risk requirement from account management:")
        print(f"          • Account balance: ${account_balance:.2f}")
        print(f"          • Target risk: ${target_risk_min:.2f} - ${target_risk_max:.2f}")
        print(f"          • Base risk value: {target_risk_value}")
        
        return target_risk_min, target_risk_max, target_risk_value
    
    def fetch_current_prices(symbol, resolution_cache):
        """
        Fetch current bid/ask prices for a symbol using external helper for normalization
        and cache for efficiency - following same pattern as populate_orders_missing_fields.
        ASSUMES MT5 IS ALREADY INITIALIZED AND LOGGED IN.
        
        Args:
            symbol: Raw symbol to fetch prices for
            resolution_cache: Cache dictionary for symbol resolution
            
        Returns:
            tuple: (success, normalized_symbol, current_bid, current_ask, current_price, tick, symbol_info, error_message)
        """
        try:
            # Check Cache First - exactly like populate_orders_missing_fields
            if symbol in resolution_cache:
                res = resolution_cache[symbol]
                normalized_symbol = res['broker_sym']
                symbol_info = res['info']
                
                # If we have cached info but it's None (previously failed), return error
                if symbol_info is None:
                    return False, None, None, None, None, None, None, f"Symbol '{symbol}' previously failed to resolve"
            else:
                # Perform mapping only once using external helper
                normalized_symbol = get_normalized_symbol(symbol)
                
                # Get symbol info from MT5 to verify it exists
                symbol_info = mt5.symbol_info(normalized_symbol)
                
                # Store in cache - exactly like populate_orders_missing_fields
                resolution_cache[symbol] = {'broker_sym': normalized_symbol, 'info': symbol_info}
                
                # Detailed log only on first discovery - matches populate function style
                if symbol_info:
                    if normalized_symbol != symbol:
                        print(f"    └─ ✅ {symbol} -> {normalized_symbol} (Mapped & Cached)")
                        stats["symbols_mapped"] += 1
                    else:
                        print(f"    └─ ✅ {symbol} (Direct match, cached)")
                        stats["symbols_unchanged"] += 1
                else:
                    print(f"    └─  MT5: '{normalized_symbol}' (from '{symbol}') not found in MarketWatch")
                    return False, None, None, None, None, None, None, f"Symbol '{normalized_symbol}' not found in MarketWatch"
            
            # If we get here, we have valid symbol_info from cache or fresh lookup
            if not symbol_info:
                return False, None, None, None, None, None, None, f"Symbol info not available for {normalized_symbol}"
            
            # Select symbol in Market Watch (required for tick data)
            if not mt5.symbol_select(normalized_symbol, True):
                return False, normalized_symbol, None, None, None, None, None, f"Failed to select symbol: {normalized_symbol}"
            
            # Get current tick for bid/ask
            tick = mt5.symbol_info_tick(normalized_symbol)
            if not tick:
                return False, normalized_symbol, None, None, None, None, None, "Tick data not available"
            
            # Get current prices
            current_bid = tick.bid
            current_ask = tick.ask
            current_price = (current_bid + current_ask) / 2  # Mid price
            
            digits = symbol_info.digits
            print(f"✅ {normalized_symbol} (Bid: {current_bid:.{digits}f}, Ask: {current_ask:.{digits}f})")
            
            return True, normalized_symbol, current_bid, current_ask, current_price, tick, symbol_info, None
            
        except Exception as e:
            return False, None, None, None, None, None, None, str(e)
    
    def get_min_volume(symbol_info):
        """
        Get the minimum allowed volume for a symbol from the broker.
        
        Args:
            symbol_info: MT5 symbol info object
            
        Returns:
            tuple: (volume_min, volume_step)
        """
        try:
            # Get volume limits from symbol info
            volume_min = symbol_info.volume_min
            volume_step = symbol_info.volume_step
            
            print(f"        📊 Live volume information:")
            print(f"          • Minimum volume: {volume_min}")
            print(f"          • Volume step: {volume_step}")
            print(f"          • Maximum volume: {symbol_info.volume_max}")
            print(f"          • Volume limit: {symbol_info.volume_limit}")
            
            return volume_min, volume_step
            
        except Exception as e:
            print(f"        ⚠️  Could not get minimum volume: {e}")
            return 0.01, 0.01  # Default to 0.01 lots as fallback
    
    def calculate_risk_in_usd(symbol_info, entry_price, exit_price, volume, account_currency):
        """
        Calculate the risk amount in USD for a given trade level.
        
        Args:
            symbol_info: MT5 symbol info object
            entry_price: Entry price for the trade
            exit_price: Exit price (stop loss) for the trade
            volume: Trade volume in lots
            account_currency: Account currency (e.g., 'USD', 'EUR')
            
        Returns:
            float: Risk amount in USD
        """
        try:
            # Calculate risk distance in price points
            risk_distance_points = abs(exit_price - entry_price)
            
            # Get tick information
            tick_size = symbol_info.trade_tick_size
            tick_value = symbol_info.trade_tick_value  # Value per tick in account currency
            
            # Calculate risk in account currency
            # Formula: (risk_distance_points / tick_size) * tick_value * volume
            risk_in_account_currency = (risk_distance_points / tick_size) * tick_value * volume
            
            # Convert to USD if account currency is not USD
            if account_currency != 'USD':
                # Get USD conversion rate
                usd_symbol = f"{account_currency}USD"
                if mt5.symbol_select(usd_symbol, True):
                    usd_tick = mt5.symbol_info_tick(usd_symbol)
                    if usd_tick:
                        conversion_rate = usd_tick.bid or 1.0
                        risk_in_usd = risk_in_account_currency * conversion_rate
                    else:
                        risk_in_usd = risk_in_account_currency  # Fallback
                else:
                    # Try inverse pair
                    usd_symbol = f"USD{account_currency}"
                    if mt5.symbol_select(usd_symbol, True):
                        usd_tick = mt5.symbol_info_tick(usd_symbol)
                        if usd_tick:
                            conversion_rate = 1.0 / usd_tick.ask if usd_tick.ask > 0 else 1.0
                            risk_in_usd = risk_in_account_currency * conversion_rate
                        else:
                            risk_in_usd = risk_in_account_currency  # Fallback
                    else:
                        risk_in_usd = risk_in_account_currency  # Fallback
            else:
                risk_in_usd = risk_in_account_currency
            
            return risk_in_usd
            
        except Exception as e:
            print(f"        ⚠️  Could not calculate risk in USD: {e}")
            return 0.0  # Return 0 as fallback
    
    def scale_volume_to_target_risk(symbol_info, entry_price, exit_price, min_volume, volume_step, 
                                   account_currency, target_risk_min, target_risk_max, max_iterations=100):
        """
        Scale volume to achieve target risk range.
        
        Args:
            symbol_info: MT5 symbol info object
            entry_price: Entry price for the trade
            exit_price: Exit price for the trade
            min_volume: Minimum allowed volume
            volume_step: Volume step increment
            account_currency: Account currency
            target_risk_min: Minimum target risk in USD
            target_risk_max: Maximum target risk in USD
            max_iterations: Maximum scaling iterations
            
        Returns:
            tuple: (optimal_volume, actual_risk_usd, scaling_attempts)
        """
        print(f"        📊 Scaling volume to match target risk range:")
        print(f"          • Target risk range: ${target_risk_min:.2f} - ${target_risk_max:.2f}")
        print(f"          • Starting volume: {min_volume}")
        
        current_volume = min_volume
        best_volume = min_volume
        best_risk = 0
        scaling_attempts = []
        
        for iteration in range(max_iterations):
            # Calculate risk for current volume
            current_risk = calculate_risk_in_usd(
                symbol_info, entry_price, exit_price, current_volume, account_currency
            )
            
            scaling_attempts.append({
                "volume": current_volume,
                "risk_usd": round(current_risk, 2)
            })
            
            print(f"          • Attempt {iteration + 1}: Volume={current_volume}, Risk=${current_risk:.2f}")
            
            # Check if we're within target range
            if target_risk_min <= current_risk <= target_risk_max:
                print(f"          ✅ Target risk achieved with volume {current_volume} (${current_risk:.2f})")
                return current_volume, round(current_risk, 2), scaling_attempts
            
            # If we've exceeded the target range
            elif current_risk > target_risk_max:
                if iteration == 0:
                    # Even minimum volume exceeds target, use minimum
                    print(f"          ⚠️  Minimum volume already exceeds target range")
                    return min_volume, round(current_risk, 2), scaling_attempts
                else:
                    # Previous volume was below target, use that
                    print(f"          ✅ Using previous volume {best_volume} (${best_risk:.2f}) - below target")
                    return best_volume, round(best_risk, 2), scaling_attempts
            
            # If we're below target range, increase volume
            else:
                best_volume = current_volume
                best_risk = current_risk
                current_volume = round(current_volume + volume_step, 2)  # Round to 2 decimals for volume
        
        # If we've reached max iterations without hitting target, use the best volume found
        print(f"          ⚠️  Max iterations reached. Using best volume {best_volume} (${best_risk:.2f})")
        return best_volume, round(best_risk, 2), scaling_attempts
    
    def get_significant_digits(price):
        """
        Determine the number of significant digits to use for pattern generation.
        """
        # Convert price to string to analyze digits
        price_str = f"{price:.10f}".rstrip('0')  # Remove trailing zeros
        
        # Split into integer and fractional parts
        if '.' in price_str:
            integer_part, fractional_part = price_str.split('.')
        else:
            integer_part, fractional_part = price_str, ''
        
        # Count digits after decimal
        digits_after = len(fractional_part)
        
        print(f"        📊 Digit Analysis:")
        print(f"          • Price: {price}")
        print(f"          • Integer part: {integer_part} ({len(integer_part)} digits)")
        print(f"          • Fractional part: {fractional_part} ({digits_after} digits)")
        
        # If we have 4 or more digits after decimal, use those
        if digits_after >= 4:
            print(f"          • Using fractional digits (4+ digits after decimal)")
            return digits_after, False, 10 ** digits_after
        
        # Otherwise, check digits before decimal
        digits_before = len(integer_part)
        if digits_before >= 3:
            print(f"          • Using integer digits (4+ digits before decimal)")
            return digits_before, True, 1  # No multiplier needed for integer part
        
        # If neither has 4+ digits, we can't generate meaningful patterns
        print(f"          ⚠️  Insufficient digits for pattern generation (<4 before and after decimal)")
        return 0, False, 1
    
    def generate_pattern_levels(price, direction='below', num_levels=10, price_digits=None):
        """
        Generate price levels ending with 000, 250, 500, 750 patterns.
        """
        # Determine how to handle this price
        sig_digits, use_integer_part, multiplier = get_significant_digits(price)
        
        if sig_digits < 3:
            print(f"        ⚠️  Cannot generate pattern levels - insufficient significant digits")
            return []
        
        if use_integer_part:
            # Handle integer-based prices (like Gold: 1950, 1925, 1900)
            # Get the integer part
            price_int = int(price)
            
            # Round to nearest 250 in the integer
            base_int = (price_int // 250) * 250
            
            print(f"        📊 Generating integer-based patterns:")
            print(f"          • Original price: {price}")
            print(f"          • Integer value: {price_int}")
            print(f"          • Base integer: {base_int}")
            print(f"          • Pattern unit: 250")
            
            pattern_levels = []
            if direction == 'below':
                for i in range(num_levels):
                    level_int = base_int - (i * 250)
                    pattern_levels.append(float(level_int))
            else:  # above
                for i in range(num_levels):
                    level_int = base_int + ((i + 1) * 250)
                    pattern_levels.append(float(level_int))
            
            return pattern_levels
            
        else:
            # Handle fractional-based prices (like Forex: 1.12345)
            # For fractional part, we want patterns in the last 3-4 digits
            
            # Get the fractional part scaled to integer
            price_int = int(round(price * multiplier))
            base_int = (price_int // 250) * 250  # Round to nearest 250 in the last digits
            
            print(f"        📊 Generating fractional-based patterns:")
            print(f"          • Original price: {price}")
            print(f"          • Scaled integer: {price_int}")
            print(f"          • Base scaled: {base_int}")
            print(f"          • Multiplier: {multiplier}")
            print(f"          • Pattern unit: 250")
            
            pattern_levels = []
            if direction == 'below':
                for i in range(num_levels):
                    level_int = base_int - (i * 250)
                    level_price = level_int / multiplier
                    pattern_levels.append(level_price)
            else:  # above
                for i in range(num_levels):
                    level_int = base_int + ((i + 1) * 250)
                    level_price = level_int / multiplier
                    pattern_levels.append(level_price)
            
            return pattern_levels
    
    def generate_grid_levels(symbol, current_bid, current_ask, symbol_info, digits):
        """
        Generate synthetic price levels for grid trading.
        """
        print(f"\n      📊 Generating synthetic price levels:")
        
        # Calculate level increment based on symbol type
        if digits == 5 or digits == 3:  # 5-digit forex or 3-digit metals
            level_increment = 0.00025  # 2.5 pips = 0.00025 for 5-digit
        elif digits == 4 or digits == 2:  # 4-digit forex or 2-digit metals
            level_increment = 0.0025  # 2.5 pips = 0.0025 for 4-digit
        else:
            # For indices, crypto, etc. - use relative increment
            level_increment = symbol_info.point * 250  # 250 points
        
        print(f"        • Price precision: {digits} digits")
        print(f"        • Level increment: {level_increment}")
        
        # Generate levels for BID (SELL) - going DOWN
        bid_floor = current_bid - (current_bid % level_increment)  # Round down to nearest level
        
        bid_levels_below = []
        for i in range(1, 11):  # Generate 10 levels below
            level_price = bid_floor - (i * level_increment)
            level_price = round(level_price, digits)
            bid_levels_below.append(level_price)
        
        # Generate levels for ASK (BUY) - going UP
        ask_ceil = current_ask + (level_increment - (current_ask % level_increment)) % level_increment
        
        ask_levels_above = []
        for i in range(0, 10):  # Generate 10 levels above
            level_price = ask_ceil + (i * level_increment)
            level_price = round(level_price, digits)
            ask_levels_above.append(level_price)
        
        print(f"        • Current BID: {current_bid:.{digits}f} (SELL)")
        print(f"        • Closest level below: {bid_levels_below[0]:.{digits}f}")
        print(f"        • Generated {len(bid_levels_below)} SELL levels below")
        print(f"        • Current ASK: {current_ask:.{digits}f} (BUY)")
        print(f"        • Closest level above: {ask_levels_above[0]:.{digits}f}")
        print(f"        • Generated {len(ask_levels_above)} BUY levels above")
        
        # Generate pattern-based levels with enhanced logic
        print(f"\n      📊 Generating pattern-based levels (ending in 000, 250, 500, 750):")
        bid_pattern_levels = generate_pattern_levels(current_bid, 'below', 10, digits)
        ask_pattern_levels = generate_pattern_levels(current_ask, 'above', 10, digits)
        
        if bid_pattern_levels:
            print(f"        SELL levels below BID {current_bid:.{digits}f}:")
            for i, level in enumerate(bid_pattern_levels[:5]):
                if level >= 1000:
                    print(f"          {i+1}. {level:.{digits if digits < 3 else 2}f}")
                else:
                    print(f"          {i+1}. {level:.{digits}f}")
        
        if ask_pattern_levels:
            print(f"        BUY levels above ASK {current_ask:.{digits}f}:")
            for i, level in enumerate(ask_pattern_levels[:5]):
                if level >= 1000:
                    print(f"          {i+1}. {level:.{digits if digits < 3 else 2}f}")
                else:
                    print(f"          {i+1}. {level:.{digits}f}")
        
        return bid_levels_below, ask_levels_above, level_increment, bid_pattern_levels, ask_pattern_levels
    
    def generate_exit_and_tp_prices(entry_price, order_type, risk_reward, price_digits):
        """
        Generate exit and TP prices based on order type and risk/reward ratio.
        """
        # Determine pattern unit based on price magnitude
        if entry_price >= 1000:  # Integer-based (Gold, indices)
            pattern_unit = 250
            entry_scaled = int(entry_price)
            base_scaled = (entry_scaled // pattern_unit) * pattern_unit
            
            if order_type in ["sell_stop", "sell_limit"]:
                # Exit is next pattern level up
                exit_scaled = base_scaled + pattern_unit
                exit_price = float(exit_scaled)
                
                risk_distance = exit_price - entry_price
                tp_price = entry_price - (risk_distance * risk_reward)
                
            elif order_type in ["buy_stop", "buy_limit"]:
                # Exit is next pattern level down
                exit_scaled = base_scaled - pattern_unit
                exit_price = float(exit_scaled)
                
                risk_distance = entry_price - exit_price
                tp_price = entry_price + (risk_distance * risk_reward)
                
            else:
                exit_price = entry_price
                tp_price = entry_price
                
        else:  # Fractional-based (Forex)
            # Determine appropriate multiplier based on price
            if price_digits >= 5:  # 5-digit forex
                multiplier = 100000
                pattern_unit = 250  # Represents 0.00250 in price terms
            elif price_digits == 4:  # 4-digit forex
                multiplier = 10000
                pattern_unit = 25   # Represents 0.0025 in price terms
            else:
                multiplier = 10000
                pattern_unit = 250
            
            entry_scaled = int(round(entry_price * multiplier))
            base_scaled = (entry_scaled // pattern_unit) * pattern_unit
            
            if order_type in ["sell_stop", "sell_limit"]:
                # Exit is next pattern level up
                exit_scaled = base_scaled + pattern_unit
                exit_price = exit_scaled / multiplier
                
                risk_distance = exit_price - entry_price
                tp_price = entry_price - (risk_distance * risk_reward)
                
            elif order_type in ["buy_stop", "buy_limit"]:
                # Exit is next pattern level down
                exit_scaled = base_scaled - pattern_unit
                exit_price = exit_scaled / multiplier
                
                risk_distance = entry_price - exit_price
                tp_price = entry_price + (risk_distance * risk_reward)
                
            else:
                exit_price = entry_price
                tp_price = entry_price
        
        # Round appropriately
        if entry_price >= 1000:
            exit_price = round(exit_price, 2)
            tp_price = round(tp_price, 2)
        else:
            exit_price = round(exit_price, price_digits)
            tp_price = round(tp_price, price_digits)
        
        return exit_price, tp_price
    
    def invert_order_type(order_type):
        """
        Invert order type (buy <-> sell, stop/limit preserved).
        """
        order_type_map = {
            "buy_stop": "sell_stop",
            "sell_stop": "buy_stop",
            "buy_limit": "sell_limit",
            "sell_limit": "buy_limit"
        }
        return order_type_map.get(order_type, order_type)
    
    def calculate_counter_tp(entry_price, exit_price, order_type, risk_reward, price_digits):
        """
        Calculate TP for counter order based on inverted position.
        """
        # Calculate risk distance in points
        if order_type in ["sell_stop", "sell_limit"]:
            # For SELL orders: TP is below entry (entry - risk_distance * risk_reward)
            risk_distance = exit_price - entry_price  # exit > entry for sell orders
            tp_price = entry_price - (risk_distance * risk_reward)
            
        elif order_type in ["buy_stop", "buy_limit"]:
            # For BUY orders: TP is above entry (entry + risk_distance * risk_reward)
            risk_distance = entry_price - exit_price  # entry > exit for buy orders
            tp_price = entry_price + (risk_distance * risk_reward)
            
        else:
            tp_price = entry_price
        
        # Determine rounding based on price magnitude
        if entry_price >= 1000:  # Integer-based (Gold, indices)
            tp_price = round(tp_price, 2)
        else:  # Fractional-based (Forex)
            tp_price = round(tp_price, price_digits)
        
        return tp_price
    
    def generate_order_counter(level_data, price_digits):
        """
        Generate a counter order for a given grid level.
        
        Args:
            level_data: Original level data
            price_digits: Number of decimal places for price
        """
        # Invert the order type
        original_order_type = level_data.get("order_type", "")
        inverted_order_type = invert_order_type(original_order_type)
        
        # Entry becomes original exit
        counter_entry = level_data.get("exit")
        
        # Exit becomes original entry
        counter_exit = level_data.get("entry")
        
        # Calculate counter TP based on inverted order type
        risk_reward = level_data.get("risk_reward", 1)
        counter_tp = calculate_counter_tp(
            counter_entry, counter_exit, inverted_order_type, risk_reward, price_digits
        )
        
        # Initialize counter order structure
        counter_order = {
            "entry": counter_entry,
            "exit": counter_exit,
            "tp": counter_tp,
            "volume": level_data.get("volume"),
            "risk_in_usd": level_data.get("risk_in_usd"),
            "min_volume_risk": level_data.get("min_volume_risk"),
            "order_type": inverted_order_type,
            "risk_reward": risk_reward
        }
        
        # Include scaling attempts if they exist in original
        if "scaling_attempts" in level_data:
            counter_order["scaling_attempts"] = level_data["scaling_attempts"]
        
        return counter_order
    
    def add_order_counters_to_grid_levels(grid_bid_levels, grid_ask_levels, digits):
        """
        Add order counters to both bid and ask grid levels.
        
        Args:
            grid_bid_levels: List of bid/sell levels
            grid_ask_levels: List of ask/buy levels
            digits: Price digits
        """
        print(f"\n      📊 GENERATING ORDER COUNTERS:")
        
        # Add counters to bid levels (sell orders)
        for level in grid_bid_levels:
            level["order_counter"] = generate_order_counter(level, digits)
        
        # Add counters to ask levels (buy orders)
        for level in grid_ask_levels:
            level["order_counter"] = generate_order_counter(level, digits)
        
        print(f"        • Added counters to {len(grid_bid_levels)} sell levels")
        print(f"        • Added counters to {len(grid_ask_levels)} buy levels")
        
        return grid_bid_levels, grid_ask_levels

    def create_grid_orders_structure(bid_pattern_levels, ask_pattern_levels, bid_order_type, ask_order_type, 
                                    risk_reward_value, digits, min_volume, volume_step, symbol_info, 
                                    account_currency, target_risk_min, target_risk_max):
        """
        Create enhanced grid orders structure with order types, risk/reward,
        exit/TP prices, live broker volume, USD risk calculation, volume scaling to target risk,
        order counters, and selection flags for oldest bid and youngest ask.
        
        Args:
            bid_pattern_levels: List of bid pattern levels
            ask_pattern_levels: List of ask pattern levels
            bid_order_type: Order type for bid prices
            ask_order_type: Order type for ask prices
            risk_reward_value: Risk/reward ratio
            digits: Price digits
            min_volume: Minimum volume
            volume_step: Volume step
            symbol_info: MT5 symbol info
            account_currency: Account currency
            target_risk_min: Minimum target risk
            target_risk_max: Maximum target risk
        """
        print(f"\n      📊 Creating grid orders structure with exit/TP prices:")
        print(f"      📊 Adding live broker volume: min={min_volume}, step={volume_step}")
        print(f"      📊 Calculating risk in USD for each level")
        print(f"      📊 Target risk range: ${target_risk_min:.2f} - ${target_risk_max:.2f}")
        print(f"      📊 Scaling volume to match target risk")
        
        # Create enhanced sell levels (below bid) - these use ask_prices_order_type (sell_stop)
        grid_bid_levels = []
        for level_price in bid_pattern_levels:
            # Generate exit and TP prices based on order type and risk/reward
            exit_price, tp_price = generate_exit_and_tp_prices(
                level_price, ask_order_type, risk_reward_value, digits
            )
            
            # Scale volume to achieve target risk range
            optimal_volume, actual_risk, scaling_attempts = scale_volume_to_target_risk(
                symbol_info, level_price, exit_price, min_volume, volume_step,
                account_currency, target_risk_min, target_risk_max
            )
            
            grid_bid_levels.append({
                "entry": level_price,
                "exit": exit_price,
                "tp": tp_price,
                "volume": optimal_volume,  # Scaled volume to match risk target
                "risk_in_usd": actual_risk,  # Actual risk amount in USD after scaling
                "min_volume_risk": scaling_attempts[0]["risk_usd"] if scaling_attempts else 0,  # Risk at min volume
                "scaling_attempts": scaling_attempts,  # All scaling attempts for transparency
                "order_type": ask_order_type,
                "risk_reward": risk_reward_value
            })
        
        # Create enhanced buy levels (above ask) - these use bid_prices_order_type (buy_stop)
        grid_ask_levels = []
        for level_price in ask_pattern_levels:
            # Generate exit and TP prices based on order type and risk/reward
            exit_price, tp_price = generate_exit_and_tp_prices(
                level_price, bid_order_type, risk_reward_value, digits
            )
            
            # Scale volume to achieve target risk range
            optimal_volume, actual_risk, scaling_attempts = scale_volume_to_target_risk(
                symbol_info, level_price, exit_price, min_volume, volume_step,
                account_currency, target_risk_min, target_risk_max
            )
            
            grid_ask_levels.append({
                "entry": level_price,
                "exit": exit_price,
                "tp": tp_price,
                "volume": optimal_volume,  # Scaled volume to match risk target
                "risk_in_usd": actual_risk,  # Actual risk amount in USD after scaling
                "min_volume_risk": scaling_attempts[0]["risk_usd"] if scaling_attempts else 0,  # Risk at min volume
                "scaling_attempts": scaling_attempts,  # All scaling attempts for transparency
                "order_type": bid_order_type,
                "risk_reward": risk_reward_value
            })
        
        # ADD SELECTION FLAGS - Simple logic: highest entry price for bid, lowest entry price for ask
        if grid_bid_levels:
            # Find the bid level with the highest entry price (oldest/nearest to current price)
            max_entry_bid = max(level["entry"] for level in grid_bid_levels)
            for level in grid_bid_levels:
                if level["entry"] == max_entry_bid:
                    level["selected_bid"] = True
                    print(f"        • Selected BID level at {max_entry_bid:.{digits}f} as oldest (highest entry)")
                else:
                    level["selected_bid"] = False
        
        if grid_ask_levels:
            # Find the ask level with the lowest entry price (youngest/nearest to current price)
            min_entry_ask = min(level["entry"] for level in grid_ask_levels)
            for level in grid_ask_levels:
                if level["entry"] == min_entry_ask:
                    level["selected_ask"] = True
                    print(f"        • Selected ASK level at {min_entry_ask:.{digits}f} as youngest (lowest entry)")
                else:
                    level["selected_ask"] = False
        
        # ADD ORDER COUNTERS HERE
        grid_bid_levels, grid_ask_levels = add_order_counters_to_grid_levels(
            grid_bid_levels, grid_ask_levels, digits
        )
        
        print(f"        • Grid sell levels: {len(grid_bid_levels)} levels (with counters)")
        print(f"        • Grid buy levels: {len(grid_ask_levels)} levels (with counters)")
        print(f"        • Risk/Reward: {risk_reward_value}:1 for all levels")
        
        return grid_bid_levels, grid_ask_levels
   
    def save_individual_symbol_price(prices_dir, symbol, price_data):
        """Save individual symbol price data to JSON file (kept for backward compatibility)."""
        symbol_file = prices_dir / f"{symbol}.json"
        with open(symbol_file, 'w', encoding='utf-8') as f:
            json.dump(price_data, f, indent=4)
    
    def save_all_symbols_prices(prices_dir, all_symbols_price_data, acc_info, bid_order_type, ask_order_type, 
                                risk_reward_value, target_risk_range, total_categories, total_symbols, 
                                successful_symbols, failed_symbols, category_results):
        """
        Save all symbols' price data to a single symbols_prices.json file,
        with symbol names as top-level keys.
        
        Args:
            prices_dir: Directory to save the file
            all_symbols_price_data: Dictionary with symbol as key and price data as value
            acc_info: Account info object
            bid_order_type: Bid order type
            ask_order_type: Ask order type
            risk_reward_value: Risk reward value
            target_risk_range: Target risk range tuple
            total_categories: Total categories count
            total_symbols: Total symbols count
            successful_symbols: Successful symbols count
            failed_symbols: Failed symbols count
            category_results: Category results dictionary
        """
        symbols_file = prices_dir / "symbols_prices.json"
        
        # Create the final structure with metadata and all symbols at top level
        final_data = {
            "account_type": "",
            "account_login": acc_info.login,
            "account_server": acc_info.server,
            "account_balance": acc_info.balance,
            "account_currency": acc_info.currency,
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "grid_configuration": {
                "bid_order_type": bid_order_type,
                "ask_order_type": ask_order_type,
                "risk_reward": risk_reward_value
            },
            "target_risk_range_usd": {
                "min": target_risk_range[0],
                "max": target_risk_range[1],
                "base": target_risk_range[2]
            },
            "total_categories": total_categories,
            "total_symbols": total_symbols,
            "successful_symbols": successful_symbols,
            "failed_symbols": failed_symbols,
            "success_rate_percent": round((successful_symbols/total_symbols*100), 1) if total_symbols > 0 else 0,
            "categories": category_results,
            **all_symbols_price_data  # Unpack the symbols directly at top level
        }
        
        with open(symbols_file, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=4, default=str)
        
        print(f"    📁 Saved all symbol prices to: {symbols_file}")
        print(f"    📊 Total symbols in file: {len(all_symbols_price_data)}")
    
    def save_category_summary(prices_dir, category, symbols, category_price_data, 
                             category_symbols_success, category_symbols_failed, 
                             login_id, bid_order_type, ask_order_type, risk_reward_value, target_risk_range):
        """Save category summary with all symbols data."""
        category_file = prices_dir / f"{category}_prices.json"
        category_summary = {
            "category": category,
            "account_type": "",
            "account_login": login_id,
            "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_symbols": len(symbols),
            "successful_symbols": category_symbols_success,
            "failed_symbols": category_symbols_failed,
            "grid_configuration": {
                "bid_order_type": bid_order_type,
                "ask_order_type": ask_order_type,
                "risk_reward": risk_reward_value
            },
            "target_risk_range_usd": {
                "min": target_risk_range[0],
                "max": target_risk_range[1],
                "base": target_risk_range[2]
            },
            "symbols": category_price_data
        }
        with open(category_file, 'w', encoding='utf-8') as f:
            json.dump(category_summary, f, indent=4)
    
    def filter_signals_with_counters(prices_dir, category_price_data, symbols_dict, 
                                     target_risk_min, target_risk_max, 
                                     bid_order_type, ask_order_type, risk_reward_value, 
                                     account_balance, account_currency):
        """
        Filter orders that meet the risk requirement and save them to signals.json,
        including order counters for each filtered level.
        """
        print(f"\n      📊 FILTERING SIGNALS WITH COUNTERS - Risk Requirement: ≤ ${target_risk_max:.2f} AND > $0")
        
        signals_data = {
            "account_type": "",
            "account_balance": account_balance,
            "account_currency": account_currency,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "risk_requirement": {
                "min_usd": target_risk_min,
                "max_usd": target_risk_max,
                "filter_criteria": f"$0 < risk_in_usd ≤ ${target_risk_max:.2f}"
            },
            "grid_configuration": {
                "bid_order_type": bid_order_type,
                "ask_order_type": ask_order_type,
                "risk_reward": risk_reward_value
            },
            "categories": {},
            "summary": {
                "total_symbols_with_signals": 0,
                "total_bid_orders": 0,
                "total_ask_orders": 0,
                "total_counter_orders": 0,
                "total_orders": 0
            }
        }
        
        total_bid_orders = 0
        total_ask_orders = 0
        total_counter_orders = 0
        symbols_with_signals = 0
        
        # Process each category
        for category, symbols in symbols_dict.items():
            category_signals = {}
            category_bid_count = 0
            category_ask_count = 0
            category_counter_count = 0
            
            for raw_symbol in symbols:
                # Normalize symbol using external helper to match keys in category_price_data
                normalized_symbol = get_normalized_symbol(raw_symbol)
                if normalized_symbol in category_price_data:
                    price_data = category_price_data[normalized_symbol]
                    digits = price_data.get("digits", 5)
                    
                    # Filter bid levels (sell orders) - exclude zero risk
                    filtered_bid_levels = []
                    for level in price_data["grid_orders"]["bid_levels"]:
                        if level["risk_in_usd"] <= target_risk_max and level["risk_in_usd"] > 0:
                            # Include the counter order
                            filtered_level = {
                                "entry": level["entry"],
                                "exit": level["exit"],
                                "tp": level["tp"],
                                "volume": level["volume"],
                                "risk_in_usd": level["risk_in_usd"],
                                "min_volume_risk": level["min_volume_risk"],
                                "order_type": level["order_type"],
                                "risk_reward": level["risk_reward"],
                                "order_counter": level["order_counter"]  # Include the counter
                            }
                            filtered_bid_levels.append(filtered_level)
                    
                    # Filter ask levels (buy orders) - exclude zero risk
                    filtered_ask_levels = []
                    for level in price_data["grid_orders"]["ask_levels"]:
                        if level["risk_in_usd"] <= target_risk_max and level["risk_in_usd"] > 0:
                            # Include the counter order
                            filtered_level = {
                                "entry": level["entry"],
                                "exit": level["exit"],
                                "tp": level["tp"],
                                "volume": level["volume"],
                                "risk_in_usd": level["risk_in_usd"],
                                "min_volume_risk": level["min_volume_risk"],
                                "order_type": level["order_type"],
                                "risk_reward": level["risk_reward"],
                                "order_counter": level["order_counter"]  # Include the counter
                            }
                            filtered_ask_levels.append(filtered_level)
                    
                    # Only include symbol if it has any filtered orders
                    if filtered_bid_levels or filtered_ask_levels:
                        category_signals[raw_symbol] = {  # Use raw_symbol as key for signals
                            "digits": price_data["digits"],
                            "current_prices": price_data["current_prices"],
                            "bid_orders": filtered_bid_levels,
                            "ask_orders": filtered_ask_levels
                        }
                        
                        symbols_with_signals += 1
                        category_bid_count += len(filtered_bid_levels)
                        category_ask_count += len(filtered_ask_levels)
                        category_counter_count += len(filtered_bid_levels) + len(filtered_ask_levels)  # One counter per order
                        
                        total_bid_orders += len(filtered_bid_levels)
                        total_ask_orders += len(filtered_ask_levels)
                        total_counter_orders += len(filtered_bid_levels) + len(filtered_ask_levels)
            
            # Add category to signals if it has any symbols
            if category_signals:
                signals_data["categories"][category] = {
                    "symbols": category_signals,
                    "summary": {
                        "symbols_with_signals": len(category_signals),
                        "bid_orders": category_bid_count,
                        "ask_orders": category_ask_count,
                        "counter_orders": category_counter_count,
                        "total_orders": category_bid_count + category_ask_count + category_counter_count
                    }
                }
        
        # Update summary
        signals_data["summary"]["total_symbols_with_signals"] = symbols_with_signals
        signals_data["summary"]["total_bid_orders"] = total_bid_orders
        signals_data["summary"]["total_ask_orders"] = total_ask_orders
        signals_data["summary"]["total_counter_orders"] = total_counter_orders
        signals_data["summary"]["total_orders"] = total_bid_orders + total_ask_orders + total_counter_orders
        
        # Save signals.json
        signals_file = prices_dir / "signals.json"
        with open(signals_file, 'w', encoding='utf-8') as f:
            json.dump(signals_data, f, indent=4)
        
        print(f"      📊 SIGNALS WITH COUNTERS SUMMARY:")
        print(f"        • Symbols with signals: {symbols_with_signals}")
        print(f"        • Total bid orders: {total_bid_orders}")
        print(f"        • Total ask orders: {total_ask_orders}")
        print(f"        • Total counter orders: {total_counter_orders}")
        print(f"        • Total orders (including counters): {total_bid_orders + total_ask_orders + total_counter_orders}")
        print(f"        • Filter criteria: $0 < risk_in_usd ≤ ${target_risk_max:.2f}")
        print(f"        • Saved to: {signals_file}")
        
        # Update stats
        stats["signals_generated"] = True
        
        return signals_data
    
    def print_investor_summary(user_brokerid, total_categories, total_symbols, successful_symbols, 
                              failed_symbols, bid_order_type, ask_order_type, risk_reward_value, 
                              target_risk_range, prices_dir):
        """Print summary for an investor."""
        print(f"\n  📊  INVESTOR SUMMARY: {user_brokerid}")
        print(f"    • Categories processed: {total_categories}")
        print(f"    • Total symbols: {total_symbols}")
        print(f"    • Successful: {successful_symbols}")
        print(f"    • Failed: {failed_symbols}")
        print(f"    • Success rate: {(successful_symbols/total_symbols*100):.1f}%")
        print(f"    • Grid Configuration: Bid Type={bid_order_type}, Ask Type={ask_order_type}, R:R={risk_reward_value}")
        print(f"    • Target Risk Range: ${target_risk_range[0]:.2f} - ${target_risk_range[1]:.2f} USD")
        print(f"    • Price files saved to: {prices_dir}")
    
    # If inv_id is provided, process only that investor
    if inv_id:
        # Get broker config
        broker_cfg = usersdictionary.get(inv_id)
        if not broker_cfg:
            print(f" [{inv_id}]  No broker config found")
            return stats
        
        inv_root = Path(INV_PATH) / inv_id
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f" [{inv_id}] ⚠️ Account management file not found")
            return stats
        
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract symbols dictionary
            symbols_dict = config.get("symbols_dictionary", {})
            if not symbols_dict:
                print(f" [{inv_id}] ⚠️ No symbols_dictionary found")
                return stats
            
            # Get grid configuration
            bid_order_type, ask_order_type, risk_reward_value = get_grid_configuration(config)
            
            # Get account info (already logged in from orchestrator)
            acc_info = mt5.account_info()
            if not acc_info:
                print(f" [{inv_id}]  Failed to get account info - MT5 not initialized?")
                return stats
            
            # Get account currency and balance
            account_currency = acc_info.currency
            account_balance = acc_info.balance
            
            # Get risk requirement based on account balance
            target_risk_min, target_risk_max, target_risk_base = get_risk_requirement(config, account_balance)
            target_risk_range = (target_risk_min, target_risk_max, target_risk_base)
            
            print(f"\n  📊  Account Details:")
            print(f"    • Balance: ${account_balance:,.2f}")
            print(f"    • Equity: ${acc_info.equity:,.2f}")
            print(f"    • Server: {acc_info.server}")
            print(f"    • Currency: {account_currency}")
            print(f"    • Target Risk Range: ${target_risk_min:.2f} - ${target_risk_max:.2f}")
            
            # Get terminal info to check connection status
            term_info = mt5.terminal_info()
            if term_info:
                print(f"    • Connected: {'✅' if term_info.connected else ''}")
                print(f"    • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else ' DISABLED'}")
            
            # Create prices directory
            prices_dir = inv_root / "prices"
            prices_dir.mkdir(exist_ok=True)
            print(f"\n  📁 Prices will be saved to: {prices_dir}")
            
            # Track statistics
            total_categories = len(symbols_dict)
            total_symbols = 0
            successful_symbols = 0
            failed_symbols = 0
            category_results = {}
            
            # Store all category price data for signals generation
            all_category_price_data = {}
            
            # NEW: Store all symbols price data for single file output
            all_symbols_price_data = {}
            
            # Symbol resolution cache - exactly like populate_orders_missing_fields
            resolution_cache = {}
            
            # Process each category in symbols_dictionary
            for category, symbols in symbols_dict.items():
                print(f"\n  📂 Category: {category.upper()} ({len(symbols)} symbols)")
                category_symbols_success = 0
                category_symbols_failed = 0
                category_price_data = {}
                
                for raw_symbol in symbols:
                    total_symbols += 1
                    print(f"    🔍 Processing: {raw_symbol}...", end=" ")
                    
                    # Fetch current prices with resolution cache - following populate function pattern
                    success, normalized_symbol, current_bid, current_ask, current_price, tick, symbol_info, error = fetch_current_prices(
                        raw_symbol, resolution_cache
                    )
                    
                    if not success:
                        print(f" {error[:50] if error else 'Unknown error'}")
                        failed_symbols += 1
                        category_symbols_failed += 1
                        continue
                    
                    # Get digits from symbol_info
                    digits = symbol_info.digits
                    
                    # Generate grid levels
                    bid_levels_below, ask_levels_above, level_increment, bid_pattern_levels, ask_pattern_levels = generate_grid_levels(
                        normalized_symbol, current_bid, current_ask, symbol_info, digits
                    )
                    
                    # Check if pattern levels were generated successfully
                    if not bid_pattern_levels or not ask_pattern_levels:
                        print(f"        ⚠️  Could not generate pattern levels for {normalized_symbol} - insufficient digits")
                        failed_symbols += 1
                        category_symbols_failed += 1
                        continue
                    
                    # Get minimum volume and volume step from broker
                    min_volume, volume_step = get_min_volume(symbol_info)
                    
                    # Create grid orders structure
                    grid_bid_levels, grid_ask_levels = create_grid_orders_structure(
                        bid_pattern_levels, ask_pattern_levels, bid_order_type, ask_order_type, 
                        risk_reward_value, digits, min_volume, volume_step, symbol_info, 
                        account_currency, target_risk_min, target_risk_max
                    )
                    
                    # Create complete price data structure
                    price_data = {
                        "original_symbol": raw_symbol,
                        "symbol": normalized_symbol,
                        "digits": digits,
                        "tick_size": symbol_info.trade_tick_size,
                        "tick_value": symbol_info.trade_tick_value,
                        "contract_size": symbol_info.trade_contract_size,
                        "account_type": "",
                        "account_login": int(broker_cfg['LOGIN_ID']),
                        "account_server": acc_info.server,
                        "account_balance": account_balance,
                        "account_currency": account_currency,
                        "category": category,
                        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        
                        # Current prices
                        "current_prices": {
                            "bid": current_bid,
                            "ask": current_ask,
                            "mid": current_price,
                            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        },
                        
                        # Generated levels
                        "generated_levels": {
                            "levels_below_bid": [round(price, digits) for price in bid_pattern_levels] if bid_pattern_levels else [],
                            "levels_above_ask": [round(price, digits) for price in ask_pattern_levels] if ask_pattern_levels else [],
                            "level_increment": level_increment,
                            "generation_method": "pattern_250_increment",
                            "patterns": ["000", "250", "500", "750"],
                            "price_type": "integer_based" if bid_pattern_levels and bid_pattern_levels[0] >= 1000 else "fractional_based"
                        },
                        
                        # Risk requirements
                        "risk_requirements": {
                            "target_risk_range_usd": {
                                "min": target_risk_min,
                                "max": target_risk_max,
                                "base": target_risk_base
                            },
                            "account_balance": account_balance,
                            "source_range": "account_balance_default_risk_management"
                        },
                        
                        # Grid orders structure with counters
                        "grid_orders": {
                            "bid_levels": grid_bid_levels,
                            "ask_levels": grid_ask_levels,
                            "configuration": {
                                "bid_order_type": bid_order_type,
                                "ask_order_type": ask_order_type,
                                "risk_reward": risk_reward_value,
                                "level_increment": level_increment,
                                "min_volume": min_volume,
                                "volume_step": volume_step,
                                "account_currency": account_currency,
                                "target_risk_range_usd": {
                                    "min": target_risk_min,
                                    "max": target_risk_max
                                },
                                "source": "accountmanagement.json & live broker"
                            }
                        }
                    }
                    
                    # Store in category data using normalized symbol as key
                    category_price_data[normalized_symbol] = price_data
                    
                    # Store in all symbols data for single file output
                    all_symbols_price_data[normalized_symbol] = price_data
                    
                    # Save individual symbol price file (kept for backward compatibility)
                    save_individual_symbol_price(prices_dir, normalized_symbol, price_data)
                    
                    successful_symbols += 1
                    category_symbols_success += 1
                
                # Save category summary if we have data
                if category_price_data:
                    save_category_summary(
                        prices_dir, category, symbols, category_price_data, 
                        category_symbols_success, category_symbols_failed, 
                        int(broker_cfg['LOGIN_ID']), bid_order_type, ask_order_type, 
                        risk_reward_value, target_risk_range
                    )
                    
                    category_results[category] = {
                        "total": len(symbols),
                        "success": category_symbols_success,
                        "failed": category_symbols_failed
                    }
                    
                    print(f"    📁 Saved category file: {category}_prices.json ({category_symbols_success}/{len(symbols)} symbols)")
                    
                    # Add to all category data for signals
                    all_category_price_data.update(category_price_data)
            
            # Save master price file for this investor (single file with all symbols)
            if successful_symbols > 0:
                save_all_symbols_prices(
                    prices_dir, all_symbols_price_data, acc_info,
                    bid_order_type, ask_order_type, risk_reward_value, target_risk_range,
                    total_categories, total_symbols, successful_symbols, failed_symbols,
                    category_results
                )
                
                # Generate and save signals.json with filtered orders including counters
                filter_signals_with_counters(
                    prices_dir, all_category_price_data, symbols_dict, 
                    target_risk_min, target_risk_max, 
                    bid_order_type, ask_order_type, risk_reward_value,
                    account_balance, account_currency
                )
                
                print_investor_summary(
                    inv_id, total_categories, total_symbols, successful_symbols,
                    failed_symbols, bid_order_type, ask_order_type, risk_reward_value,
                    target_risk_range, prices_dir
                )
                
                # Update stats
                stats["total_symbols"] = total_symbols
                stats["successful_symbols"] = successful_symbols
                stats["failed_symbols"] = failed_symbols
                stats["total_categories"] = total_categories
            
        except Exception as e:
            print(f" [{inv_id}]  Error: {e}")
            import traceback
            traceback.print_exc()
            
    return stats

def filter_unauthorized_symbols(inv_id=None):
    """
    Verifies and filters risk entries based on allowed symbols defined in accountmanagement.json.
    Targets signals.json records and removes unauthorized symbol orders.
    Matches sanitized versions of symbols to handle broker suffixes (e.g., EURUSDm vs EURUSD).
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the filtering process
    """
    print(f"\n{'='*10} 🛡️ SYMBOL AUTHORIZATION FILTER - SIGNALS.JSON TARGET {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

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
        investor_ids = [inv_id] if os.path.isdir(os.path.join(INV_PATH, inv_id)) else []
    else:
        investor_ids = [f for f in os.listdir(INV_PATH) if os.path.isdir(os.path.join(INV_PATH, f))]
    
    total_investors_processed = 0
    total_investors_modified = 0
    total_symbols_removed = 0
    total_orders_removed = 0
    
    # Statistics dictionary for return
    filter_stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "investors_modified": 0,
        "total_symbols_removed": 0,
        "total_orders_removed": 0,
        "categories_removed": 0,
        "processing_success": False
    }

    for current_inv_id in investor_ids:
        print(f"\n [{current_inv_id}] 🔍 Verifying symbol permissions in signals.json...")
        inv_folder = Path(INV_PATH) / current_inv_id
        acc_mgmt_path = inv_folder / "accountmanagement.json"
        signals_path = inv_folder / "prices" / "signals.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue
            
        if not signals_path.exists():
            print(f"  └─ ⚠️  signals.json not found. Skipping.")
            continue

        try:
            # Load account management configuration
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Load signals.json
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            # Extract and sanitize the list of allowed symbols
            sym_dict = config.get("symbols_dictionary", {})
            allowed_sanitized = {sanitize(s) for sublist in sym_dict.values() for s in sublist}
            
            if not allowed_sanitized:
                print(f"  └─ 🔘 No symbols defined in dictionary. Skipping filter.")
                continue

            print(f"  └─ 📋 Allowed symbols (sanitized): {', '.join(sorted(allowed_sanitized))}")
            
            # Track removed orders
            investor_symbols_removed = 0
            investor_orders_removed = 0
            categories_modified = 0
            symbols_removed_list = []
            
            # Process each category in signals.json
            modified = False
            categories_to_remove = []
            
            for category, category_data in signals_data.get("categories", {}).items():
                symbols_in_category = category_data.get("symbols", {})
                original_symbol_count = len(symbols_in_category)
                
                # Filter symbols in this category
                filtered_symbols = {}
                symbols_removed_in_category = []
                
                for symbol, symbol_data in symbols_in_category.items():
                    # Check if symbol is allowed
                    if sanitize(symbol) in allowed_sanitized:
                        filtered_symbols[symbol] = symbol_data
                    else:
                        symbols_removed_in_category.append(symbol)
                        symbols_removed_list.append(f"{category}.{symbol}")
                        
                        # Count orders removed for this symbol
                        bid_orders = len(symbol_data.get("bid_orders", []))
                        ask_orders = len(symbol_data.get("ask_orders", []))
                        counter_orders = bid_orders + ask_orders  # One counter per order
                        investor_orders_removed += bid_orders + ask_orders + counter_orders
                
                # Update category data if symbols were removed
                if symbols_removed_in_category:
                    modified = True
                    categories_modified += 1
                    investor_symbols_removed += len(symbols_removed_in_category)
                    
                    if filtered_symbols:
                        # Update category with filtered symbols
                        signals_data["categories"][category]["symbols"] = filtered_symbols
                        
                        # Update category summary
                        signals_data["categories"][category]["summary"] = {
                            "symbols_with_signals": len(filtered_symbols),
                            "bid_orders": sum(len(s.get("bid_orders", [])) for s in filtered_symbols.values()),
                            "ask_orders": sum(len(s.get("ask_orders", [])) for s in filtered_symbols.values()),
                            "counter_orders": sum(len(s.get("bid_orders", [])) + len(s.get("ask_orders", [])) for s in filtered_symbols.values()),
                            "total_orders": sum(
                                len(s.get("bid_orders", [])) + len(s.get("ask_orders", [])) + 
                                (len(s.get("bid_orders", [])) + len(s.get("ask_orders", [])))  # counters
                                for s in filtered_symbols.values()
                            )
                        }
                        
                        print(f"    └─ 📌 Category '{category}': Removed {len(symbols_removed_in_category)} unauthorized symbols")
                        for sym in symbols_removed_in_category[:3]:  # Show first 3 removed
                            print(f"       • Removed: {sym}")
                        if len(symbols_removed_in_category) > 3:
                            print(f"       • ... and {len(symbols_removed_in_category) - 3} more")
                    else:
                        # No symbols left in this category, mark for removal
                        categories_to_remove.append(category)
                        print(f"    └─ 📌 Category '{category}': All symbols unauthorized - will remove category")
            
            # Remove empty categories
            for category in categories_to_remove:
                del signals_data["categories"][category]
                print(f"    └─ 🗑️ Removed empty category: {category}")
            
            # Update main summary if modifications were made
            if modified or categories_to_remove:
                # Recalculate global summary
                total_symbols = 0
                total_bid_orders = 0
                total_ask_orders = 0
                total_counter_orders = 0
                
                for cat_data in signals_data.get("categories", {}).values():
                    cat_symbols = cat_data.get("symbols", {})
                    total_symbols += len(cat_symbols)
                    
                    for sym_data in cat_symbols.values():
                        bid_count = len(sym_data.get("bid_orders", []))
                        ask_count = len(sym_data.get("ask_orders", []))
                        total_bid_orders += bid_count
                        total_ask_orders += ask_count
                        total_counter_orders += bid_count + ask_count
                
                signals_data["summary"] = {
                    "total_symbols_with_signals": total_symbols,
                    "total_bid_orders": total_bid_orders,
                    "total_ask_orders": total_ask_orders,
                    "total_counter_orders": total_counter_orders,
                    "total_orders": total_bid_orders + total_ask_orders + total_counter_orders
                }
                
                # Add filter metadata
                signals_data["filter_applied"] = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "unauthorized_symbols_removed": symbols_removed_list,
                    "total_orders_removed": investor_orders_removed,
                    "filter_type": "symbol_authorization"
                }
                
                # Save modified signals.json
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                
                total_investors_modified += 1
                total_symbols_removed += investor_symbols_removed
                total_orders_removed += investor_orders_removed
                
                print(f"\n  └─ 📊 FILTER SUMMARY for {current_inv_id}:")
                print(f"     • Categories modified: {categories_modified}")
                print(f"     • Categories removed: {len(categories_to_remove)}")
                print(f"     • Unauthorized symbols removed: {investor_symbols_removed}")
                print(f"     • Total orders purged: {investor_orders_removed}")
                print(f"     • Remaining symbols: {total_symbols}")
                print(f"     • Remaining orders (with counters): {total_bid_orders + total_ask_orders + total_counter_orders}")
                print(f"     • ✅ signals.json updated successfully")
            else:
                print(f"  └─ ✅ All symbols in signals.json are authorized. No changes needed.")

            total_investors_processed += 1

        except json.JSONDecodeError as e:
            print(f"  └─  Invalid JSON in {current_inv_id}: {e}")
        except Exception as e:
            print(f"  └─  Error processing {current_inv_id}: {e}")
            import traceback
            traceback.print_exc()

    # Update statistics
    filter_stats["investors_processed"] = total_investors_processed
    filter_stats["investors_modified"] = total_investors_modified
    filter_stats["total_symbols_removed"] = total_symbols_removed
    filter_stats["total_orders_removed"] = total_orders_removed
    filter_stats["processing_success"] = total_investors_processed > 0

    print(f"\n{'='*10} 🏁 FILTERING COMPLETE {'='*10}")
    print(f"   Processed: {total_investors_processed} investors")
    print(f"   Modified: {total_investors_modified} investors")
    print(f"   Total symbols removed: {total_symbols_removed}")
    print(f"   Total orders removed: {total_orders_removed}")
    print(f"{'='*10}\n")
    
    return filter_stats

def fetch_15m_candles(inv_id=None):
    """
    Fetch 15-minute candles (100 candles) for all symbols in symbols_dictionary.
    Uses same symbol normalization pattern as symbols_grid_prices.
    Saves all symbols' candle data to symbols_prices.json as [symbol]_tf_candles top-level keys.
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the candle fetching
    """
    print(f"\n{'='*10} 🕯️ FETCHING 15M CANDLES (100 CANDLES) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols": 0,
        "successful_symbols": 0,
        "failed_symbols": 0,
        "total_candles_fetched": 0,
        "symbols_mapped": 0,
        "symbols_unchanged": 0,
        "current_candle_forming": False  # Flag to mark current forming candle
    }

    def clean(s): 
        """Clean symbol string by removing special chars and converting to uppercase"""
        if s is None:
            return ""
        return str(s).replace(" ", "").replace("_", "").replace("/", "").replace(".", "").upper()
    
    def fetch_symbol_candles(symbol, resolution_cache):
        """
        Fetch 100 15-minute candles for a symbol using external helper for normalization
        and cache for efficiency. Numbers candles from 100 (current forming) down to 1 (oldest).
        
        Args:
            symbol: Raw symbol to fetch candles for
            resolution_cache: Cache dictionary for symbol resolution
            
        Returns:
            tuple: (success, normalized_symbol, candles_data, current_candle, error_message)
        """
        try:
            # Check Cache First - exactly like populate_orders_missing_fields
            if symbol in resolution_cache:
                res = resolution_cache[symbol]
                normalized_symbol = res['broker_sym']
                symbol_info = res['info']
                
                # If we have cached info but it's None (previously failed), return error
                if symbol_info is None:
                    return False, None, None, None, f"Symbol '{symbol}' previously failed to resolve"
            else:
                # Perform mapping only once using external helper
                normalized_symbol = get_normalized_symbol(symbol)
                
                # Get symbol info from MT5 to verify it exists
                symbol_info = mt5.symbol_info(normalized_symbol)
                
                # Store in cache - exactly like populate_orders_missing_fields
                resolution_cache[symbol] = {'broker_sym': normalized_symbol, 'info': symbol_info}
                
                # Detailed log only on first discovery
                if symbol_info:
                    if normalized_symbol != symbol:
                        print(f"    └─ ✅ {symbol} -> {normalized_symbol} (Mapped & Cached)")
                        stats["symbols_mapped"] += 1
                    else:
                        print(f"    └─ ✅ {symbol} (Direct match, cached)")
                        stats["symbols_unchanged"] += 1
                else:
                    print(f"    └─  MT5: '{normalized_symbol}' (from '{symbol}') not found in MarketWatch")
                    return False, None, None, None, f"Symbol '{normalized_symbol}' not found in MarketWatch"
            
            # If we get here, we have valid symbol_info from cache or fresh lookup
            if not symbol_info:
                return False, None, None, None, f"Symbol info not available for {normalized_symbol}"
            
            # Select symbol in Market Watch (required for data)
            if not mt5.symbol_select(normalized_symbol, True):
                return False, normalized_symbol, None, None, f"Failed to select symbol: {normalized_symbol}"
            
            # Define timeframe (15 minutes)
            timeframe = mt5.TIMEFRAME_M15
            
            # Get current time
            current_time = datetime.now()
            
            # Fetch 100 candles (plus 1 extra to account for current forming candle)
            rates = mt5.copy_rates_from(normalized_symbol, timeframe, current_time, 101)
            
            if rates is None or len(rates) == 0:
                return False, normalized_symbol, None, None, "No candle data available"
            
            # Separate completed candles and current forming candle
            completed_candles = rates[:-1] if len(rates) > 1 else []  # All except last
            current_candle = rates[-1] if len(rates) > 0 else None  # Last candle (may be forming)
            
            # Reverse the completed candles so newest is first (index 0)
            completed_candles_reversed = list(reversed(completed_candles))
            
            # Convert to list of dictionaries for JSON serialization with explicit type conversion
            # Number from 100 down to 1, where 100 is current forming (if exists)
            candles_list = []
            
            # Start numbering from 100 (if we have current forming candle)
            next_number = 100
            
            # Add current forming candle first (as number 100) if it exists
            if current_candle is not None:
                current_candle_dict = {
                    'candle_number': next_number,  # Always 100 for current forming
                    'time': int(current_candle[0]),  # Convert numpy.int64 to Python int
                    'time_str': datetime.fromtimestamp(int(current_candle[0])).strftime('%Y-%m-%d %H:%M:%S'),
                    'open': float(current_candle[1]),
                    'high': float(current_candle[2]),
                    'low': float(current_candle[3]),
                    'close': float(current_candle[4]),
                    'tick_volume': int(current_candle[5]),
                    'spread': int(current_candle[6]),
                    'real_volume': int(current_candle[7]) if current_candle[7] is not None else 0,
                    'is_forming': True  # Flag to indicate this is the current forming candle
                }
                
                # Log the current forming candle
                print(f"      🕯️ Current forming 15m candle (#{current_candle_dict['candle_number']}) starting at {current_candle_dict['time_str']}:")
                print(f"        Open: {current_candle_dict['open']:.{symbol_info.digits}f}, "
                      f"High: {current_candle_dict['high']:.{symbol_info.digits}f}, "
                      f"Low: {current_candle_dict['low']:.{symbol_info.digits}f}, "
                      f"Close: {current_candle_dict['close']:.{symbol_info.digits}f}")
                
                # Add current forming candle to list
                candles_list.append(current_candle_dict)
                next_number -= 1
            
            # Add completed candles in reverse order (newest to oldest)
            for i, rate in enumerate(completed_candles_reversed[:100]):  # Limit to 100 total candles
                # Convert numpy types to Python native types
                candle_dict = {
                    'candle_number': next_number - i,  # Decreasing numbers: 99, 98, 97...
                    'time': int(rate[0]),  # Convert numpy.int64 to Python int
                    'time_str': datetime.fromtimestamp(int(rate[0])).strftime('%Y-%m-%d %H:%M:%S'),
                    'open': float(rate[1]),  # Convert to float
                    'high': float(rate[2]),
                    'low': float(rate[3]),
                    'close': float(rate[4]),
                    'tick_volume': int(rate[5]),  # Convert to int
                    'spread': int(rate[6]),  # Convert to int
                    'real_volume': int(rate[7]) if rate[7] is not None else 0,  # Convert to int
                    'is_forming': False  # Completed candle
                }
                candles_list.append(candle_dict)
            
            # Ensure candles are sorted by number descending (highest first)
            candles_list.sort(key=lambda x: x['candle_number'], reverse=True)
            
            # Separate current forming candle for return value
            current_candle_dict = next((c for c in candles_list if c['is_forming']), None)
            
            # Prepare candles data structure for this symbol
            symbol_candles_data = {
                'symbol': normalized_symbol,
                'original_symbol': symbol,
                'timeframe': 'M15',
                'total_candles_fetched': len([c for c in candles_list if not c['is_forming']]),
                'has_current_forming': current_candle_dict is not None,
                'current_forming_candle': current_candle_dict,
                'candles': candles_list,  # Now includes numbered candles with current forming first
                'candle_numbering': {
                    'current_forming_number': 100,
                    'oldest_completed_number': 100 - len([c for c in candles_list if not c['is_forming']]),
                    'numbering_scheme': '100 (current forming) down to 1 (oldest)'
                },
                'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'digits': int(symbol_info.digits) if symbol_info.digits is not None else 5
            }
            
            return True, normalized_symbol, symbol_candles_data, current_candle_dict, None
            
        except Exception as e:
            return False, None, None, None, str(e)
    
    def save_candles_to_symbols_prices(prices_dir, all_symbols_candle_data, symbols_prices_data):
        """
        Save candle data to symbols_prices.json as [symbol]_tf_candles top-level keys.
        
        Args:
            prices_dir: Directory containing the files
            all_symbols_candle_data: Dictionary with symbol as key and candle data as value
            symbols_prices_data: Existing symbols_prices.json data
        """
        symbols_prices_path = prices_dir / "symbols_prices.json"
        
        # Add each symbol's candle data as a top-level key
        candles_added = 0
        for symbol, candle_data in all_symbols_candle_data.items():
            candle_key = f"{symbol}_tf_candles"
            symbols_prices_data[candle_key] = candle_data
            candles_added += 1
        
        # Add metadata about candle fetching
        symbols_prices_data['candles_fetch_metadata'] = {
            'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'timeframe': 'M15',
            'total_symbols_fetched': len(all_symbols_candle_data),
            'current_candle_forming': any(data.get('has_current_forming', False) for data in all_symbols_candle_data.values()),
            'note': 'Candle data stored as [symbol]_tf_candles top-level keys'
        }
        
        # Save back to file
        with open(symbols_prices_path, 'w', encoding='utf-8') as f:
            json.dump(symbols_prices_data, f, indent=4, default=str)
        
        print(f"    📁 Saved candle data to symbols_prices.json as {candles_added} [symbol]_tf_candles entries")
    
    # If inv_id is provided, process only that investor
    if inv_id:
        # Get broker config
        broker_cfg = usersdictionary.get(inv_id)
        if not broker_cfg:
            print(f" [{inv_id}]  No broker config found")
            return stats
        
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        prices_dir.mkdir(exist_ok=True)
        
        # Path to symbols_prices.json
        symbols_prices_path = prices_dir / "symbols_prices.json"
        
        # Check if symbols_prices.json exists, if not create basic structure
        if symbols_prices_path.exists():
            print(f" [{inv_id}] 📂 Loading existing symbols_prices.json...")
            with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                symbols_prices_data = json.load(f)
        else:
            print(f" [{inv_id}] ⚠️ symbols_prices.json not found, creating new file...")
            symbols_prices_data = {
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'account_type': '',
                'account_login': int(broker_cfg['LOGIN_ID']) if 'LOGIN_ID' in broker_cfg else None
            }
        
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f" [{inv_id}] ⚠️ Account management file not found")
            return stats
        
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Extract symbols dictionary
            symbols_dict = config.get("symbols_dictionary", {})
            if not symbols_dict:
                print(f" [{inv_id}] ⚠️ No symbols_dictionary found")
                return stats
            
            print(f"\n  📁 Candle data will be saved to symbols_prices.json")
            
            # Track statistics
            total_symbols = 0
            successful_symbols = 0
            failed_symbols = 0
            total_candles_fetched = 0
            current_candle_forming = False
            
            # Symbol resolution cache - exactly like populate_orders_missing_fields
            resolution_cache = {}
            
            # Dictionary to store all symbols' candle data
            all_symbols_candle_data = {}
            
            # Process each category in symbols_dictionary
            for category, symbols in symbols_dict.items():
                print(f"\n  📂 Category: {category.upper()} ({len(symbols)} symbols)")
                
                for raw_symbol in symbols:
                    total_symbols += 1
                    print(f"    🔍 Fetching 15m candles for: {raw_symbol}...", end=" ")
                    
                    # Fetch candles with resolution cache
                    success, normalized_symbol, symbol_candles_data, current_candle, error = fetch_symbol_candles(
                        raw_symbol, resolution_cache
                    )
                    
                    if not success:
                        print(f" {error[:50] if error else 'Unknown error'}")
                        failed_symbols += 1
                        continue
                    
                    # Check if we have a current forming candle
                    if current_candle is not None:
                        current_candle_forming = True
                        print(f"✅ (has forming candle)")
                    else:
                        print(f"✅")
                    
                    # Store in the all symbols dictionary using normalized symbol as key
                    all_symbols_candle_data[normalized_symbol] = symbol_candles_data
                    
                    successful_symbols += 1
                    total_candles_fetched += symbol_candles_data['total_candles_fetched']
                    
                    # Print sample of fetched candles with numbers
                    if symbol_candles_data['candles']:
                        # Show first few candles (newest/current)
                        print(f"        📊 Fetched {symbol_candles_data['total_candles_fetched']} completed candles + 1 forming = {len(symbol_candles_data['candles'])} total")
                        print(f"        📋 Candle numbering: 100 = current forming, 99-1 = completed (newest to oldest)")
                        
                        # Show current forming (if exists)
                        if symbol_candles_data['has_current_forming']:
                            current = symbol_candles_data['current_forming_candle']
                            print(f"          • Current Forming #{current['candle_number']}: {current['time_str']} (O:{current['open']:.{symbol_candles_data['digits']}f}, "
                                  f"H:{current['high']:.{symbol_candles_data['digits']}f}, L:{current['low']:.{symbol_candles_data['digits']}f}, C:{current['close']:.{symbol_candles_data['digits']}f})")
                        
                        # Show first completed candle (most recent completed)
                        first_completed = next((c for c in symbol_candles_data['candles'] if not c['is_forming']), None)
                        if first_completed:
                            print(f"          • Most Recent Completed #{first_completed['candle_number']}: {first_completed['time_str']} (O:{first_completed['open']:.{symbol_candles_data['digits']}f}, "
                                  f"C:{first_completed['close']:.{symbol_candles_data['digits']}f})")
                        
                        # Show oldest candle
                        oldest = symbol_candles_data['candles'][-1]
                        print(f"          • Oldest #{oldest['candle_number']}: {oldest['time_str']} (O:{oldest['open']:.{symbol_candles_data['digits']}f}, "
                              f"C:{oldest['close']:.{symbol_candles_data['digits']}f})")
            
            # Save all candle data to symbols_prices.json
            if all_symbols_candle_data:
                save_candles_to_symbols_prices(prices_dir, all_symbols_candle_data, symbols_prices_data)
            
            # Update stats
            stats["total_symbols"] = total_symbols
            stats["successful_symbols"] = successful_symbols
            stats["failed_symbols"] = failed_symbols
            stats["total_candles_fetched"] = total_candles_fetched
            stats["current_candle_forming"] = current_candle_forming
            
            print(f"\n  📊 CANDLE FETCHING SUMMARY:")
            print(f"    • Total symbols: {total_symbols}")
            print(f"    • Successful: {successful_symbols}")
            print(f"    • Failed: {failed_symbols}")
            print(f"    • Total completed candles: {total_candles_fetched}")
            print(f"    • Current forming candle: {'✅ Yes' if current_candle_forming else ' No'}")
            print(f"    • Numbering scheme: 100 = current forming, 99-1 = completed (newest to oldest)")
            print(f"    • Symbols mapped: {stats['symbols_mapped']}")
            print(f"    • Symbols unchanged: {stats['symbols_unchanged']}")
            print(f"    • All candle data saved to symbols_prices.json as [symbol]_tf_candles")
            
        except Exception as e:
            print(f" [{inv_id}]  Error: {e}")
            import traceback
            traceback.print_exc()
    
    return stats

def identify_first_crosser_candle(inv_id=None):
    """
    Identify which selected order (bid or ask) gets crossed first by a 15-minute candle.
    The race is between:
    - Selected bid order vs its counter order
    - Selected ask order vs its counter order
    
    CORRECTED LOGIC:
    - For buy stop or sell limit: Candle must open ABOVE entry AND low < entry (crosses down)
    - For sell stop or buy limit: Candle must open BELOW entry AND high > entry (crosses up)
    
    Only the very first crossing candle wins the race.
    CURRENT FORMING CANDLE (number 100) IS EXCLUDED - search starts from completed candles only.
    
    RESULTS:
    - Updates symbols_prices.json with [symbol]_crossedcandle_report top-level keys
    - Updates signals.json by marking the winning order with first_most_recent_crossed_candle_orders: true
      (only the winning order gets the flag, not its counter)
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the crosser candle analysis
    """
    print(f"\n{'='*10} 🏁 IDENTIFY FIRST CROSSER CANDLE (RACE ANALYSIS) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        "symbols_with_crosser": 0,
        "bid_wins": 0,  # Selected bid or its counter won
        "ask_wins": 0,  # Selected ask or its counter won
        "no_crosser": 0,
        "orders_marked_in_signals": 0,  # Orders marked with crosser flag
        "crosser_details": {}
    }
    
    def get_candle_color(candle):
        """Determine if candle is green (bullish) or red (bearish)"""
        if candle['close'] > candle['open']:
            return "GREEN", "bullish"
        elif candle['close'] < candle['open']:
            return "RED", "bearish"
        else:
            return "NEUTRAL", "neutral"
    
    def check_candle_crosses_order(candle, order_entry, order_type):
        """
        Check if a candle crosses a specific order.
        
        CORRECTED LOGIC:
        - For buy stop or sell limit: Candle must open ABOVE entry AND low < entry (crosses down)
        - For sell stop or buy limit: Candle must open BELOW entry AND high > entry (crosses up)
        
        Args:
            candle: Candle dictionary with open, high, low
            order_entry: Entry price of the order
            order_type: Type of order (sell_stop, buy_stop, sell_limit, buy_limit)
            
        Returns:
            bool: True if candle crosses the order
        """
        open_price = candle['open']
        high_price = candle['high']
        low_price = candle['low']
        
        # For buy stop or sell limit: Candle opens above entry and goes down through it
        if order_type in ['buy_stop', 'sell_limit']:
            # Need open above entry and low below entry (crosses downward)
            return open_price > order_entry and low_price < order_entry
        
        # For sell stop or buy limit: Candle opens below entry and goes up through it
        elif order_type in ['sell_stop', 'buy_limit']:
            # Need open below entry and high above entry (crosses upward)
            return open_price < order_entry and high_price > order_entry
        
        return False
    
    def mark_crosser_order_in_signals(signals_path, symbol, winning_order_data, winner_type, cross_direction):
        """
        Mark the winning order in signals.json with first_most_recent_crossed_candle_orders: true.
        
        Args:
            signals_path: Path to signals.json file
            symbol: Symbol name
            winning_order_data: The order data that won the race
            winner_type: Type of winner (selected_bid, selected_bid_counter, selected_ask, selected_ask_counter)
            cross_direction: Direction of cross (UP or DOWN)
            
        Returns:
            int: Number of orders marked (0 or 1)
        """
        if not signals_path.exists():
            print(f"            ⚠️  signals.json not found at {signals_path}")
            return 0
        
        try:
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            orders_marked = 0
            
            # Navigate to the symbol in signals.json
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Check if this symbol exists in this category
                if symbol in symbols_in_category:
                    symbol_signals = symbols_in_category[symbol]
                    
                    # Find and mark the winning order
                    winner_entry = winning_order_data['entry']
                    winner_order_type = winning_order_data['order_type']
                    
                    # Check in bid_orders
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - winner_entry) < 0.00001 and 
                            bid_order.get('order_type') == winner_order_type):
                            # Found the matching order - add the flag
                            if 'first_most_recent_crossed_candle_orders' not in bid_order:
                                bid_order['first_most_recent_crossed_candle_orders'] = True
                                orders_marked += 1
                                print(f"            ✅ Marked bid order at {winner_entry} with first_most_recent_crossed_candle_orders: true")
                                print(f"               • Winner Type: {winner_type}")
                                print(f"               • Cross Direction: {cross_direction}")
                    
                    # Check in ask_orders
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - winner_entry) < 0.00001 and 
                            ask_order.get('order_type') == winner_order_type):
                            # Found the matching order - add the flag
                            if 'first_most_recent_crossed_candle_orders' not in ask_order:
                                ask_order['first_most_recent_crossed_candle_orders'] = True
                                orders_marked += 1
                                print(f"            ✅ Marked ask order at {winner_entry} with first_most_recent_crossed_candle_orders: true")
                                print(f"               • Winner Type: {winner_type}")
                                print(f"               • Cross Direction: {cross_direction}")
                    
                    break  # Found the symbol
            
            # Save the updated signals.json if any marks were made
            if orders_marked > 0:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"            ✅ Updated signals.json with {orders_marked} order marked with crosser flag")
            
            return orders_marked
            
        except Exception as e:
            print(f"             Error marking order in signals.json: {e}")
            return 0
    
    def find_first_crosser_candle_for_order(order_data, completed_candles_list, order_side):
        """
        Find the first candle that crosses a specific order.
        Searches ONLY completed candles (current forming candle excluded).
        
        Args:
            order_data: Order dictionary with entry and order_type
            completed_candles_list: List of COMPLETED candles sorted by time (newest first)
            order_side: 'bid' or 'ask' for logging
            
        Returns:
            tuple: (crossed_candle, candle_index) or (None, -1) if no cross
        """
        order_entry = order_data['entry']
        order_type = order_data['order_type']
        
        # Search through COMPLETED candles only (already sorted newest to oldest)
        for idx, candle in enumerate(completed_candles_list):
            if check_candle_crosses_order(candle, order_entry, order_type):
                # Found a crossing candle
                cross_direction = "DOWN" if order_type in ['buy_stop', 'sell_limit'] else "UP"
                # Get candle color
                color_desc, color_type = get_candle_color(candle)
                
                print(f"          🎯 Candle #{candle['candle_number']} at {candle['time_str']} crosses {order_side} order {cross_direction}:")
                print(f"            • Entry: {order_entry:.{candle.get('digits', 2)}f}")
                print(f"            • Order Type: {order_type}")
                print(f"            • Candle: O:{candle['open']:.{candle.get('digits', 2)}f}, "
                      f"H:{candle['high']:.{candle.get('digits', 2)}f}, "
                      f"L:{candle['low']:.{candle.get('digits', 2)}f}")
                print(f"            • Candle Color: {color_desc}")
                return candle, idx
        
        return None, -1
    
    def analyze_symbol_crosser(symbol_data, symbol_candles_data, symbol, signals_path):
        """
        Analyze the race between selected bid and selected ask orders for a symbol.
        Uses ONLY completed candles (current forming candle excluded).
        
        Args:
            symbol_data: Symbol price data from symbols_prices.json
            symbol_candles_data: Candle data from [symbol]_tf_candles
            symbol: Symbol name
            signals_path: Path to signals.json file
            
        Returns:
            dict: Analysis results for this symbol
        """
        print(f"\n    🔍 Analyzing race for {symbol}:")
        
        # Extract selected bid and ask orders
        grid_orders = symbol_data.get('grid_orders', {})
        bid_levels = grid_orders.get('bid_levels', [])
        ask_levels = grid_orders.get('ask_levels', [])
        
        # Find selected bid and ask orders
        selected_bid = None
        selected_ask = None
        
        for level in bid_levels:
            if level.get('selected_bid'):
                selected_bid = level
                break
        
        for level in ask_levels:
            if level.get('selected_ask'):
                selected_ask = level
                break
        
        if not selected_bid or not selected_ask:
            print(f"      ⚠️  Missing selected bid or ask order for {symbol}")
            return None
        
        # Get candles from symbol_candles_data
        if not symbol_candles_data:
            print(f"      ⚠️  No candle data found for {symbol}")
            return None
        
        # Get all candles and filter out current forming candle
        all_candles = symbol_candles_data.get('candles', [])
        completed_candles = [c for c in all_candles if not c.get('is_forming', False)]
        
        if not completed_candles:
            print(f"      ⚠️  No completed candles found for {symbol}")
            return None
        
        # Sort completed candles by candle_number descending (newest first)
        completed_candles_sorted = sorted(completed_candles, key=lambda x: x['candle_number'], reverse=True)
        
        print(f"      📊 Analyzing {len(completed_candles_sorted)} COMPLETED candles for race conditions")
        print(f"      ⚠️  Current forming candle (#100) EXCLUDED from analysis")
        print(f"      🏁 RACE PARTICIPANTS:")
        print(f"        • Selected Bid (Order Type: {selected_bid['order_type']}, Entry: {selected_bid['entry']:.{symbol_data.get('digits', 2)}f})")
        print(f"        • Selected Bid Counter (Order Type: {selected_bid['order_counter']['order_type']}, Entry: {selected_bid['order_counter']['entry']:.{symbol_data.get('digits', 2)}f})")
        print(f"        • Selected Ask (Order Type: {selected_ask['order_type']}, Entry: {selected_ask['entry']:.{symbol_data.get('digits', 2)}f})")
        print(f"        • Selected Ask Counter (Order Type: {selected_ask['order_counter']['order_type']}, Entry: {selected_ask['order_counter']['entry']:.{symbol_data.get('digits', 2)}f})")
        
        # Track all potential crossers with their candle index
        crosser_candidates = []
        
        # Check selected bid order
        bid_candle, bid_idx = find_first_crosser_candle_for_order(selected_bid, completed_candles_sorted, "selected bid")
        if bid_candle:
            cross_direction = "DOWN" if selected_bid['order_type'] in ['buy_stop', 'sell_limit'] else "UP"
            crosser_candidates.append({
                'type': 'selected_bid',
                'candle': bid_candle,
                'index': bid_idx,
                'order': selected_bid,
                'cross_direction': cross_direction
            })
        
        # Check selected bid counter order
        bid_counter_candle, bid_counter_idx = find_first_crosser_candle_for_order(
            selected_bid['order_counter'], completed_candles_sorted, "selected bid counter"
        )
        if bid_counter_candle:
            cross_direction = "DOWN" if selected_bid['order_counter']['order_type'] in ['buy_stop', 'sell_limit'] else "UP"
            crosser_candidates.append({
                'type': 'selected_bid_counter',
                'candle': bid_counter_candle,
                'index': bid_counter_idx,
                'order': selected_bid['order_counter'],
                'cross_direction': cross_direction
            })
        
        # Check selected ask order
        ask_candle, ask_idx = find_first_crosser_candle_for_order(selected_ask, completed_candles_sorted, "selected ask")
        if ask_candle:
            cross_direction = "DOWN" if selected_ask['order_type'] in ['buy_stop', 'sell_limit'] else "UP"
            crosser_candidates.append({
                'type': 'selected_ask',
                'candle': ask_candle,
                'index': ask_idx,
                'order': selected_ask,
                'cross_direction': cross_direction
            })
        
        # Check selected ask counter order
        ask_counter_candle, ask_counter_idx = find_first_crosser_candle_for_order(
            selected_ask['order_counter'], completed_candles_sorted, "selected ask counter"
        )
        if ask_counter_candle:
            cross_direction = "DOWN" if selected_ask['order_counter']['order_type'] in ['buy_stop', 'sell_limit'] else "UP"
            crosser_candidates.append({
                'type': 'selected_ask_counter',
                'candle': ask_counter_candle,
                'index': ask_counter_idx,
                'order': selected_ask['order_counter'],
                'cross_direction': cross_direction
            })
        
        # Determine the winner (lowest index = earliest candle in newest-to-oldest search)
        if crosser_candidates:
            # Sort by index to find the first cross (lowest index)
            winner = min(crosser_candidates, key=lambda x: x['index'])
            
            # Verify the winner actually meets the correct condition based on order type
            winner_order = winner['order']
            winner_candle = winner['candle']
            winner_order_type = winner_order['order_type']
            winner_entry = winner_order['entry']
            cross_direction = winner['cross_direction']
            
            # Double-check the condition
            is_valid = False
            if winner_order_type in ['buy_stop', 'sell_limit']:
                is_valid = winner_candle['open'] > winner_entry and winner_candle['low'] < winner_entry
            else:  # sell_stop or buy_limit
                is_valid = winner_candle['open'] < winner_entry and winner_candle['high'] > winner_entry
            
            if not is_valid:
                print(f"      ⚠️  Warning: Winner doesn't meet condition - this shouldn't happen")
            
            # Get candle color
            color_desc, color_type = get_candle_color(winner_candle)
            
            print(f"\n      🏆 WINNER FOUND:")
            print(f"        • Winner Type: {winner['type'].upper()}")
            print(f"        • Cross Direction: {cross_direction}")
            print(f"        • Candle #{winner['candle']['candle_number']} at {winner['candle']['time_str']}")
            print(f"        • Candle Color: {color_desc}")
            print(f"        • Order Entry: {winner_entry:.{symbol_data.get('digits', 2)}f}")
            print(f"        • Order Type: {winner_order_type}")
            print(f"        • Candle Range: {winner_candle['low']:.{symbol_data.get('digits', 2)}f} - {winner_candle['high']:.{symbol_data.get('digits', 2)}f}")
            
            # Mark the winning order in signals.json
            orders_marked = mark_crosser_order_in_signals(
                signals_path,
                symbol,
                winner_order,
                winner['type'],
                cross_direction
            )
            
            # Determine if bid or ask side won
            winner_side = 'bid' if 'bid' in winner['type'] else 'ask'
            
            # Determine if winner is counter or main order
            is_counter = 'counter' in winner['type']
            is_main = not is_counter  # Main if not counter
            
            # Create detailed crosser report structure
            crosser_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signals_updated': orders_marked > 0,
                'orders_marked_in_signals': orders_marked,
                'race_summary': {
                    'winner_side': winner_side,
                    'winner_type': winner['type'],
                    'cross_direction': cross_direction,
                    'total_candidates': len(crosser_candidates),
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True
                },
                'winning_order': {
                    'entry': winner_entry,
                    'order_type': winner_order_type,
                    'volume': winner_order.get('volume'),
                    'risk_in_usd': winner_order.get('risk_in_usd'),
                    'is_counter': is_counter,
                    'is_main': is_main,
                    'original_side': 'bid' if 'bid' in winner['type'] else 'ask'
                },
                'winning_candle': {
                    'number': winner_candle['candle_number'],
                    'time': winner_candle['time'],
                    'time_str': winner_candle['time_str'],
                    'open': winner_candle['open'],
                    'high': winner_candle['high'],
                    'low': winner_candle['low'],
                    'close': winner_candle['close'],
                    'color': color_desc,
                    'candle_type': color_type,
                    'is_forming': winner_candle.get('is_forming', False)
                },
                'all_candidates': [
                    {
                        'type': c['type'],
                        'candle_number': c['candle']['candle_number'],
                        'candle_time': c['candle']['time_str'],
                        'order_entry': c['order']['entry'],
                        'order_type': c['order']['order_type'],
                        'cross_direction': c['cross_direction']
                    } for c in crosser_candidates
                ],
                'selected_orders': {
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type'],
                        'counter_entry': selected_bid['order_counter']['entry'],
                        'counter_order_type': selected_bid['order_counter']['order_type']
                    },
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type'],
                        'counter_entry': selected_ask['order_counter']['entry'],
                        'counter_order_type': selected_ask['order_counter']['order_type']
                    }
                }
            }
            
            return crosser_report
        else:
            print(f"\n      ⏳ No crossing candle found in COMPLETED candles for any order")
            
            # Create empty crosser report
            empty_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signals_updated': False,
                'orders_marked_in_signals': 0,
                'race_summary': {
                    'winner_side': 'none',
                    'winner_type': 'none',
                    'cross_direction': 'none',
                    'total_candidates': 0,
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True
                },
                'winning_order': None,
                'winning_candle': None,
                'all_candidates': [],
                'selected_orders': {
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type'],
                        'counter_entry': selected_bid['order_counter']['entry'],
                        'counter_order_type': selected_bid['order_counter']['order_type']
                    },
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type'],
                        'counter_entry': selected_ask['order_counter']['entry'],
                        'counter_order_type': selected_ask['order_counter']['order_type']
                    }
                },
                'note': 'No crossing candle found in completed candles'
            }
            
            return empty_report
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to symbols_prices.json (single source of truth)
        symbols_prices_path = prices_dir / "symbols_prices.json"
        signals_path = prices_dir / "signals.json"
        
        if not symbols_prices_path.exists():
            print(f" [{inv_id}]  symbols_prices.json not found at {symbols_prices_path}")
            return stats
        
        try:
            # Load the single file
            print(f" [{inv_id}] 📂 Loading symbols_prices.json...")
            with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                symbols_prices_data = json.load(f)
            
            # Extract symbols data (excluding metadata and other special keys)
            metadata_keys = ['account_type', 'account_login', 'account_server', 'account_balance', 
                           'account_currency', 'collected_at', 'grid_configuration', 
                           'target_risk_range_usd', 'total_categories', 'total_symbols',
                           'successful_symbols', 'failed_symbols', 'success_rate_percent', 'categories',
                           'candles_fetch_metadata', 'crosser_analysis_metadata',
                           'liquidator_analysis_metadata', 'ranging_analysis_metadata']
            
            # Also exclude any keys that end with _tf_candles or _crossedcandle_report
            symbols_dict = {}
            candles_dict = {}
            
            for key, value in symbols_prices_data.items():
                if key in metadata_keys:
                    continue
                elif key.endswith('_tf_candles'):
                    # This is candle data
                    symbol_name = key.replace('_tf_candles', '')
                    candles_dict[symbol_name] = value
                elif key.endswith('_crossedcandle_report'):
                    # Skip existing reports, we'll regenerate
                    continue
                else:
                    # This is symbol data
                    symbols_dict[key] = value
            
            print(f"  📊 Found {len(symbols_dict)} symbols and {len(candles_dict)} candle datasets in symbols_prices.json")
            
            # Track reports to add
            reports_added = 0
            
            for symbol, symbol_data in symbols_dict.items():
                stats["total_symbols_analyzed"] += 1
                
                # Get candle data for this symbol
                symbol_candles_data = candles_dict.get(symbol)
                
                if not symbol_candles_data:
                    print(f"  ⚠️  No candle data found for {symbol} (looking for {symbol}_tf_candles)")
                    stats["no_crosser"] += 1
                    continue
                
                # Analyze this symbol using ONLY completed candles
                crosser_report = analyze_symbol_crosser(
                    symbol_data, symbol_candles_data, symbol, signals_path
                )
                
                if crosser_report:
                    # Add the report as a top-level key with naming convention: symbol_crossedcandle_report
                    report_key = f"{symbol}_crossedcandle_report"
                    symbols_prices_data[report_key] = crosser_report
                    reports_added += 1
                    
                    if crosser_report['race_summary']['winner_side'] != 'none':
                        stats["symbols_with_crosser"] += 1
                        if crosser_report['race_summary']['winner_side'] == 'bid':
                            stats["bid_wins"] += 1
                        else:
                            stats["ask_wins"] += 1
                        
                        stats["orders_marked_in_signals"] += crosser_report['orders_marked_in_signals']
                        
                        # Store in stats
                        stats["crosser_details"][symbol] = {
                            'winner_side': crosser_report['race_summary']['winner_side'],
                            'winner_type': crosser_report['race_summary']['winner_type'],
                            'candle_number': crosser_report['winning_candle']['number'] if crosser_report['winning_candle'] else None,
                            'candle_time': crosser_report['winning_candle']['time_str'] if crosser_report['winning_candle'] else None,
                            'candle_color': crosser_report['winning_candle']['color'] if crosser_report['winning_candle'] else None,
                            'orders_marked': crosser_report['orders_marked_in_signals']
                        }
                    else:
                        stats["no_crosser"] += 1
            
            # Save the updated symbols_prices.json with all crosser reports as top-level keys
            if reports_added > 0:
                # Add metadata about the crosser analysis at top level
                symbols_prices_data['crosser_analysis_metadata'] = {
                    'analyzed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_symbols_analyzed': stats['total_symbols_analyzed'],
                    'symbols_with_crosser': stats['symbols_with_crosser'],
                    'bid_wins': stats['bid_wins'],
                    'ask_wins': stats['ask_wins'],
                    'no_crosser': stats['no_crosser'],
                    'orders_marked_in_signals': stats['orders_marked_in_signals'],
                    'reports_added': reports_added,
                    'note': 'Current forming candle (#100) was excluded from all searches. Reports are stored as [symbol]_crossedcandle_report top-level keys. Winning orders are marked in signals.json with first_most_recent_crossed_candle_orders: true. Only the winning order gets the flag (not its counter).'
                }
                
                # Save back to file
                with open(symbols_prices_path, 'w', encoding='utf-8') as f:
                    json.dump(symbols_prices_data, f, indent=4, default=str)
                
                print(f"\n  ✅ Updated symbols_prices.json with {reports_added} crossedcandle_report entries as top-level keys")
                print(f"  ✅ Updated signals.json with {stats['orders_marked_in_signals']} orders marked with first_most_recent_crossed_candle_orders: true")
            
            # Print summary
            print(f"\n  📊 CROSSER CANDLE ANALYSIS SUMMARY:")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            print(f"    • Symbols with crosser: {stats['symbols_with_crosser']}")
            print(f"    • Bid side wins: {stats['bid_wins']}")
            print(f"    • Ask side wins: {stats['ask_wins']}")
            print(f"    • No crosser found: {stats['no_crosser']}")
            print(f"    • SIGNALS UPDATED: {stats['orders_marked_in_signals']} orders marked in signals.json")
            print(f"    • Flag added: first_most_recent_crossed_candle_orders: true to winning order only")
            print(f"    • Current forming candle (#100) was EXCLUDED from all searches")
            print(f"    • Reports saved in symbols_prices.json as [symbol]_crossedcandle_report")
            print(f"    • Candle data read from [symbol]_tf_candles")
            print(f"    • Example: BTCUSD, BTCUSD_tf_candles, BTCUSD_crossedcandle_report")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in crosser analysis: {e}")
            import traceback
            traceback.print_exc()
    
    return stats

def identify_trapped_candles(inv_id=None):
    """
    Identify candles that are trapped between an order and its counter order.
    
    A candle is considered "trapped" when it forms completely between the two price levels:
    - Upper level: Buy Stop & Sell Limit (same price above current)
    - Lower level: Sell Stop & Buy Limit (same price below current)
    
    The candle must be completely contained between these two levels:
    - Candle HIGH < Upper Level price
    - Candle LOW > Lower Level price
    
    AGE FILTERING:
    - Only marks trapped levels if the MOST RECENT trapped candle is OLDER than the crossed candle
    - If the most recent trapped candle is YOUNGER than crossed candle, NO flags are added
    - Age determined by candle number (higher = OLDER, lower = YOUNGER)
    - Example: Trapped #90 vs Crossed #85 → 90 > 85 → Trapped is OLDER → ALLOW marking
    - Example: Trapped #80 vs Crossed #85 → 80 < 85 → Trapped is YOUNGER → SKIP marking
    
    CURRENT FORMING CANDLE (number 100) IS EXCLUDED - search starts from completed candles only.
    
    RESULTS:
    - Updates symbols_prices.json with [symbol]_trappedcandle_report top-level keys
    - Updates signals.json ONLY IF age condition is met, marking trapped levels with first_trapped_levels: true
      (both the main order and its counter order receive the flag)
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the trapped candle analysis
    """
    print(f"\n{'='*10} 🪤 IDENTIFY TRAPPED CANDLES (BETWEEN ORDER & COUNTER) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        "symbols_with_trapped_candles": 0,
        "symbols_with_trapped_and_older_than_crossed": 0,  # Symbols that passed age filter
        "symbols_skipped_due_to_age": 0,  # Symbols skipped because trapped is younger
        "total_trapped_candles_found": 0,
        "orders_marked_in_signals": 0,        # Main orders marked with trapped flag
        "counter_orders_marked": 0,           # Counter orders marked with trapped flag
        "trapped_by_level": {
            "between_bid_ask_levels": 0,      # Candle trapped between bid and ask levels
        },
        "no_trapped_candles": 0,
        "age_filter_results": {
            "older_than_crossed": 0,
            "younger_than_crossed_skipped": 0,
            "no_crosser_data": 0
        },
        "trapped_details": {}
    }
    
    def get_candle_color(candle):
        """Determine if candle is green (bullish) or red (bearish)"""
        if candle['close'] > candle['open']:
            return "GREEN", "bullish"
        elif candle['close'] < candle['open']:
            return "RED", "bearish"
        else:
            return "NEUTRAL", "neutral"
    
    def get_crossed_candle_info(symbols_prices_data, symbol):
        """
        Extract crossed candle information from symbol's crossedcandle_report.
        
        Args:
            symbols_prices_data: Complete symbols_prices.json data
            symbol: Symbol name
            
        Returns:
            dict: Crossed candle info or None
        """
        report_key = f"{symbol}_crossedcandle_report"
        if report_key in symbols_prices_data:
            report = symbols_prices_data[report_key]
            if report.get('winning_candle'):
                return {
                    'candle_number': report['winning_candle']['number'],
                    'candle_time': report['winning_candle']['time_str'],
                    'winner_type': report.get('race_summary', {}).get('winner_type'),
                    'cross_direction': report.get('race_summary', {}).get('cross_direction')
                }
        return None
    
    def determine_age_relationship(trapped_candle_number, crossed_candle_number):
        """
        Determine if trapped candle is older, younger, or same age as crossed candle.
        
        Age logic: Higher candle number = OLDER (further in past)
                  Lower candle number = YOUNGER (more recent)
        
        Args:
            trapped_candle_number: Candle number of most recent trapped candle
            crossed_candle_number: Candle number of crossed candle
            
        Returns:
            str: 'older', 'younger', or 'sameage'
        """
        if trapped_candle_number > crossed_candle_number:
            return "older"
        elif trapped_candle_number < crossed_candle_number:
            return "younger"
        else:
            return "sameage"
    
    def mark_trapped_levels_in_signals(signals_path, symbol, lower_order_data, upper_order_data, age_info=None):
        """
        Mark the trapped levels in signals.json with first_trapped_levels: true.
        Marks both the main order and its counter order.
        
        Args:
            signals_path: Path to signals.json file
            symbol: Symbol name
            lower_order_data: The lower level order data that has trapped candles
            upper_order_data: The upper level order data that has trapped candles
            age_info: Optional age comparison info for logging
            
        Returns:
            tuple: (main_orders_marked, counter_orders_marked)
        """
        if not signals_path.exists():
            print(f"            ⚠️  signals.json not found at {signals_path}")
            return 0, 0
        
        try:
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            main_orders_marked = 0
            counter_orders_marked = 0
            
            # Navigate to the symbol in signals.json
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Check if this symbol exists in this category
                if symbol in symbols_in_category:
                    symbol_signals = symbols_in_category[symbol]
                    
                    # Mark the LOWER level order (and its counter)
                    lower_entry = lower_order_data['entry']
                    lower_order_type = lower_order_data['order_type']
                    
                    # Check in bid_orders for lower level
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - lower_entry) < 0.00001 and 
                            bid_order.get('order_type') == lower_order_type):
                            # Found the matching order - add the flag
                            if 'first_trapped_levels' not in bid_order:
                                bid_order['first_trapped_levels'] = True
                                main_orders_marked += 1
                                print(f"            ✅ Marked bid order at {lower_entry} with first_trapped_levels: true")
                            
                            # Also mark its counter order
                            if 'order_counter' in bid_order:
                                if 'first_trapped_levels' not in bid_order['order_counter']:
                                    bid_order['order_counter']['first_trapped_levels'] = True
                                    counter_orders_marked += 1
                                    print(f"            ✅ Marked counter order for bid level with first_trapped_levels: true")
                    
                    # Check in ask_orders for lower level
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - lower_entry) < 0.00001 and 
                            ask_order.get('order_type') == lower_order_type):
                            if 'first_trapped_levels' not in ask_order:
                                ask_order['first_trapped_levels'] = True
                                main_orders_marked += 1
                                print(f"            ✅ Marked ask order at {lower_entry} with first_trapped_levels: true")
                            
                            if 'order_counter' in ask_order:
                                if 'first_trapped_levels' not in ask_order['order_counter']:
                                    ask_order['order_counter']['first_trapped_levels'] = True
                                    counter_orders_marked += 1
                                    print(f"            ✅ Marked counter order for ask level with first_trapped_levels: true")
                    
                    # Mark the UPPER level order (and its counter)
                    upper_entry = upper_order_data['entry']
                    upper_order_type = upper_order_data['order_type']
                    
                    # Check in bid_orders for upper level
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - upper_entry) < 0.00001 and 
                            bid_order.get('order_type') == upper_order_type):
                            if 'first_trapped_levels' not in bid_order:
                                bid_order['first_trapped_levels'] = True
                                main_orders_marked += 1
                                print(f"            ✅ Marked bid order at {upper_entry} with first_trapped_levels: true")
                            
                            if 'order_counter' in bid_order:
                                if 'first_trapped_levels' not in bid_order['order_counter']:
                                    bid_order['order_counter']['first_trapped_levels'] = True
                                    counter_orders_marked += 1
                                    print(f"            ✅ Marked counter order for bid level with first_trapped_levels: true")
                    
                    # Check in ask_orders for upper level
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - upper_entry) < 0.00001 and 
                            ask_order.get('order_type') == upper_order_type):
                            if 'first_trapped_levels' not in ask_order:
                                ask_order['first_trapped_levels'] = True
                                main_orders_marked += 1
                                print(f"            ✅ Marked ask order at {upper_entry} with first_trapped_levels: true")
                            
                            if 'order_counter' in ask_order:
                                if 'first_trapped_levels' not in ask_order['order_counter']:
                                    ask_order['order_counter']['first_trapped_levels'] = True
                                    counter_orders_marked += 1
                                    print(f"            ✅ Marked counter order for ask level with first_trapped_levels: true")
                    
                    break  # Found the symbol
            
            # Save the updated signals.json if any marks were made
            if main_orders_marked > 0 or counter_orders_marked > 0:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"            ✅ Updated signals.json with {main_orders_marked} main orders and {counter_orders_marked} counter orders marked")
            
            return main_orders_marked, counter_orders_marked
            
        except Exception as e:
            print(f"             Error marking orders in signals.json: {e}")
            return 0, 0
    
    def check_candle_trapped(candle, lower_level_price, upper_level_price, pair_name):
        """
        Check if a candle is trapped between the lower and upper price levels.
        
        Args:
            candle: Candle dictionary with open, high, low
            lower_level_price: Price of the lower level (Sell Stop / Buy Limit)
            upper_level_price: Price of the upper level (Buy Stop / Sell Limit)
            pair_name: 'bid' or 'ask' for logging
            
        Returns:
            tuple: (is_trapped, details) or (False, None)
        """
        candle_high = candle['high']
        candle_low = candle['low']
        
        # A candle is trapped if it's completely between the two levels:
        # - High is below the upper level
        # - Low is above the lower level
        # This means the entire candle fits between them without touching either
        
        is_trapped = candle_high < upper_level_price and candle_low > lower_level_price
        
        if is_trapped:
            # Get candle color
            color_desc, color_type = get_candle_color(candle)
            
            details = {
                'lower_level': lower_level_price,
                'upper_level': upper_level_price,
                'candle_high': candle_high,
                'candle_low': candle_low,
                'candle_open': candle['open'],
                'candle_close': candle['close'],
                'candle_color': color_desc,
                'candle_type': color_type,
                'condition': f"Low ({candle_low:.{candle.get('digits', 2)}f}) > Lower Level ({lower_level_price:.{candle.get('digits', 2)}f}) AND High ({candle_high:.{candle.get('digits', 2)}f}) < Upper Level ({upper_level_price:.{candle.get('digits', 2)}f})",
                'gap_size': upper_level_price - lower_level_price,
                'candle_size': candle_high - candle_low
            }
            return True, details
        
        return False, None
    
    def find_trapped_candles_for_pair(lower_order, upper_order, completed_candles_list, pair_name):
        """
        Find all candles trapped between the lower and upper order levels.
        
        Args:
            lower_order: The order at the lower level (Sell Stop or Buy Limit)
            upper_order: The order at the upper level (Buy Stop or Sell Limit)
            completed_candles_list: List of COMPLETED candles sorted by time (newest first)
            pair_name: 'bid' or 'ask' for logging
            
        Returns:
            list: List of trapped candles with details
        """
        trapped_candles = []
        
        lower_price = lower_order['entry']
        upper_price = upper_order['entry']
        lower_type = lower_order['order_type']
        upper_type = upper_order['order_type']
        
        # Validate that this is a valid pair (one from each level)
        is_valid_pair = False
        
        # Upper level orders (above current price)
        upper_level_types = ['buy_stop', 'sell_limit']
        # Lower level orders (below current price)
        lower_level_types = ['sell_stop', 'buy_limit']
        
        if upper_type in upper_level_types and lower_type in lower_level_types:
            is_valid_pair = True
            print(f"          🔍 Checking for trapped candles between levels:")
            print(f"            • Lower Level ({lower_type}): {lower_price:.{completed_candles_list[0].get('digits', 2)}f}")
            print(f"            • Upper Level ({upper_type}): {upper_price:.{completed_candles_list[0].get('digits', 2)}f}")
            print(f"            • Gap Size: {upper_price - lower_price:.{completed_candles_list[0].get('digits', 2)}f}")
        else:
            # This shouldn't happen with correct data structure
            print(f"          ⚠️  Invalid pair: {upper_type} (upper) and {lower_type} (lower)")
            return trapped_candles
        
        # Search through COMPLETED candles only
        for idx, candle in enumerate(completed_candles_list):
            is_trapped, details = check_candle_trapped(candle, lower_price, upper_price, pair_name)
            
            if is_trapped:
                # Found a trapped candle
                trapped_candles.append({
                    'candle': candle,
                    'candle_index': idx,
                    'details': details,
                    'lower_order': lower_order,
                    'upper_order': upper_order,
                    'pair_name': pair_name
                })
                
                print(f"          🪤 Candle #{candle['candle_number']} at {candle['time_str']} is TRAPPED:")
                print(f"            • {details['condition']}")
                print(f"            • Candle Color: {details['candle_color']}")
                print(f"            • Candle Range: {candle['low']:.{candle.get('digits', 2)}f} - {candle['high']:.{candle.get('digits', 2)}f}")
                print(f"            • Gap Size: {details['gap_size']:.{candle.get('digits', 2)}f}, Candle Size: {details['candle_size']:.{candle.get('digits', 2)}f}")
        
        return trapped_candles
    
    def analyze_symbol_trapped_candles(symbol_data, symbol_candles_data, symbol, signals_path, symbols_prices_data):
        """
        Analyze trapped candles between the bid level and ask level.
        Applies age filter: Only mark in signals if most recent trapped candle is OLDER than crossed candle.
        
        Args:
            symbol_data: Symbol price data from symbols_prices.json
            symbol_candles_data: Candle data from [symbol]_tf_candles
            symbol: Symbol name
            signals_path: Path to signals.json file
            symbols_prices_data: Complete symbols_prices.json data (for accessing crosser report)
            
        Returns:
            dict: Analysis results for this symbol
        """
        print(f"\n    🔍 Analyzing trapped candles for {symbol}:")
        
        # Extract selected bid and ask orders
        grid_orders = symbol_data.get('grid_orders', {})
        bid_levels = grid_orders.get('bid_levels', [])
        ask_levels = grid_orders.get('ask_levels', [])
        
        # Find selected bid and ask orders
        selected_bid = None
        selected_ask = None
        
        for level in bid_levels:
            if level.get('selected_bid'):
                selected_bid = level
                break
        
        for level in ask_levels:
            if level.get('selected_ask'):
                selected_ask = level
                break
        
        if not selected_bid or not selected_ask:
            print(f"      ⚠️  Missing selected bid or ask order for {symbol}")
            return None
        
        # Get candles from symbol_candles_data
        if not symbol_candles_data:
            print(f"      ⚠️  No candle data found for {symbol}")
            return None
        
        # Get all candles and filter out current forming candle
        all_candles = symbol_candles_data.get('candles', [])
        completed_candles = [c for c in all_candles if not c.get('is_forming', False)]
        
        if not completed_candles:
            print(f"      ⚠️  No completed candles found for {symbol}")
            return None
        
        # Sort completed candles by candle_number descending (newest first)
        completed_candles_sorted = sorted(completed_candles, key=lambda x: x['candle_number'], reverse=True)
        
        # Get crossed candle info for age comparison
        crossed_info = get_crossed_candle_info(symbols_prices_data, symbol)
        
        # Determine which level is higher and which is lower
        bid_price = selected_bid['entry']
        ask_price = selected_ask['entry']
        
        # The bid level (Sell Stop / Buy Limit) should be LOWER
        # The ask level (Buy Stop / Sell Limit) should be HIGHER
        # But let's verify and sort them properly
        lower_level_price = min(bid_price, ask_price)
        upper_level_price = max(bid_price, ask_price)
        
        # Identify which order belongs to which level
        if bid_price < ask_price:
            lower_order = selected_bid
            upper_order = selected_ask
            print(f"      📊 Bid level is LOWER ({bid_price:.{symbol_data.get('digits', 2)}f}), Ask level is HIGHER ({ask_price:.{symbol_data.get('digits', 2)}f})")
        else:
            lower_order = selected_ask
            upper_order = selected_bid
            print(f"      📊 Ask level is LOWER ({ask_price:.{symbol_data.get('digits', 2)}f}), Bid level is HIGHER ({bid_price:.{symbol_data.get('digits', 2)}f})")
        
        print(f"      📊 Analyzing {len(completed_candles_sorted)} COMPLETED candles for trapped conditions")
        print(f"      ⚠️  Current forming candle (#100) EXCLUDED from analysis")
        
        if crossed_info:
            print(f"      📊 Crossed candle reference: #{crossed_info['candle_number']} at {crossed_info['candle_time']}")
        else:
            print(f"      📊 No crossed candle data available for age comparison")
        
        print(f"      🪤 PRICE LEVELS:")
        print(f"        • Lower Level: {lower_order['order_type']} @ {lower_order['entry']:.{symbol_data.get('digits', 2)}f}")
        print(f"        • Upper Level: {upper_order['order_type']} @ {upper_order['entry']:.{symbol_data.get('digits', 2)}f}")
        print(f"        • Gap Size: {upper_level_price - lower_level_price:.{symbol_data.get('digits', 2)}f}")
        
        # Track all trapped candles
        all_trapped_candles = []
        
        # Check for candles trapped between the two levels
        pair_trapped = find_trapped_candles_for_pair(
            lower_order,
            upper_order,
            completed_candles_sorted,
            "between_levels"
        )
        all_trapped_candles.extend(pair_trapped)
        
        # Sort trapped candles by index (newest first)
        all_trapped_candles.sort(key=lambda x: x['candle_index'])
        
        if all_trapped_candles:
            print(f"\n      🪤 TOTAL TRAPPED CANDLES FOUND: {len(all_trapped_candles)}")
            
            # Get the most recent trapped candle (first in list)
            most_recent_trapped = all_trapped_candles[0]
            most_recent_candle_number = most_recent_trapped['candle']['candle_number']
            
            # Initialize age filter variables
            age_filter_passed = False
            age_relationship = None
            age_filter_reason = ""
            
            # Apply age filter if crossed candle data exists
            if crossed_info:
                age_relationship = determine_age_relationship(most_recent_candle_number, crossed_info['candle_number'])
                print(f"      🔍 Age comparison: Trapped #{most_recent_candle_number} vs Crossed #{crossed_info['candle_number']} = {age_relationship}")
                
                if age_relationship == "older":
                    age_filter_passed = True
                    stats["age_filter_results"]["older_than_crossed"] += 1
                    age_filter_reason = f"Trapped #{most_recent_candle_number} is OLDER than Crossed #{crossed_info['candle_number']} → ALLOW marking"
                    print(f"      ✅ Age filter PASSED: {age_filter_reason}")
                elif age_relationship == "younger":
                    age_filter_passed = False
                    stats["age_filter_results"]["younger_than_crossed_skipped"] += 1
                    age_filter_reason = f"Trapped #{most_recent_candle_number} is YOUNGER than Crossed #{crossed_info['candle_number']} → SKIP marking in signals.json"
                    print(f"      ⚠️ Age filter FAILED: {age_filter_reason}")
                else:  # sameage
                    age_filter_passed = True  # Same age counts as allowed
                    stats["age_filter_results"]["older_than_crossed"] += 1  # Count as "older" for stats
                    age_filter_reason = f"Trapped #{most_recent_candle_number} is SAME AGE as Crossed #{crossed_info['candle_number']} → ALLOW marking"
                    print(f"      ✅ Age filter PASSED: {age_filter_reason}")
            else:
                # No crossed data available - still mark but track in stats
                age_filter_passed = True
                stats["age_filter_results"]["no_crosser_data"] += 1
                age_filter_reason = "No crossed candle data available for comparison → ALLOW marking by default"
                print(f"      ⚠️ {age_filter_reason}")
            
            # Mark the trapped levels in signals.json ONLY if age filter passed
            main_marked = 0
            counter_marked = 0
            
            if age_filter_passed:
                main_marked, counter_marked = mark_trapped_levels_in_signals(
                    signals_path,
                    symbol,
                    lower_order,
                    upper_order
                )
                
                # Update stats for successful marks
                stats["orders_marked_in_signals"] += main_marked
                stats["counter_orders_marked"] += counter_marked
                stats["symbols_with_trapped_and_older_than_crossed"] += 1
            else:
                print(f"      🚫 SKIPPED marking in signals.json because trapped candles are YOUNGER than crossed candle")
                stats["symbols_skipped_due_to_age"] += 1
            
            # Create trapped candle report structure (always create report, even if age filter failed)
            trapped_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signals_updated': age_filter_passed,
                'age_filter_applied': {
                    'passed': age_filter_passed,
                    'reason': age_filter_reason,
                    'most_recent_trapped_candle': most_recent_candle_number,
                    'crossed_candle': crossed_info['candle_number'] if crossed_info else None,
                    'relationship': age_relationship,
                    'age_rule': 'Higher candle number = OLDER, Lower = YOUNGER'
                },
                'orders_marked_in_signals': main_marked if age_filter_passed else 0,
                'counter_orders_marked_in_signals': counter_marked if age_filter_passed else 0,
                'summary': {
                    'total_trapped_candles': len(all_trapped_candles),
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True,
                    'lower_level': {
                        'price': lower_order['entry'],
                        'order_type': lower_order['order_type']
                    },
                    'upper_level': {
                        'price': upper_order['entry'],
                        'order_type': upper_order['order_type']
                    },
                    'gap_size': upper_level_price - lower_level_price
                },
                'trapped_candles': [
                    {
                        'candle_number': tc['candle']['candle_number'],
                        'candle_time': tc['candle']['time_str'],
                        'candle_time_raw': tc['candle']['time'],
                        'open': tc['candle']['open'],
                        'high': tc['candle']['high'],
                        'low': tc['candle']['low'],
                        'close': tc['candle']['close'],
                        'color': tc['details']['candle_color'],
                        'candle_type': tc['details']['candle_type'],
                        'trapped_details': tc['details'],
                        'verification': {
                            'low_above_lower': tc['candle']['low'] > lower_order['entry'],
                            'high_below_upper': tc['candle']['high'] < upper_order['entry'],
                            'is_trapped': True
                        }
                    }
                    for tc in all_trapped_candles
                ],
                'selected_orders': {
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type'],
                        'counter_entry': selected_bid['order_counter']['entry'],
                        'counter_order_type': selected_bid['order_counter']['order_type']
                    },
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type'],
                        'counter_entry': selected_ask['order_counter']['entry'],
                        'counter_order_type': selected_ask['order_counter']['order_type']
                    }
                }
            }
            
            return trapped_report
        else:
            print(f"\n      ⏳ No trapped candles found in COMPLETED candles")
            
            # Create empty trapped report
            empty_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'signals_updated': False,
                'age_filter_applied': None,
                'orders_marked_in_signals': 0,
                'counter_orders_marked_in_signals': 0,
                'summary': {
                    'total_trapped_candles': 0,
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True,
                    'lower_level': {
                        'price': lower_order['entry'],
                        'order_type': lower_order['order_type']
                    },
                    'upper_level': {
                        'price': upper_order['entry'],
                        'order_type': upper_order['order_type']
                    },
                    'gap_size': upper_level_price - lower_level_price
                },
                'trapped_candles': [],
                'selected_orders': {
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type'],
                        'counter_entry': selected_bid['order_counter']['entry'],
                        'counter_order_type': selected_bid['order_counter']['order_type']
                    },
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type'],
                        'counter_entry': selected_ask['order_counter']['entry'],
                        'counter_order_type': selected_ask['order_counter']['order_type']
                    }
                },
                'note': 'No trapped candles found in completed candles'
            }
            
            return empty_report
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to symbols_prices.json (single source of truth)
        symbols_prices_path = prices_dir / "symbols_prices.json"
        signals_path = prices_dir / "signals.json"
        
        if not symbols_prices_path.exists():
            print(f" [{inv_id}]  symbols_prices.json not found at {symbols_prices_path}")
            return stats
        
        try:
            # Load the single file
            print(f" [{inv_id}] 📂 Loading symbols_prices.json...")
            with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                symbols_prices_data = json.load(f)
            
            # Extract symbols data (excluding metadata and other special keys)
            metadata_keys = ['account_type', 'account_login', 'account_server', 'account_balance', 
                           'account_currency', 'collected_at', 'grid_configuration', 
                           'target_risk_range_usd', 'total_categories', 'total_symbols',
                           'successful_symbols', 'failed_symbols', 'success_rate_percent', 'categories',
                           'candles_fetch_metadata', 'crosser_analysis_metadata', 
                           'liquidator_analysis_metadata', 'ranging_analysis_metadata',
                           'trapped_analysis_metadata']
            
            # Also exclude any keys that end with _tf_candles, _crossedcandle_report, or _trappedcandle_report
            symbols_dict = {}
            candles_dict = {}
            
            for key, value in symbols_prices_data.items():
                if key in metadata_keys:
                    continue
                elif key.endswith('_tf_candles'):
                    # This is candle data
                    symbol_name = key.replace('_tf_candles', '')
                    candles_dict[symbol_name] = value
                elif key.endswith('_crossedcandle_report') or key.endswith('_trappedcandle_report') or \
                     key.endswith('_anylevel_liquidator_report') or key.endswith('_ranging_report'):
                    # Skip existing reports, we'll regenerate
                    continue
                else:
                    # This is symbol data
                    symbols_dict[key] = value
            
            print(f"  📊 Found {len(symbols_dict)} symbols and {len(candles_dict)} candle datasets in symbols_prices.json")
            
            # Track reports to add
            reports_added = 0
            
            for symbol, symbol_data in symbols_dict.items():
                stats["total_symbols_analyzed"] += 1
                
                # Get candle data for this symbol
                symbol_candles_data = candles_dict.get(symbol)
                
                if not symbol_candles_data:
                    print(f"  ⚠️  No candle data found for {symbol} (looking for {symbol}_tf_candles)")
                    stats["no_trapped_candles"] += 1
                    continue
                
                # Analyze this symbol for trapped candles using ONLY completed candles
                # Pass symbols_prices_data for accessing crosser report
                trapped_report = analyze_symbol_trapped_candles(
                    symbol_data, symbol_candles_data, symbol, signals_path, symbols_prices_data
                )
                
                if trapped_report:
                    # Add the report as a top-level key with naming convention: symbol_trappedcandle_report
                    report_key = f"{symbol}_trappedcandle_report"
                    symbols_prices_data[report_key] = trapped_report
                    reports_added += 1
                    
                    if trapped_report['summary']['total_trapped_candles'] > 0:
                        stats["symbols_with_trapped_candles"] += 1
                        stats["total_trapped_candles_found"] += trapped_report['summary']['total_trapped_candles']
                        stats["trapped_by_level"]["between_bid_ask_levels"] += trapped_report['summary']['total_trapped_candles']
                        
                        # Only add to stats if age filter passed (otherwise marking counts are 0)
                        if trapped_report.get('signals_updated', False):
                            stats["orders_marked_in_signals"] += trapped_report['orders_marked_in_signals']
                            stats["counter_orders_marked"] += trapped_report['counter_orders_marked_in_signals']
                        
                        # Store in stats
                        stats["trapped_details"][symbol] = {
                            'total_trapped': trapped_report['summary']['total_trapped_candles'],
                            'lower_level': trapped_report['summary']['lower_level'],
                            'upper_level': trapped_report['summary']['upper_level'],
                            'gap_size': trapped_report['summary']['gap_size'],
                            'age_filter_passed': trapped_report.get('age_filter_applied', {}).get('passed', False) if trapped_report.get('age_filter_applied') else None,
                            'age_relationship': trapped_report.get('age_filter_applied', {}).get('relationship') if trapped_report.get('age_filter_applied') else None,
                            'orders_marked': trapped_report['orders_marked_in_signals'],
                            'counter_marked': trapped_report['counter_orders_marked_in_signals'],
                            'latest_trapped': [
                                {
                                    'candle_number': tc['candle_number'],
                                    'candle_time': tc['candle_time'],
                                    'color': tc['color'],
                                    'low': tc['low'],
                                    'high': tc['high']
                                }
                                for tc in trapped_report['trapped_candles'][:3]  # Show latest 3
                            ] if trapped_report['trapped_candles'] else []
                        }
                    else:
                        stats["no_trapped_candles"] += 1
            
            # Save the updated symbols_prices.json with all trapped reports as top-level keys
            if reports_added > 0:
                # Add metadata about the trapped analysis at top level
                symbols_prices_data['trapped_analysis_metadata'] = {
                    'analyzed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_symbols_analyzed': stats['total_symbols_analyzed'],
                    'symbols_with_trapped_candles': stats['symbols_with_trapped_candles'],
                    'symbols_with_trapped_and_older_than_crossed': stats['symbols_with_trapped_and_older_than_crossed'],
                    'symbols_skipped_due_to_age': stats['symbols_skipped_due_to_age'],
                    'total_trapped_candles_found': stats['total_trapped_candles_found'],
                    'trapped_by_level': stats['trapped_by_level'],
                    'age_filter_results': stats['age_filter_results'],
                    'orders_marked_in_signals': stats['orders_marked_in_signals'],
                    'counter_orders_marked_in_signals': stats['counter_orders_marked'],
                    'no_trapped_candles': stats['no_trapped_candles'],
                    'reports_added': reports_added,
                    'note': 'Current forming candle (#100) was excluded from all searches. Age filter applied: Only mark signals if most recent trapped candle is OLDER than crossed candle (higher candle number = OLDER). Reports are stored as [symbol]_trappedcandle_report top-level keys.'
                }
                
                # Save back to file
                with open(symbols_prices_path, 'w', encoding='utf-8') as f:
                    json.dump(symbols_prices_data, f, indent=4, default=str)
                
                print(f"\n  ✅ Updated symbols_prices.json with {reports_added} trappedcandle_report entries as top-level keys")
                
                if stats['orders_marked_in_signals'] > 0 or stats['counter_orders_marked'] > 0:
                    print(f"  ✅ Updated signals.json with {stats['orders_marked_in_signals']} main orders and {stats['counter_orders_marked']} counter orders marked with first_trapped_levels: true")
                else:
                    print(f"  ⚠️ No signals.json updates - all trapped candles were YOUNGER than crossed candles")
            
            # Print summary with age filter details
            print(f"\n  📊 TRAPPED CANDLE ANALYSIS SUMMARY:")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            print(f"    • Symbols with trapped candles: {stats['symbols_with_trapped_candles']}")
            print(f"    • Total trapped candles found: {stats['total_trapped_candles_found']}")
            print(f"    • Trapped between bid/ask levels: {stats['trapped_by_level']['between_bid_ask_levels']}")
            print(f"    • No trapped candles: {stats['no_trapped_candles']}")
            
            print(f"\n    📊 AGE FILTER RESULTS (vs Crossed Candle):")
            print(f"      • Older (ALLOWED): {stats['age_filter_results']['older_than_crossed']}")
            print(f"      • Younger (SKIPPED): {stats['age_filter_results']['younger_than_crossed_skipped']}")
            print(f"      • No crosser data (ALLOWED by default): {stats['age_filter_results']['no_crosser_data']}")
            print(f"      • Symbols marked in signals: {stats['symbols_with_trapped_and_older_than_crossed']}")
            print(f"      • Symbols skipped due to age: {stats['symbols_skipped_due_to_age']}")
            
            print(f"\n    • SIGNALS UPDATED: {stats['orders_marked_in_signals']} main orders marked in signals.json")
            print(f"    • COUNTER ORDERS UPDATED: {stats['counter_orders_marked']} counter orders marked in signals.json")
            print(f"    • Flag added: first_trapped_levels: true to both main and counter orders")
            print(f"    • Current forming candle (#100) was EXCLUDED from all searches")
            print(f"    • Age rule: Higher candle number = OLDER, Lower = YOUNGER")
            print(f"    • Reports saved in symbols_prices.json as [symbol]_trappedcandle_report")
            print(f"    • Example: BTCUSD, BTCUSD_tf_candles, BTCUSD_trappedcandle_report")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in trapped candle analysis: {e}")
            import traceback
            traceback.print_exc()
    
    return stats   

def identify_levels_liquidator_candle(inv_id=None):
    """
    Identify the first candle that liquidates ANY TWO CONSECUTIVE grid levels
    (takes out both orders in the same candle), regardless of selection status.
    
    A candle is considered a "levels liquidator" when:
    - Its HIGH is greater than the UPPER LEVEL (any ask/buy level above)
    - Its LOW is less than the LOWER LEVEL (any bid/sell level below)
    
    This means the candle's range completely covers two consecutive grid levels,
    effectively taking out (liquidating) both orders in a single candle.
    
    AGE COMPARISON FEATURE:
    - Compares liquidator candle with crossed candle (from crosser analysis)
    - Compares liquidator candle with trapped candle (from trapped analysis)
    - Adds flags: liquidator_{color}_is_{older/younger/sameage}_than_crossed_candle: true
    - Adds flags: liquidator_{color}_is_{older/younger/sameage}_than_trapped_candle: true
    - Age determined by candle number (higher number = OLDER, lower number = YOUNGER)
    
    SEARCH LOGIC:
    - Analyzes ALL grid levels (bid_levels and ask_levels from symbols_prices.json)
    - Tests EVERY possible consecutive level pair (level N and level N+1)
    - Finds the FIRST (most recent) candle that liquidates ANY such pair
    - Current forming candle (#100) is EXCLUDED from search
    
    RESULTS:
    - Updates symbols_prices.json with [symbol]_anylevel_liquidator_report top-level keys
    - Updates signals.json by marking the liquidated orders with:
      * first_both_levels_{color}liquidator: true
      * liquidator_{color}_is_{older/younger/sameage}_than_crossed_candle: true
      * liquidator_{color}_is_{older/younger/sameage}_than_trapped_candle: true
      (both the main order and its counter order receive the flags)
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the any-level liquidator candle analysis
    """
    print(f"\n{'='*10} 💧 IDENTIFY ANY LEVELS LIQUIDATOR CANDLE (ALL GRID LEVELS) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        "symbols_with_any_liquidator": 0,
        "symbols_without_any_liquidator": 0,
        "total_level_pairs_analyzed": 0,
        "liquidator_pairs_found": 0,
        "orders_marked_in_signals": 0,
        "counter_orders_marked": 0,
        "age_comparisons": {
            "vs_crossed": {
                "older": 0,
                "younger": 0,
                "sameage": 0,
                "no_crosser_data": 0
            },
            "vs_trapped": {
                "older": 0,
                "younger": 0,
                "sameage": 0,
                "no_trapped_data": 0
            }
        },
        "liquidator_details": {}
    }
    
    def get_candle_color(candle):
        """Determine if candle is green (bullish) or red (bearish)"""
        if candle['close'] > candle['open']:
            return "GREEN", "bullish"
        elif candle['close'] < candle['open']:
            return "RED", "bearish"
        else:
            return "NEUTRAL", "neutral"
    
    def determine_age_relationship(liquidator_candle_number, reference_candle_number):
        """
        Determine if liquidator is older, younger, or same age as reference candle.
        
        Age logic: Higher candle number = OLDER (further in past)
                  Lower candle number = YOUNGER (more recent)
        
        Args:
            liquidator_candle_number: Candle number of liquidator
            reference_candle_number: Candle number to compare against
            
        Returns:
            str: 'older', 'younger', or 'sameage'
        """
        if liquidator_candle_number > reference_candle_number:
            return "older"
        elif liquidator_candle_number < reference_candle_number:
            return "younger"
        else:
            return "sameage"
    
    def get_crossed_candle_info(symbols_prices_data, symbol):
        """
        Extract crossed candle information from symbol's crossedcandle_report.
        
        Args:
            symbols_prices_data: Complete symbols_prices.json data
            symbol: Symbol name
            
        Returns:
            dict: Crossed candle info or None
        """
        report_key = f"{symbol}_crossedcandle_report"
        if report_key in symbols_prices_data:
            report = symbols_prices_data[report_key]
            if report.get('winning_candle'):
                return {
                    'candle_number': report['winning_candle']['number'],
                    'candle_time': report['winning_candle']['time_str'],
                    'winner_type': report.get('race_summary', {}).get('winner_type'),
                    'cross_direction': report.get('race_summary', {}).get('cross_direction')
                }
        return None
    
    def get_trapped_candle_info(symbols_prices_data, symbol):
        """
        Extract trapped candle information from symbol's trappedcandle_report.
        Returns the most recent trapped candle (first in list).
        
        Args:
            symbols_prices_data: Complete symbols_prices.json data
            symbol: Symbol name
            
        Returns:
            dict: Most recent trapped candle info or None
        """
        report_key = f"{symbol}_trappedcandle_report"
        if report_key in symbols_prices_data:
            report = symbols_prices_data[report_key]
            if report.get('trapped_candles') and len(report['trapped_candles']) > 0:
                # Get the most recent trapped candle (first in list)
                latest_trapped = report['trapped_candles'][0]
                return {
                    'candle_number': latest_trapped['candle_number'],
                    'candle_time': latest_trapped['candle_time'],
                    'candle_color': latest_trapped.get('color')
                }
        return None
    
    def check_candle_liquidates_level_pair(candle, lower_level, upper_level):
        """
        Check if a candle liquidates a specific pair of levels.
        
        Args:
            candle: Candle dictionary with open, high, low, close
            lower_level: Dictionary of the lower level order
            upper_level: Dictionary of the upper level order
            
        Returns:
            tuple: (is_liquidator, details) or (False, None)
        """
        candle_high = candle['high']
        candle_low = candle['low']
        lower_price = lower_level['entry']
        upper_price = upper_level['entry']
        
        # A candle liquidates both levels if:
        # - High is above the upper level (took out upper order)
        # - Low is below the lower level (took out lower order)
        is_liquidator = candle_high > upper_price and candle_low < lower_price
        
        if is_liquidator:
            # Determine candle color
            color_desc, color_type = get_candle_color(candle)
            
            # Determine which order types were taken
            lower_taken = candle_low < lower_price
            upper_taken = candle_high > upper_price
            
            details = {
                'lower_level': lower_price,
                'upper_level': upper_price,
                'lower_order_type': lower_level['order_type'],
                'upper_order_type': upper_level['order_type'],
                'candle_high': candle_high,
                'candle_low': candle_low,
                'candle_open': candle['open'],
                'candle_close': candle['close'],
                'candle_color': color_desc,
                'candle_color_lower': color_desc.lower(),  # Added for flag creation
                'candle_type': color_type,
                'lower_taken': lower_taken,
                'upper_taken': upper_taken,
                'condition': f"High ({candle_high:.{candle.get('digits', 2)}f}) > Upper Level ({upper_price:.{candle.get('digits', 2)}f}) AND Low ({candle_low:.{candle.get('digits', 2)}f}) < Lower Level ({lower_price:.{candle.get('digits', 2)}f})",
                'range_covered': candle_high - candle_low,
                'levels_range': upper_price - lower_price
            }
            return True, details
        
        return False, None
    
    def find_first_liquidator_for_symbol(all_bid_levels, all_ask_levels, completed_candles_list, symbol_data):
        """
        Find the FIRST (most recent) candle that liquidates ANY consecutive level pair.
        Tests all possible combinations of bid and ask levels.
        
        Args:
            all_bid_levels: List of all bid level orders
            all_ask_levels: List of all ask level orders
            completed_candles_list: List of COMPLETED candles sorted by time (newest first)
            symbol_data: Symbol data for digits formatting
            
        Returns:
            tuple: (liquidator_candle, liquidator_details, level_pair_info, candle_index) 
                   or (None, None, None, -1)
        """
        print(f"\n          🔍 Searching for ANY liquidator candle in ALL grid levels:")
        print(f"            • Total bid levels: {len(all_bid_levels)}")
        print(f"            • Total ask levels: {len(all_ask_levels)}")
        
        # Collect all levels with their type and entry price
        all_levels = []
        
        # Add bid levels (these are typically lower levels - sell orders)
        for i, level in enumerate(all_bid_levels):
            all_levels.append({
                'entry': level['entry'],
                'order_type': level['order_type'],
                'original_index': i,
                'original_type': 'bid',
                'level_data': level
            })
        
        # Add ask levels (these are typically higher levels - buy orders)
        for i, level in enumerate(all_ask_levels):
            all_levels.append({
                'entry': level['entry'],
                'order_type': level['order_type'],
                'original_index': i,
                'original_type': 'ask',
                'level_data': level
            })
        
        # Sort all levels by price (ascending - lower to higher)
        all_levels_sorted = sorted(all_levels, key=lambda x: x['entry'])
        
        print(f"            • Total combined levels: {len(all_levels_sorted)}")
        print(f"            • Testing consecutive level pairs (lower + next higher):")
        
        # Test each consecutive pair of levels
        level_pairs_tested = 0
        liquidator_candidates = []
        
        for i in range(len(all_levels_sorted) - 1):
            lower_level = all_levels_sorted[i]
            upper_level = all_levels_sorted[i + 1]
            
            # Only test if the lower level is actually lower (should be by sorting)
            if lower_level['entry'] >= upper_level['entry']:
                continue
            
            level_pairs_tested += 1
            
            # Format level types for display
            lower_type_desc = f"{lower_level['original_type']} ({lower_level['order_type']})"
            upper_type_desc = f"{upper_level['original_type']} ({upper_level['order_type']})"
            
            print(f"              📊 Pair {level_pairs_tested}: {lower_level['entry']:.{symbol_data.get('digits', 2)}f} [{lower_type_desc}] - {upper_level['entry']:.{symbol_data.get('digits', 2)}f} [{upper_type_desc}]")
            
            # Search through candles for this pair (stop at first match)
            for candle_idx, candle in enumerate(completed_candles_list):
                is_liquidator, details = check_candle_liquidates_level_pair(
                    candle, lower_level['level_data'], upper_level['level_data']
                )
                
                if is_liquidator:
                    # Found a liquidator candle for this pair
                    liquidator_candidates.append({
                        'candle': candle,
                        'candle_index': candle_idx,
                        'candle_number': candle['candle_number'],
                        'lower_level': lower_level,
                        'upper_level': upper_level,
                        'details': details,
                        'pair_description': f"Level {i+1} ({lower_level['entry']:.{symbol_data.get('digits', 2)}f}) - Level {i+2} ({upper_level['entry']:.{symbol_data.get('digits', 2)}f})"
                    })
                    
                    print(f"                💧 FOUND LIQUIDATOR at candle #{candle['candle_number']} ({details['candle_color']})")
                    break  # Stop searching candles for this pair, move to next pair
        
        # After testing all pairs, find the most recent liquidator (lowest candle_index)
        if liquidator_candidates:
            # Sort by candle_index to find the most recent (lowest index in newest-first list)
            winner = min(liquidator_candidates, key=lambda x: x['candle_index'])
            
            print(f"\n          🏆 OVERALL WINNER FOUND:")
            print(f"            • Liquidating Pair: {winner['pair_description']}")
            print(f"            • Lower Level Type: {winner['lower_level']['original_type']} ({winner['lower_level']['order_type']})")
            print(f"            • Upper Level Type: {winner['upper_level']['original_type']} ({winner['upper_level']['order_type']})")
            print(f"            • Candle #{winner['candle']['candle_number']} at {winner['candle']['time_str']}")
            print(f"            • Candle Color: {winner['details']['candle_color']}")
            
            return winner['candle'], winner['details'], winner, winner['candle_index']
        else:
            print(f"\n          ⏳ No liquidator candle found for any level pair")
            return None, None, None, -1
    
    def mark_liquidated_orders_in_signals(signals_path, symbol, lower_level_data, upper_level_data, 
                                          candle_color, age_vs_crossed=None, age_vs_trapped=None,
                                          crossed_info=None, trapped_info=None):
        """
        Mark the liquidated orders in signals.json with:
        - first_both_levels_{color}_liquidator: true
        - liquidator_{color}_is_{older/younger/sameage}_than_crossed_candle: true (if crossed data exists)
        - liquidator_{color}_is_{older/younger/sameage}_than_trapped_candle: true (if trapped data exists)
        
        Args:
            signals_path: Path to signals.json file
            symbol: Symbol name
            lower_level_data: The lower level order data that was liquidated
            upper_level_data: The upper level order data that was liquidated
            candle_color: Color of the liquidator candle ("GREEN" or "RED")
            age_vs_crossed: Age relationship with crossed candle ('older', 'younger', 'sameage') or None
            age_vs_trapped: Age relationship with trapped candle ('older', 'younger', 'sameage') or None
            crossed_info: Crossed candle info for logging
            trapped_info: Trapped candle info for logging
            
        Returns:
            tuple: (main_orders_marked, counter_orders_marked)
        """
        if not signals_path.exists():
            print(f"            ⚠️  signals.json not found at {signals_path}")
            return 0, 0
        
        # Create the color-specific flag names
        color_lower = candle_color.lower()
        main_flag = f"first_both_levels_{color_lower}_liquidator"
        
        print(f"            🏷️  Using main flag: {main_flag}: true")
        
        # Create age comparison flags if applicable
        age_flags = []
        if age_vs_crossed and crossed_info:
            crossed_flag = f"liquidator_{color_lower}_is_{age_vs_crossed}_than_crossed_candle"
            age_flags.append(crossed_flag)
            print(f"            🏷️  Adding age flag: {crossed_flag}: true")
            print(f"            📊 Comparison: Liquidator #{crossed_info['liquidator_num']} vs Crossed #{crossed_info['crossed_num']} = {age_vs_crossed}")
        
        if age_vs_trapped and trapped_info:
            trapped_flag = f"liquidator_{color_lower}_is_{age_vs_trapped}_than_trapped_candle"
            age_flags.append(trapped_flag)
            print(f"            🏷️  Adding age flag: {trapped_flag}: true")
            print(f"            📊 Comparison: Liquidator #{trapped_info['liquidator_num']} vs Trapped #{trapped_info['trapped_num']} = {age_vs_trapped}")
        
        try:
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            main_orders_marked = 0
            counter_orders_marked = 0
            
            # Navigate to the symbol in signals.json
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Check if this symbol exists in this category
                if symbol in symbols_in_category:
                    symbol_signals = symbols_in_category[symbol]
                    
                    # Mark the lower level order
                    lower_entry = lower_level_data['entry']
                    lower_order_type = lower_level_data['order_type']
                    
                    # Check in bid_orders
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - lower_entry) < 0.00001 and 
                            bid_order.get('order_type') == lower_order_type):
                            # Found the matching order - add the main flag
                            if main_flag not in bid_order:
                                bid_order[main_flag] = True
                                main_orders_marked += 1
                            
                            # Add age comparison flags
                            for age_flag in age_flags:
                                if age_flag not in bid_order:
                                    bid_order[age_flag] = True
                            
                            # Also mark its counter order
                            if 'order_counter' in bid_order:
                                if main_flag not in bid_order['order_counter']:
                                    bid_order['order_counter'][main_flag] = True
                                    counter_orders_marked += 1
                                
                                for age_flag in age_flags:
                                    if age_flag not in bid_order['order_counter']:
                                        bid_order['order_counter'][age_flag] = True
                    
                    # Check in ask_orders for lower level
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - lower_entry) < 0.00001 and 
                            ask_order.get('order_type') == lower_order_type):
                            if main_flag not in ask_order:
                                ask_order[main_flag] = True
                                main_orders_marked += 1
                            
                            for age_flag in age_flags:
                                if age_flag not in ask_order:
                                    ask_order[age_flag] = True
                            
                            if 'order_counter' in ask_order:
                                if main_flag not in ask_order['order_counter']:
                                    ask_order['order_counter'][main_flag] = True
                                    counter_orders_marked += 1
                                
                                for age_flag in age_flags:
                                    if age_flag not in ask_order['order_counter']:
                                        ask_order['order_counter'][age_flag] = True
                    
                    # Mark the upper level order
                    upper_entry = upper_level_data['entry']
                    upper_order_type = upper_level_data['order_type']
                    
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - upper_entry) < 0.00001 and 
                            ask_order.get('order_type') == upper_order_type):
                            if main_flag not in ask_order:
                                ask_order[main_flag] = True
                                main_orders_marked += 1
                            
                            for age_flag in age_flags:
                                if age_flag not in ask_order:
                                    ask_order[age_flag] = True
                            
                            if 'order_counter' in ask_order:
                                if main_flag not in ask_order['order_counter']:
                                    ask_order['order_counter'][main_flag] = True
                                    counter_orders_marked += 1
                                
                                for age_flag in age_flags:
                                    if age_flag not in ask_order['order_counter']:
                                        ask_order['order_counter'][age_flag] = True
                    
                    # Check in bid_orders for upper level
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - upper_entry) < 0.00001 and 
                            bid_order.get('order_type') == upper_order_type):
                            if main_flag not in bid_order:
                                bid_order[main_flag] = True
                                main_orders_marked += 1
                            
                            for age_flag in age_flags:
                                if age_flag not in bid_order:
                                    bid_order[age_flag] = True
                            
                            if 'order_counter' in bid_order:
                                if main_flag not in bid_order['order_counter']:
                                    bid_order['order_counter'][main_flag] = True
                                    counter_orders_marked += 1
                                
                                for age_flag in age_flags:
                                    if age_flag not in bid_order['order_counter']:
                                        bid_order['order_counter'][age_flag] = True
                    
                    break  # Found the symbol
            
            # Save the updated signals.json if any marks were made
            if main_orders_marked > 0 or counter_orders_marked > 0:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                
                flag_summary = f"{main_flag}"
                if age_flags:
                    flag_summary += f" + {', '.join(age_flags)}"
                
                print(f"            ✅ Updated signals.json with {main_orders_marked} main orders and {counter_orders_marked} counter orders")
                print(f"            ✅ Flags added: {flag_summary}")
            
            return main_orders_marked, counter_orders_marked
            
        except Exception as e:
            print(f"             Error marking orders in signals.json: {e}")
            return 0, 0
    
    def analyze_symbol_any_liquidator(symbol_data, symbol_candles_data, symbol, signals_path, symbols_prices_data):
        """
        Analyze ANY liquidator candle for a symbol across all grid levels.
        Also performs age comparison with crossed and trapped candles.
        
        Args:
            symbol_data: Symbol price data from symbols_prices.json
            symbol_candles_data: Candle data from [symbol]_tf_candles
            symbol: Symbol name
            signals_path: Path to signals.json file
            symbols_prices_data: Complete symbols_prices.json data (for accessing other reports)
            
        Returns:
            dict: Analysis results for this symbol
        """
        print(f"\n    🔍 Analyzing ANY level liquidator for {symbol}:")
        
        # Extract ALL grid orders
        grid_orders = symbol_data.get('grid_orders', {})
        all_bid_levels = grid_orders.get('bid_levels', [])
        all_ask_levels = grid_orders.get('ask_levels', [])
        
        if not all_bid_levels or not all_ask_levels:
            print(f"      ⚠️  Missing bid or ask levels for {symbol}")
            return None
        
        # Get candles from symbol_candles_data
        if not symbol_candles_data:
            print(f"      ⚠️  No candle data found for {symbol}")
            return None
        
        # Get all candles and filter out current forming candle
        all_candles = symbol_candles_data.get('candles', [])
        completed_candles = [c for c in all_candles if not c.get('is_forming', False)]
        
        if not completed_candles:
            print(f"      ⚠️  No completed candles found for {symbol}")
            return None
        
        # Sort completed candles by candle_number descending (newest first)
        completed_candles_sorted = sorted(completed_candles, key=lambda x: x['candle_number'], reverse=True)
        
        # Get crossed and trapped candle info for age comparison
        crossed_info = get_crossed_candle_info(symbols_prices_data, symbol)
        trapped_info = get_trapped_candle_info(symbols_prices_data, symbol)
        
        print(f"      📊 Analyzing {len(completed_candles_sorted)} COMPLETED candles")
        print(f"      📊 Total grid levels: {len(all_bid_levels)} bid levels + {len(all_ask_levels)} ask levels = {len(all_bid_levels) + len(all_ask_levels)} levels")
        print(f"      ⚠️  Current forming candle (#100) EXCLUDED from analysis")
        
        if crossed_info:
            print(f"      📊 Crossed candle reference: #{crossed_info['candle_number']} at {crossed_info['candle_time']}")
        else:
            print(f"      📊 No crossed candle data available for age comparison")
        
        if trapped_info:
            print(f"      📊 Trapped candle reference: #{trapped_info['candle_number']} at {trapped_info['candle_time']}")
        else:
            print(f"      📊 No trapped candle data available for age comparison")
        
        print(f"      💧 SEARCHING FOR ANY LIQUIDATOR CANDLE:")
        print(f"        • Testing all consecutive level pairs")
        print(f"        • Finding the most recent liquidator across ALL pairs")
        
        # Find the first liquidator candle for ANY level pair
        liquidator_candle, liquidator_details, level_pair_info, candle_index = find_first_liquidator_for_symbol(
            all_bid_levels, all_ask_levels, completed_candles_sorted, symbol_data
        )
        
        if liquidator_candle and liquidator_details and level_pair_info:
            # Get candle color for flag creation
            candle_color = liquidator_details['candle_color']
            liquidator_candle_number = liquidator_candle['candle_number']
            
            # Perform age comparisons
            age_vs_crossed = None
            age_vs_trapped = None
            crossed_comparison_info = None
            trapped_comparison_info = None
            
            if crossed_info:
                age_vs_crossed = determine_age_relationship(liquidator_candle_number, crossed_info['candle_number'])
                crossed_comparison_info = {
                    'liquidator_num': liquidator_candle_number,
                    'crossed_num': crossed_info['candle_number'],
                    'relationship': age_vs_crossed
                }
                print(f"      🔍 Age comparison vs crossed: Liquidator #{liquidator_candle_number} is {age_vs_crossed} than crossed #{crossed_info['candle_number']}")
                
                # Update stats
                if age_vs_crossed == 'older':
                    stats["age_comparisons"]["vs_crossed"]["older"] += 1
                elif age_vs_crossed == 'younger':
                    stats["age_comparisons"]["vs_crossed"]["younger"] += 1
                else:
                    stats["age_comparisons"]["vs_crossed"]["sameage"] += 1
            else:
                stats["age_comparisons"]["vs_crossed"]["no_crosser_data"] += 1
            
            if trapped_info:
                age_vs_trapped = determine_age_relationship(liquidator_candle_number, trapped_info['candle_number'])
                trapped_comparison_info = {
                    'liquidator_num': liquidator_candle_number,
                    'trapped_num': trapped_info['candle_number'],
                    'relationship': age_vs_trapped
                }
                print(f"      🔍 Age comparison vs trapped: Liquidator #{liquidator_candle_number} is {age_vs_trapped} than trapped #{trapped_info['candle_number']}")
                
                # Update stats
                if age_vs_trapped == 'older':
                    stats["age_comparisons"]["vs_trapped"]["older"] += 1
                elif age_vs_trapped == 'younger':
                    stats["age_comparisons"]["vs_trapped"]["younger"] += 1
                else:
                    stats["age_comparisons"]["vs_trapped"]["sameage"] += 1
            else:
                stats["age_comparisons"]["vs_trapped"]["no_trapped_data"] += 1
            
            # Mark the liquidated orders in signals.json with all flags
            main_marked, counter_marked = mark_liquidated_orders_in_signals(
                signals_path, 
                symbol, 
                level_pair_info['lower_level']['level_data'],
                level_pair_info['upper_level']['level_data'],
                candle_color,
                age_vs_crossed,
                age_vs_trapped,
                crossed_comparison_info,
                trapped_comparison_info
            )
            
            # Update stats
            stats["orders_marked_in_signals"] += main_marked
            stats["counter_orders_marked"] += counter_marked
            
            # Create comprehensive liquidator report with age comparisons
            liquidator_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_type': 'any_levels_liquidator',
                'signals_updated': True,
                'orders_marked_in_signals': main_marked,
                'counter_orders_marked_in_signals': counter_marked,
                'flags_added': {
                    'main_flag': f"first_both_levels_{candle_color.lower()}_liquidator",
                    'age_flags': {
                        'vs_crossed': f"liquidator_{candle_color.lower()}_is_{age_vs_crossed}_than_crossed_candle" if age_vs_crossed else None,
                        'vs_trapped': f"liquidator_{candle_color.lower()}_is_{age_vs_trapped}_than_trapped_candle" if age_vs_trapped else None
                    }
                },
                'summary': {
                    'has_liquidator': True,
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True,
                    'total_levels': len(all_bid_levels) + len(all_ask_levels),
                    'total_level_pairs_tested': len(all_bid_levels) + len(all_ask_levels) - 1,
                    'liquidator_position': {
                        'candle_index': candle_index,
                        'candles_from_now': candle_index + 1  # +1 because index is 0-based
                    }
                },
                'age_comparisons': {
                    'vs_crossed_candle': {
                        'available': crossed_info is not None,
                        'crossed_candle_number': crossed_info['candle_number'] if crossed_info else None,
                        'crossed_candle_time': crossed_info['candle_time'] if crossed_info else None,
                        'liquidator_candle_number': liquidator_candle_number,
                        'relationship': age_vs_crossed,
                        'flag_added': f"liquidator_{candle_color.lower()}_is_{age_vs_crossed}_than_crossed_candle" if age_vs_crossed else None
                    } if crossed_info else {'available': False},
                    'vs_trapped_candle': {
                        'available': trapped_info is not None,
                        'trapped_candle_number': trapped_info['candle_number'] if trapped_info else None,
                        'trapped_candle_time': trapped_info['candle_time'] if trapped_info else None,
                        'liquidator_candle_number': liquidator_candle_number,
                        'relationship': age_vs_trapped,
                        'flag_added': f"liquidator_{candle_color.lower()}_is_{age_vs_trapped}_than_trapped_candle" if age_vs_trapped else None
                    } if trapped_info else {'available': False}
                },
                'liquidator_pair': {
                    'lower_level': {
                        'price': level_pair_info['lower_level']['entry'],
                        'order_type': level_pair_info['lower_level']['order_type'],
                        'level_type': level_pair_info['lower_level']['original_type'],
                        'original_index': level_pair_info['lower_level']['original_index'],
                        'full_order_data': level_pair_info['lower_level']['level_data']
                    },
                    'upper_level': {
                        'price': level_pair_info['upper_level']['entry'],
                        'order_type': level_pair_info['upper_level']['order_type'],
                        'level_type': level_pair_info['upper_level']['original_type'],
                        'original_index': level_pair_info['upper_level']['original_index'],
                        'full_order_data': level_pair_info['upper_level']['level_data']
                    },
                    'levels_range': level_pair_info['upper_level']['entry'] - level_pair_info['lower_level']['entry'],
                    'pair_description': level_pair_info['pair_description']
                },
                'liquidator_candle': {
                    'number': liquidator_candle['candle_number'],
                    'time': liquidator_candle['time'],
                    'time_str': liquidator_candle['time_str'],
                    'open': liquidator_candle['open'],
                    'high': liquidator_candle['high'],
                    'low': liquidator_candle['low'],
                    'close': liquidator_candle['close'],
                    'color': liquidator_details['candle_color'],
                    'candle_type': liquidator_details['candle_type'],
                    'range': liquidator_candle['high'] - liquidator_candle['low'],
                    'body_size': abs(liquidator_candle['close'] - liquidator_candle['open']),
                    'upper_shadow': liquidator_candle['high'] - max(liquidator_candle['open'], liquidator_candle['close']),
                    'lower_shadow': min(liquidator_candle['open'], liquidator_candle['close']) - liquidator_candle['low']
                },
                'liquidation_details': {
                    'upper_level_taken': liquidator_details['upper_taken'],
                    'lower_level_taken': liquidator_details['lower_taken'],
                    'upper_order_type_taken': liquidator_details['upper_order_type'],
                    'lower_order_type_taken': liquidator_details['lower_order_type'],
                    'liquidation_condition': liquidator_details['condition'],
                    'candle_vs_levels': {
                        'high_vs_upper': f"{liquidator_candle['high']:.{symbol_data.get('digits', 2)}f} > {level_pair_info['upper_level']['entry']:.{symbol_data.get('digits', 2)}f}",
                        'low_vs_lower': f"{liquidator_candle['low']:.{symbol_data.get('digits', 2)}f} < {level_pair_info['lower_level']['entry']:.{symbol_data.get('digits', 2)}f}"
                    }
                },
                'all_levels_summary': {
                    'total_bid_levels': len(all_bid_levels),
                    'total_ask_levels': len(all_ask_levels),
                    'bid_levels': [
                        {
                            'entry': level['entry'],
                            'order_type': level['order_type'],
                            'selected': level.get('selected_bid', False),
                            f'has_{candle_color.lower()}_liquidator_flag': level.get(f'first_both_levels_{candle_color.lower()}_liquidator', False)
                        } for level in all_bid_levels
                    ],
                    'ask_levels': [
                        {
                            'entry': level['entry'],
                            'order_type': level['order_type'],
                            'selected': level.get('selected_ask', False),
                            f'has_{candle_color.lower()}_liquidator_flag': level.get(f'first_both_levels_{candle_color.lower()}_liquidator', False)
                        } for level in all_ask_levels
                    ]
                }
            }
            
            return liquidator_report
        else:
            print(f"\n      ⏳ No liquidator candle found in COMPLETED candles for any level pair")
            
            # Create empty liquidator report
            empty_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_type': 'any_levels_liquidator',
                'signals_updated': False,
                'orders_marked_in_signals': 0,
                'counter_orders_marked_in_signals': 0,
                'flags_added': None,
                'summary': {
                    'has_liquidator': False,
                    'candles_analyzed': len(completed_candles_sorted),
                    'current_forming_excluded': True,
                    'total_levels': len(all_bid_levels) + len(all_ask_levels),
                    'total_level_pairs_tested': len(all_bid_levels) + len(all_ask_levels) - 1
                },
                'age_comparisons': {
                    'vs_crossed_candle': {'available': False},
                    'vs_trapped_candle': {'available': False}
                },
                'liquidator_pair': None,
                'liquidator_candle': None,
                'liquidation_details': None,
                'all_levels_summary': {
                    'total_bid_levels': len(all_bid_levels),
                    'total_ask_levels': len(all_ask_levels),
                    'bid_levels': [
                        {
                            'entry': level['entry'],
                            'order_type': level['order_type'],
                            'selected': level.get('selected_bid', False)
                        } for level in all_bid_levels
                    ],
                    'ask_levels': [
                        {
                            'entry': level['entry'],
                            'order_type': level['order_type'],
                            'selected': level.get('selected_ask', False)
                        } for level in all_ask_levels
                    ]
                },
                'note': 'No liquidator candle found in completed candles for any level pair'
            }
            
            return empty_report
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to symbols_prices.json (single source of truth)
        symbols_prices_path = prices_dir / "symbols_prices.json"
        signals_path = prices_dir / "signals.json"
        
        if not symbols_prices_path.exists():
            print(f" [{inv_id}]  symbols_prices.json not found at {symbols_prices_path}")
            return stats
        
        try:
            # Load the single file
            print(f" [{inv_id}] 📂 Loading symbols_prices.json...")
            with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                symbols_prices_data = json.load(f)
            
            # Extract symbols data (excluding metadata and other special keys)
            metadata_keys = ['account_type', 'account_login', 'account_server', 'account_balance', 
                           'account_currency', 'collected_at', 'grid_configuration', 
                           'target_risk_range_usd', 'total_categories', 'total_symbols',
                           'successful_symbols', 'failed_symbols', 'success_rate_percent', 'categories',
                           'candles_fetch_metadata', 'crosser_analysis_metadata', 
                           'liquidator_analysis_metadata', 'anylevel_liquidator_metadata',
                           'trapped_analysis_metadata']
            
            # Also exclude any keys that end with specific patterns
            symbols_dict = {}
            candles_dict = {}
            
            for key, value in symbols_prices_data.items():
                if key in metadata_keys:
                    continue
                elif key.endswith('_tf_candles'):
                    # This is candle data
                    symbol_name = key.replace('_tf_candles', '')
                    candles_dict[symbol_name] = value
                elif key.endswith('_crossedcandle_report') or \
                     key.endswith('_liquidatorcandle_report') or \
                     key.endswith('_trappedcandle_report') or \
                     key.endswith('_anylevel_liquidator_report'):
                    # Skip existing reports, we'll regenerate
                    continue
                else:
                    # This is symbol data
                    symbols_dict[key] = value
            
            print(f"  📊 Found {len(symbols_dict)} symbols and {len(candles_dict)} candle datasets in symbols_prices.json")
            
            # Track reports to add
            reports_added = 0
            total_level_pairs = 0
            
            for symbol, symbol_data in symbols_dict.items():
                stats["total_symbols_analyzed"] += 1
                
                # Get candle data for this symbol
                symbol_candles_data = candles_dict.get(symbol)
                
                if not symbol_candles_data:
                    print(f"  ⚠️  No candle data found for {symbol} (looking for {symbol}_tf_candles)")
                    stats["symbols_without_any_liquidator"] += 1
                    continue
                
                # Analyze this symbol for ANY liquidator candle using ALL grid levels
                # Pass the complete symbols_prices_data for accessing crossed/trapped reports
                liquidator_report = analyze_symbol_any_liquidator(
                    symbol_data, symbol_candles_data, symbol, signals_path, symbols_prices_data
                )
                
                if liquidator_report:
                    # Add the report as a top-level key
                    report_key = f"{symbol}_anylevel_liquidator_report"
                    symbols_prices_data[report_key] = liquidator_report
                    reports_added += 1
                    
                    # Update statistics
                    level_pairs_tested = len(symbol_data.get('grid_orders', {}).get('bid_levels', [])) + \
                                        len(symbol_data.get('grid_orders', {}).get('ask_levels', [])) - 1
                    total_level_pairs += max(0, level_pairs_tested)
                    
                    if liquidator_report['summary']['has_liquidator']:
                        stats["symbols_with_any_liquidator"] += 1
                        stats["liquidator_pairs_found"] += 1
                        
                        # Store in stats with age comparison info
                        age_info = liquidator_report.get('age_comparisons', {})
                        stats["liquidator_details"][symbol] = {
                            'candle_number': liquidator_report['liquidator_candle']['number'],
                            'candle_time': liquidator_report['liquidator_candle']['time_str'],
                            'candle_color': liquidator_report['liquidator_candle']['color'],
                            'flags_added': liquidator_report.get('flags_added', {}),
                            'age_vs_crossed': age_info.get('vs_crossed_candle', {}).get('relationship') if age_info.get('vs_crossed_candle', {}).get('available') else None,
                            'age_vs_trapped': age_info.get('vs_trapped_candle', {}).get('relationship') if age_info.get('vs_trapped_candle', {}).get('available') else None,
                            'lower_level_price': liquidator_report['liquidator_pair']['lower_level']['price'],
                            'upper_level_price': liquidator_report['liquidator_pair']['upper_level']['price'],
                            'lower_level_type': f"{liquidator_report['liquidator_pair']['lower_level']['level_type']} ({liquidator_report['liquidator_pair']['lower_level']['order_type']})",
                            'upper_level_type': f"{liquidator_report['liquidator_pair']['upper_level']['level_type']} ({liquidator_report['liquidator_pair']['upper_level']['order_type']})",
                            'orders_marked_in_signals': liquidator_report['orders_marked_in_signals'],
                            'counter_orders_marked': liquidator_report['counter_orders_marked_in_signals']
                        }
                    else:
                        stats["symbols_without_any_liquidator"] += 1
            
            stats["total_level_pairs_analyzed"] = total_level_pairs
            
            # Save the updated symbols_prices.json with all any-level liquidator reports
            if reports_added > 0:
                # Add metadata about the any-level liquidator analysis at top level
                symbols_prices_data['anylevel_liquidator_metadata'] = {
                    'analyzed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_symbols_analyzed': stats['total_symbols_analyzed'],
                    'symbols_with_any_liquidator': stats['symbols_with_any_liquidator'],
                    'symbols_without_any_liquidator': stats['symbols_without_any_liquidator'],
                    'total_level_pairs_analyzed': stats['total_level_pairs_analyzed'],
                    'liquidator_pairs_found': stats['liquidator_pairs_found'],
                    'orders_marked_in_signals': stats['orders_marked_in_signals'],
                    'counter_orders_marked_in_signals': stats['counter_orders_marked'],
                    'age_comparisons': stats['age_comparisons'],
                    'reports_added': reports_added,
                    'note': 'Current forming candle (#100) was excluded from all searches. Reports are stored as [symbol]_anylevel_liquidator_report top-level keys. Analysis considers ALL grid levels. Liquidated orders receive: first_both_levels_{color}_liquidator: true AND age comparison flags (liquidator_{color}_is_{older/younger/sameage}_than_{crossed/trapped}_candle: true). Age determined by candle number (higher = OLDER, lower = YOUNGER).'
                }
                
                # Save back to file
                with open(symbols_prices_path, 'w', encoding='utf-8') as f:
                    json.dump(symbols_prices_data, f, indent=4, default=str)
                
                print(f"\n  ✅ Updated symbols_prices.json with {reports_added} anylevel_liquidator_report entries as top-level keys")
                print(f"  ✅ Updated signals.json with {stats['orders_marked_in_signals']} main orders and {stats['counter_orders_marked']} counter orders")
                print(f"  ✅ Added age comparison flags based on candle numbers (higher = OLDER, lower = YOUNGER)")
            
            # Print detailed summary
            print(f"\n  📊 ANY LEVEL LIQUIDATOR CANDLE ANALYSIS SUMMARY:")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            print(f"    • Symbols with any liquidator: {stats['symbols_with_any_liquidator']}")
            print(f"    • Symbols without any liquidator: {stats['symbols_without_any_liquidator']}")
            print(f"    • Total level pairs analyzed: {stats['total_level_pairs_analyzed']}")
            print(f"    • Liquidator pairs found: {stats['liquidator_pairs_found']}")
            print(f"    • Current forming candle (#100) was EXCLUDED from all searches")
            print(f"    • Reports saved in symbols_prices.json as [symbol]_anylevel_liquidator_report")
            print(f"    • SIGNALS UPDATED: {stats['orders_marked_in_signals']} main orders marked in signals.json")
            print(f"    • COUNTER ORDERS UPDATED: {stats['counter_orders_marked']} counter orders marked in signals.json")
            
            print(f"\n    📊 AGE COMPARISON STATISTICS:")
            print(f"      VS CROSSED CANDLE:")
            print(f"        • Older: {stats['age_comparisons']['vs_crossed']['older']}")
            print(f"        • Younger: {stats['age_comparisons']['vs_crossed']['younger']}")
            print(f"        • Same age: {stats['age_comparisons']['vs_crossed']['sameage']}")
            print(f"        • No crosser data: {stats['age_comparisons']['vs_crossed']['no_crosser_data']}")
            print(f"      VS TRAPPED CANDLE:")
            print(f"        • Older: {stats['age_comparisons']['vs_trapped']['older']}")
            print(f"        • Younger: {stats['age_comparisons']['vs_trapped']['younger']}")
            print(f"        • Same age: {stats['age_comparisons']['vs_trapped']['sameage']}")
            print(f"        • No trapped data: {stats['age_comparisons']['vs_trapped']['no_trapped_data']}")
            
            print(f"\n    📊 FLAG FORMATS:")
            print(f"      • Main: first_both_levels_{{color}}_liquidator: true")
            print(f"      • Age vs Crossed: liquidator_{{color}}_is_{{older/younger/sameage}}_than_crossed_candle: true")
            print(f"      • Age vs Trapped: liquidator_{{color}}_is_{{older/younger/sameage}}_than_trapped_candle: true")
            print(f"      • Age rule: Higher candle number = OLDER, Lower candle number = YOUNGER")
            print(f"      • Example: liquidator_green_is_older_than_crossed_candle: true")
            print(f"      • Example: liquidator_red_is_younger_than_trapped_candle: true")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in any-level liquidator analysis: {e}")
            import traceback
            traceback.print_exc()
    
    return stats

def identify_ranging_orders_candles(inv_id=None):
    """
    Identify the FIRST ranging levels after the liquidator candle for SELECTED orders.
    
    A ranging condition is met when price stays within specific boundaries for SELECTED orders:
    
    For SELECTED ASK (Buy Stop / Sell Limit):
    - Need at least 1 candle where HIGH < order TP AND LOW < entry/exit
    - This shows price is ranging below the selected ask level
    
    For SELECTED BID (Sell Stop / Buy Limit):
    - Need at least 1 candle where LOW > order TP AND LOW > entry/exit
    - This shows price is ranging above the selected bid level
    
    SEARCH RULES:
    - Start search from candle AFTER liquidator (liquidator_candle_number + 1)
    - Search up to candle #99 (excluding current forming candle #100)
    - Need at least 1 candle meeting condition for BOTH selected orders to mark as ranging
    - Records the FIRST (most recent) occurrence where BOTH conditions are met
    
    RESULTS:
    - Updates symbols_prices.json with [symbol]_ranging_report top-level keys
    - Updates signals.json by marking the ranging orders with first_ranging_levels: true
      (only the selected orders that meet ranging conditions receive the flag)
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the ranging orders analysis
    """
    print(f"\n{'='*10} 📊 IDENTIFY FIRST RANGING LEVELS AFTER LIQUIDATOR (SELECTED ORDERS) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        "symbols_with_liquidator": 0,        # Symbols with liquidator (can analyze)
        "symbols_with_ranging": 0,            # Symbols where FIRST ranging found
        "symbols_without_ranging": 0,         # Symbols analyzed but no ranging found
        "symbols_no_liquidator": 0,           # Symbols without liquidator (can't analyze)
        "selected_orders_marked_in_signals": 0,  # Selected orders marked with ranging flag
        "counter_orders_marked": 0,            # Counter orders marked with ranging flag
        "ranging_details": {}
    }
    
    def get_candle_color(candle):
        """Determine if candle is green (bullish) or red (bearish)"""
        if candle['close'] > candle['open']:
            return "GREEN", "bullish"
        elif candle['close'] < candle['open']:
            return "RED", "bearish"
        else:
            return "NEUTRAL", "neutral"
    
    def check_selected_ask_ranging_condition(candle, selected_ask):
        """
        Check if a candle meets the ranging condition for SELECTED ASK order.
        
        Condition: Candle HIGH < order TP AND candle LOW < entry/exit
        
        Args:
            candle: Candle dictionary
            selected_ask: Selected ask order dictionary (Buy Stop or Sell Limit)
            
        Returns:
            tuple: (meets_condition, details)
        """
        # Verify this is an ask level order
        if selected_ask['order_type'] not in ['buy_stop', 'sell_limit']:
            return False, None
        
        order_entry = selected_ask['entry']
        order_exit = selected_ask['exit']
        order_tp = selected_ask['tp']
        
        # For selected ask orders, we check:
        # 1. HIGH < TP (price never reached take profit)
        # 2. LOW < entry/exit (price went below entry/exit at some point)
        # This shows price is ranging below the ask level
        
        high_condition = candle['high'] < order_tp
        low_condition = candle['low'] < min(order_entry, order_exit)  # Below either entry or exit
        
        meets_condition = high_condition and low_condition
        
        if meets_condition:
            # Determine candle color
            color_desc, color_type = get_candle_color(candle)
            
            details = {
                'order_type': selected_ask['order_type'],
                'order_entry': order_entry,
                'order_exit': order_exit,
                'order_tp': order_tp,
                'candle_high': candle['high'],
                'candle_low': candle['low'],
                'candle_open': candle['open'],
                'candle_close': candle['close'],
                'candle_color': color_desc,
                'candle_type': color_type,
                'condition': f"High ({candle['high']:.{candle.get('digits', 2)}f}) < TP ({order_tp:.{candle.get('digits', 2)}f}) AND Low ({candle['low']:.{candle.get('digits', 2)}f}) < min(entry,exit) ({min(order_entry, order_exit):.{candle.get('digits', 2)}f})",
                'level': 'selected_ask',
                'ranging_direction': 'below_ask_level'
            }
            return True, details
        
        return False, None
    
    def check_selected_bid_ranging_condition(candle, selected_bid):
        """
        Check if a candle meets the ranging condition for SELECTED BID order.
        
        Condition: Candle LOW > order TP AND candle LOW > entry/exit
        
        Args:
            candle: Candle dictionary
            selected_bid: Selected bid order dictionary (Sell Stop or Buy Limit)
            
        Returns:
            tuple: (meets_condition, details)
        """
        # Verify this is a bid level order
        if selected_bid['order_type'] not in ['sell_stop', 'buy_limit']:
            return False, None
        
        order_entry = selected_bid['entry']
        order_exit = selected_bid['exit']
        order_tp = selected_bid['tp']
        
        # For selected bid orders, we check:
        # 1. LOW > TP (price never reached take profit)
        # 2. LOW > entry/exit (price stayed above entry/exit)
        # This shows price is ranging above the bid level
        
        low_condition_1 = candle['low'] > order_tp
        low_condition_2 = candle['low'] > max(order_entry, order_exit)  # Above both entry and exit
        
        meets_condition = low_condition_1 and low_condition_2
        
        if meets_condition:
            # Determine candle color
            color_desc, color_type = get_candle_color(candle)
            
            details = {
                'order_type': selected_bid['order_type'],
                'order_entry': order_entry,
                'order_exit': order_exit,
                'order_tp': order_tp,
                'candle_high': candle['high'],
                'candle_low': candle['low'],
                'candle_open': candle['open'],
                'candle_close': candle['close'],
                'candle_color': color_desc,
                'candle_type': color_type,
                'condition': f"Low ({candle['low']:.{candle.get('digits', 2)}f}) > TP ({order_tp:.{candle.get('digits', 2)}f}) AND Low ({candle['low']:.{candle.get('digits', 2)}f}) > max(entry,exit) ({max(order_entry, order_exit):.{candle.get('digits', 2)}f})",
                'level': 'selected_bid',
                'ranging_direction': 'above_bid_level'
            }
            return True, details
        
        return False, None
    
    def mark_ranging_orders_in_signals(signals_path, symbol, selected_ask_data, selected_bid_data):
        """
        Mark the ranging selected orders in signals.json with first_ranging_levels: true.
        Marks only the selected orders, not their counters.
        
        Args:
            signals_path: Path to signals.json file
            symbol: Symbol name
            selected_ask_data: The selected ask order data that is ranging
            selected_bid_data: The selected bid order data that is ranging
            
        Returns:
            tuple: (selected_orders_marked, counter_orders_marked)
        """
        if not signals_path.exists():
            print(f"            ⚠️  signals.json not found at {signals_path}")
            return 0, 0
        
        try:
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            selected_orders_marked = 0
            counter_orders_marked = 0
            
            # Navigate to the symbol in signals.json
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Check if this symbol exists in this category
                if symbol in symbols_in_category:
                    symbol_signals = symbols_in_category[symbol]
                    
                    # Mark the selected ask order (main order only)
                    ask_entry = selected_ask_data['entry']
                    ask_order_type = selected_ask_data['order_type']
                    
                    # Check in ask_orders for selected ask
                    for ask_order in symbol_signals.get('ask_orders', []):
                        if (abs(ask_order.get('entry', 0) - ask_entry) < 0.00001 and 
                            ask_order.get('order_type') == ask_order_type):
                            if 'first_ranging_levels' not in ask_order:
                                ask_order['first_ranging_levels'] = True
                                selected_orders_marked += 1
                                print(f"            ✅ Marked selected ask order at {ask_entry} with first_ranging_levels: true")
                            
                            # IMPORTANT: DO NOT mark counter order
                            # Only the selected order gets the flag
                    
                    # Mark the selected bid order (main order only)
                    bid_entry = selected_bid_data['entry']
                    bid_order_type = selected_bid_data['order_type']
                    
                    for bid_order in symbol_signals.get('bid_orders', []):
                        if (abs(bid_order.get('entry', 0) - bid_entry) < 0.00001 and 
                            bid_order.get('order_type') == bid_order_type):
                            if 'first_ranging_levels' not in bid_order:
                                bid_order['first_ranging_levels'] = True
                                selected_orders_marked += 1
                                print(f"            ✅ Marked selected bid order at {bid_entry} with first_ranging_levels: true")
                            
                            # IMPORTANT: DO NOT mark counter order
                            # Only the selected order gets the flag
                    
                    break  # Found the symbol
            
            # Save the updated signals.json if any marks were made
            if selected_orders_marked > 0:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"            ✅ Updated signals.json with {selected_orders_marked} selected orders marked with first_ranging_levels: true")
            
            return selected_orders_marked, counter_orders_marked
            
        except Exception as e:
            print(f"             Error marking orders in signals.json: {e}")
            return 0, 0
    
    def find_first_ranging_for_selected(completed_candles_list, selected_ask, selected_bid, start_candle_number, symbol_data):
        """
        Find the FIRST (most recent) occurrence where BOTH selected orders meet ranging conditions.
        
        Args:
            completed_candles_list: List of COMPLETED candles sorted by time (newest first)
            selected_ask: Selected ask order (Buy Stop or Sell Limit)
            selected_bid: Selected bid order (Sell Stop or Buy Limit)
            start_candle_number: Candle number to start search from (liquidator candle + 1)
            symbol_data: Symbol data for formatting
            
        Returns:
            dict: First ranging analysis results or None
        """
        print(f"          🔍 Searching for FIRST ranging occurrence for SELECTED orders from #{start_candle_number} to #99...")
        
        # Filter candles from start_candle_number up to 99 (excluding current forming #100)
        search_candles = [
            c for c in completed_candles_list 
            if c['candle_number'] >= start_candle_number and c['candle_number'] < 100
        ]
        
        # Sort by candle_number ascending (oldest to newest) for sequential search
        # We want the FIRST occurrence, so we start from the earliest candle after liquidator
        search_candles_sorted = sorted(search_candles, key=lambda x: x['candle_number'])
        
        if not search_candles_sorted:
            print(f"          ⚠️  No candles found in range #{start_candle_number}-99")
            return None
        
        print(f"          📊 Scanning {len(search_candles_sorted)} candles from #{search_candles_sorted[0]['candle_number']} to #{search_candles_sorted[-1]['candle_number']}")
        print(f"          🔎 Looking for first candle where BOTH selected orders meet conditions...")
        
        # Track the first occurrence where both conditions are met
        # They don't have to be the same candle, but need to occur in sequence
        
        ask_met_candle = None
        bid_met_candle = None
        first_ask_index = None
        first_bid_index = None
        
        for idx, candle in enumerate(search_candles_sorted):
            # Check selected ask condition if not already found
            if ask_met_candle is None:
                ask_meets, ask_details = check_selected_ask_ranging_condition(candle, selected_ask)
                if ask_meets:
                    ask_met_candle = candle
                    first_ask_index = idx
                    print(f"            📈 First SELECTED ASK condition met at candle #{candle['candle_number']}")
                    print(f"              • {ask_details['condition']}")
            
            # Check selected bid condition if not already found
            if bid_met_candle is None:
                bid_meets, bid_details = check_selected_bid_ranging_condition(candle, selected_bid)
                if bid_meets:
                    bid_met_candle = candle
                    first_bid_index = idx
                    print(f"            📉 First SELECTED BID condition met at candle #{candle['candle_number']}")
                    print(f"              • {bid_details['condition']}")
            
            # If both found, we have the first occurrence
            if ask_met_candle is not None and bid_met_candle is not None:
                # Determine which candle came first (lower index)
                ranging_start_candle = search_candles_sorted[min(first_ask_index, first_bid_index)]
                
                print(f"\n          🏆 FIRST RANGING OCCURRENCE FOUND FOR SELECTED ORDERS:")
                print(f"            • Selected ASK condition first met at candle #{ask_met_candle['candle_number']}")
                print(f"            • Selected BID condition first met at candle #{bid_met_candle['candle_number']}")
                print(f"            • Ranging started from candle #{ranging_start_candle['candle_number']}")
                
                return {
                    'found': True,
                    'ask_candle': ask_met_candle,
                    'bid_candle': bid_met_candle,
                    'ask_index': first_ask_index,
                    'bid_index': first_bid_index,
                    'ranging_start_candle': ranging_start_candle,
                    'ask_details': ask_details,
                    'bid_details': bid_details,
                    'search_range': {
                        'start_candle': start_candle_number,
                        'end_candle': 99,
                        'candles_analyzed': len(search_candles_sorted)
                    }
                }
        
        print(f"\n          ⏳ No ranging occurrence found for selected orders in range #{start_candle_number}-99")
        return {
            'found': False,
            'ask_candle': ask_met_candle,
            'bid_candle': bid_met_candle,
            'ask_index': first_ask_index,
            'bid_index': first_bid_index,
            'search_range': {
                'start_candle': start_candle_number,
                'end_candle': 99,
                'candles_analyzed': len(search_candles_sorted)
            }
        }
    
    def analyze_symbol_first_ranging(symbol_data, symbol_candles_data, liquidator_report, symbol, signals_path):
        """
        Analyze FIRST ranging conditions for SELECTED orders after its liquidator candle.
        
        Args:
            symbol_data: Symbol price data from symbols_prices.json
            symbol_candles_data: Candle data from [symbol]_tf_candles
            liquidator_report: Liquidator candle report for this symbol
            symbol: Symbol name
            signals_path: Path to signals.json file
            
        Returns:
            dict: First ranging analysis results for this symbol
        """
        print(f"\n    🔍 Analyzing FIRST ranging conditions for SELECTED orders of {symbol}:")
        
        # Check if liquidator exists
        if not liquidator_report or not liquidator_report.get('summary', {}).get('has_liquidator', False):
            print(f"      ⚠️  No liquidator candle found for {symbol} - cannot analyze ranging")
            return None
        
        # Extract selected bid and ask orders from symbol_data
        grid_orders = symbol_data.get('grid_orders', {})
        bid_levels = grid_orders.get('bid_levels', [])
        ask_levels = grid_orders.get('ask_levels', [])
        
        # Find selected bid and ask orders
        selected_bid = None
        selected_ask = None
        
        for level in bid_levels:
            if level.get('selected_bid'):
                selected_bid = level
                break
        
        for level in ask_levels:
            if level.get('selected_ask'):
                selected_ask = level
                break
        
        if not selected_bid or not selected_ask:
            print(f"      ⚠️  Missing selected bid or ask order for {symbol}")
            return None
        
        print(f"      🎯 SELECTED ORDERS FOR RANGING ANALYSIS:")
        print(f"        • SELECTED BID: {selected_bid['entry']:.{symbol_data.get('digits', 2)}f} ({selected_bid['order_type']})")
        print(f"        • SELECTED ASK: {selected_ask['entry']:.{symbol_data.get('digits', 2)}f} ({selected_ask['order_type']})")
        
        # Get liquidator candle number
        liquidator_candle_number = liquidator_report['liquidator_candle']['number']
        start_search_from = liquidator_candle_number + 1
        
        print(f"\n      💧 Liquidator candle: #{liquidator_candle_number} at {liquidator_report['liquidator_candle']['time_str']}")
        print(f"      🔍 Starting search from candle #{start_search_from}")
        print(f"      ⚠️  Current forming candle (#100) EXCLUDED")
        
        # Get candles from symbol_candles_data
        if not symbol_candles_data:
            print(f"      ⚠️  No candle data found for {symbol}")
            return None
        
        # Get all candles and filter out current forming candle
        all_candles = symbol_candles_data.get('candles', [])
        completed_candles = [c for c in all_candles if not c.get('is_forming', False)]
        
        if not completed_candles:
            print(f"      ⚠️  No completed candles found for {symbol}")
            return None
        
        print(f"\n      📈 SELECTED ASK ORDER DETAILS:")
        print(f"         • Type: {selected_ask['order_type']}")
        print(f"         • Entry: {selected_ask['entry']:.{symbol_data.get('digits', 2)}f}")
        print(f"         • Exit: {selected_ask['exit']:.{symbol_data.get('digits', 2)}f}")
        print(f"         • TP: {selected_ask['tp']:.{symbol_data.get('digits', 2)}f}")
        
        print(f"\n      📉 SELECTED BID ORDER DETAILS:")
        print(f"         • Type: {selected_bid['order_type']}")
        print(f"         • Entry: {selected_bid['entry']:.{symbol_data.get('digits', 2)}f}")
        print(f"         • Exit: {selected_bid['exit']:.{symbol_data.get('digits', 2)}f}")
        print(f"         • TP: {selected_bid['tp']:.{symbol_data.get('digits', 2)}f}")
        
        # Find first ranging occurrence for selected orders
        ranging_result = find_first_ranging_for_selected(
            completed_candles,
            selected_ask,
            selected_bid,
            start_search_from,
            symbol_data
        )
        
        if ranging_result and ranging_result['found']:
            # Mark the ranging selected orders in signals.json
            selected_marked, counter_marked = mark_ranging_orders_in_signals(
                signals_path,
                symbol,
                selected_ask,
                selected_bid
            )
            
            # Update stats (counter_marked should be 0 with new logic)
            stats["selected_orders_marked_in_signals"] += selected_marked
            stats["counter_orders_marked"] += counter_marked
            
            # Create ranging report focused on selected orders
            ranging_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_type': 'first_ranging_selected_orders',
                'signals_updated': True,
                'selected_orders_marked_in_signals': selected_marked,
                'counter_orders_marked_in_signals': counter_marked,
                'liquidator_reference': {
                    'candle_number': liquidator_candle_number,
                    'candle_time': liquidator_report['liquidator_candle']['time_str']
                },
                'ranging_summary': {
                    'has_ranging': True,
                    'search_start_candle': start_search_from,
                    'search_end_candle': 99,
                    'candles_analyzed': ranging_result['search_range']['candles_analyzed'],
                    'first_occurrence': {
                        'selected_ask_candle_number': ranging_result['ask_candle']['candle_number'],
                        'selected_ask_candle_time': ranging_result['ask_candle']['time_str'],
                        'selected_bid_candle_number': ranging_result['bid_candle']['candle_number'],
                        'selected_bid_candle_time': ranging_result['bid_candle']['time_str'],
                        'ranging_start_candle': ranging_result['ranging_start_candle']['candle_number']
                    }
                },
                'selected_orders': {
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type'],
                        'exit': selected_ask['exit'],
                        'tp': selected_ask['tp'],
                        'volume': selected_ask.get('volume'),
                        'risk_in_usd': selected_ask.get('risk_in_usd')
                    },
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type'],
                        'exit': selected_bid['exit'],
                        'tp': selected_bid['tp'],
                        'volume': selected_bid.get('volume'),
                        'risk_in_usd': selected_bid.get('risk_in_usd')
                    }
                },
                'first_ranging_candles': {
                    'selected_ask_condition_candle': {
                        'number': ranging_result['ask_candle']['candle_number'],
                        'time': ranging_result['ask_candle']['time'],
                        'time_str': ranging_result['ask_candle']['time_str'],
                        'open': ranging_result['ask_candle']['open'],
                        'high': ranging_result['ask_candle']['high'],
                        'low': ranging_result['ask_candle']['low'],
                        'close': ranging_result['ask_candle']['close'],
                        'color': get_candle_color(ranging_result['ask_candle'])[0],
                        'condition_met': ranging_result['ask_details']['condition']
                    },
                    'selected_bid_condition_candle': {
                        'number': ranging_result['bid_candle']['candle_number'],
                        'time': ranging_result['bid_candle']['time'],
                        'time_str': ranging_result['bid_candle']['time_str'],
                        'open': ranging_result['bid_candle']['open'],
                        'high': ranging_result['bid_candle']['high'],
                        'low': ranging_result['bid_candle']['low'],
                        'close': ranging_result['bid_candle']['close'],
                        'color': get_candle_color(ranging_result['bid_candle'])[0],
                        'condition_met': ranging_result['bid_details']['condition']
                    }
                },
                'note': 'Only selected bid and ask orders are analyzed for ranging conditions. Only selected orders receive the first_ranging_levels: true flag (counters are NOT marked).'
            }
            
            return ranging_report
        else:
            print(f"\n      ⏳ No ranging occurrence found for selected orders of {symbol}")
            
            # Create empty ranging report
            empty_report = {
                'symbol': symbol,
                'analysis_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'analysis_type': 'first_ranging_selected_orders',
                'signals_updated': False,
                'selected_orders_marked_in_signals': 0,
                'counter_orders_marked_in_signals': 0,
                'liquidator_reference': {
                    'candle_number': liquidator_candle_number,
                    'candle_time': liquidator_report['liquidator_candle']['time_str']
                },
                'ranging_summary': {
                    'has_ranging': False,
                    'search_start_candle': start_search_from,
                    'search_end_candle': 99,
                    'candles_analyzed': ranging_result['search_range']['candles_analyzed'] if ranging_result else 0
                },
                'selected_orders': {
                    'ask': {
                        'entry': selected_ask['entry'],
                        'order_type': selected_ask['order_type']
                    },
                    'bid': {
                        'entry': selected_bid['entry'],
                        'order_type': selected_bid['order_type']
                    }
                },
                'first_ranging_candles': None,
                'note': 'No ranging occurrence found for selected orders in candles after liquidator'
            }
            
            return empty_report
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to symbols_prices.json
        symbols_prices_path = prices_dir / "symbols_prices.json"
        signals_path = prices_dir / "signals.json"
        
        if not symbols_prices_path.exists():
            print(f" [{inv_id}]  symbols_prices.json not found at {symbols_prices_path}")
            return stats
        
        try:
            # Load the file
            print(f" [{inv_id}] 📂 Loading symbols_prices.json...")
            with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                symbols_prices_data = json.load(f)
            
            # Extract symbols data, candle data, and liquidator reports
            metadata_keys = ['account_type', 'account_login', 'account_server', 'account_balance', 
                           'account_currency', 'collected_at', 'grid_configuration', 
                           'target_risk_range_usd', 'total_categories', 'total_symbols',
                           'successful_symbols', 'failed_symbols', 'success_rate_percent', 'categories',
                           'candles_fetch_metadata', 'crosser_analysis_metadata', 
                           'trapped_analysis_metadata', 'liquidator_analysis_metadata',
                           'anylevel_liquidator_metadata', 'ranging_analysis_metadata']
            
            symbols_dict = {}
            candles_dict = {}
            liquidator_reports = {}
            
            for key, value in symbols_prices_data.items():
                if key in metadata_keys:
                    continue
                elif key.endswith('_tf_candles'):
                    symbol_name = key.replace('_tf_candles', '')
                    candles_dict[symbol_name] = value
                elif key.endswith('_anylevel_liquidator_report'):
                    symbol_name = key.replace('_anylevel_liquidator_report', '')
                    liquidator_reports[symbol_name] = value
                elif key.endswith('_crossedcandle_report') or \
                     key.endswith('_trappedcandle_report') or \
                     key.endswith('_ranging_report'):
                    continue
                else:
                    symbols_dict[key] = value
            
            print(f"  📊 Found {len(symbols_dict)} symbols, {len(candles_dict)} candle datasets, and {len(liquidator_reports)} liquidator reports")
            
            # Track reports to add
            reports_added = 0
            
            for symbol, symbol_data in symbols_dict.items():
                stats["total_symbols_analyzed"] += 1
                
                # Get candle data for this symbol
                symbol_candles_data = candles_dict.get(symbol)
                
                if not symbol_candles_data:
                    print(f"  ⚠️  No candle data found for {symbol}")
                    stats["symbols_no_liquidator"] += 1
                    continue
                
                # Get liquidator report for this symbol
                liquidator_report = liquidator_reports.get(symbol)
                
                if not liquidator_report:
                    print(f"  ⚠️  No liquidator report found for {symbol} - cannot analyze ranging")
                    stats["symbols_no_liquidator"] += 1
                    continue
                
                stats["symbols_with_liquidator"] += 1
                
                # Analyze first ranging for selected orders of this symbol
                ranging_report = analyze_symbol_first_ranging(
                    symbol_data, symbol_candles_data, liquidator_report, symbol, signals_path
                )
                
                if ranging_report:
                    # Add the report as a top-level key
                    report_key = f"{symbol}_ranging_report"
                    symbols_prices_data[report_key] = ranging_report
                    reports_added += 1
                    
                    # Update statistics
                    if ranging_report['ranging_summary']['has_ranging']:
                        stats["symbols_with_ranging"] += 1
                        
                        # Store details
                        stats["ranging_details"][symbol] = {
                            'has_ranging': True,
                            'selected_ask_candle': ranging_report['first_ranging_candles']['selected_ask_condition_candle']['number'],
                            'selected_bid_candle': ranging_report['first_ranging_candles']['selected_bid_condition_candle']['number'],
                            'ranging_start': ranging_report['ranging_summary']['first_occurrence']['ranging_start_candle'],
                            'selected_orders_marked': ranging_report['selected_orders_marked_in_signals']
                        }
                    else:
                        stats["symbols_without_ranging"] += 1
                        
                        stats["ranging_details"][symbol] = {
                            'has_ranging': False,
                            'liquidator_candle': ranging_report['liquidator_reference']['candle_number']
                        }
            
            # Save the updated symbols_prices.json
            if reports_added > 0:
                symbols_prices_data['ranging_analysis_metadata'] = {
                    'analyzed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'total_symbols_analyzed': stats['total_symbols_analyzed'],
                    'symbols_with_liquidator': stats['symbols_with_liquidator'],
                    'symbols_with_ranging': stats['symbols_with_ranging'],
                    'symbols_without_ranging': stats['symbols_without_ranging'],
                    'symbols_no_liquidator': stats['symbols_no_liquidator'],
                    'selected_orders_marked_in_signals': stats['selected_orders_marked_in_signals'],
                    'reports_added': reports_added,
                    'note': 'First ranging analysis starts from candle after liquidator. ONLY selected bid and ask orders are analyzed. Only selected orders receive the first_ranging_levels: true flag (counters are NOT marked). Reports stored as [symbol]_ranging_report.'
                }
                
                # Save back to file
                with open(symbols_prices_path, 'w', encoding='utf-8') as f:
                    json.dump(symbols_prices_data, f, indent=4, default=str)
                
                print(f"\n  ✅ Updated symbols_prices.json with {reports_added} ranging_report entries")
                print(f"  ✅ Updated signals.json with {stats['selected_orders_marked_in_signals']} selected orders marked with first_ranging_levels: true")
                print(f"      (Counter orders are NOT marked - only selected orders)")
            
            # Print summary
            print(f"\n  📊 FIRST RANGING ANALYSIS SUMMARY (SELECTED ORDERS ONLY):")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            print(f"    • Symbols with liquidator: {stats['symbols_with_liquidator']}")
            print(f"    • Symbols WITH FIRST RANGING: {stats['symbols_with_ranging']}")
            print(f"    • Symbols WITHOUT ranging: {stats['symbols_without_ranging']}")
            print(f"    • Symbols no liquidator: {stats['symbols_no_liquidator']}")
            print(f"    • SELECTED ORDERS MARKED: {stats['selected_orders_marked_in_signals']}")
            print(f"    • Flag added: first_ranging_levels: true to SELECTED orders only")
            print(f"    • Counter orders are NOT marked with ranging flag")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in ranging analysis: {e}")
            import traceback
            traceback.print_exc()
    
    return stats

def remove_ranging_levels(inv_id=None):
    """
    Remove all orders at price levels that have first_ranging_levels: true flag.
    
    This function works independently to clean up ranging levels from signals.json.
    It removes entire levels including main orders and their counters when the
    first_ranging_levels flag is true on either the main order or its counter.
    
    EXECUTION LOGIC:
    - Scans all bid_orders and ask_orders for first_ranging_levels: true flags
    - Collects all entry prices where this flag appears (on main orders or counters)
    - Removes ALL orders (both main and counter) at those price levels
    - When a main order is removed and its counter is at a non-ranging level:
      * The counter is promoted to become the main order
    - When a counter order is removed:
      * The order_counter field is removed from the main order
    
    Empty bid_orders or ask_orders arrays are removed entirely to clean up the structure.
    
    Args:
        inv_id: Optional specific investor ID to process.
                If None, processes all investors in INV_PATH.
        
    Returns:
        dict: Statistics about the ranging levels removal
    """
    print(f"\n{'='*10} 🧹 RANGING LEVELS CLEANUP - REMOVE ALL RANGING LEVELS {'='*10}")
    
    # Statistics for this run
    stats = {
        "investors_processed": 0,
        "total_symbols_analyzed": 0,
        "symbols_with_ranging_cleanup": 0,
        "ranging_levels_removed": 0,
        "ranging_main_orders_removed": 0,
        "ranging_counter_orders_removed": 0,
        "ranging_other_orders_same_entry": 0,
        "bid_orders_removed": 0,
        "ask_orders_removed": 0,
        "bid_orders_promoted": 0,
        "ask_orders_promoted": 0,
        "empty_bid_arrays_removed": 0,
        "empty_ask_arrays_removed": 0,
        "investor_details": {}
    }
    
    # Determine which investors to process
    investors_to_process = []
    
    if inv_id:
        # Process single investor
        inv_path = Path(INV_PATH) / inv_id
        if inv_path.exists():
            investors_to_process = [inv_id]
        else:
            print(f" Investor {inv_id} not found at {inv_path}")
            return stats
    else:
        # Process all investors
        inv_base = Path(INV_PATH)
        if inv_base.exists():
            investors_to_process = [d.name for d in inv_base.iterdir() if d.is_dir()]
            print(f"📂 Found {len(investors_to_process)} investors to process")
        else:
            print(f" Investor path not found: {INV_PATH}")
            return stats
    
    # Process each investor
    for current_inv_id in investors_to_process:
        print(f"\n [{current_inv_id}] 🔍 Processing investor for ranging level cleanup...")
        
        inv_root = Path(INV_PATH) / current_inv_id
        prices_dir = inv_root / "prices"
        signals_path = prices_dir / "signals.json"
        
        # Initialize investor stats
        investor_stats = {
            "symbols_processed": 0,
            "symbols_with_ranging": 0,
            "ranging_levels_removed": 0,
            "bid_orders_removed": 0,
            "ask_orders_removed": 0,
            "bid_orders_promoted": 0,
            "ask_orders_promoted": 0,
            "symbols": {}
        }
        
        # Check if signals.json exists
        if not signals_path.exists():
            print(f" [{current_inv_id}] ⚠️ signals.json not found at {signals_path}")
            continue
        
        try:
            print(f" [{current_inv_id}] 📂 Loading signals.json...")
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            # Track if any changes were made for this investor
            changes_made = False
            
            # Process each category and symbol
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Create a list of symbols to process
                symbols_to_process = list(symbols_in_category.keys())
                
                for symbol in symbols_to_process:
                    symbol_signals = symbols_in_category[symbol]
                    stats["total_symbols_analyzed"] += 1
                    investor_stats["symbols_processed"] += 1
                    
                    print(f"\n    🔍 Analyzing {symbol} for ranging levels:")
                    
                    # Collect all entry prices that have first_ranging_levels: true
                    ranging_levels_to_remove = set()
                    
                    # Check bid orders for ranging flags
                    for order in symbol_signals.get('bid_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in bid_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in bid_orders counter")
                    
                    # Check ask orders for ranging flags
                    for order in symbol_signals.get('ask_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in ask_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in ask_orders counter")
                    
                    if ranging_levels_to_remove:
                        print(f"      🧹 Cleaning up {len(ranging_levels_to_remove)} ranging levels: {sorted(ranging_levels_to_remove)}")
                        stats["symbols_with_ranging_cleanup"] += 1
                        investor_stats["symbols_with_ranging"] += 1
                        stats["ranging_levels_removed"] += len(ranging_levels_to_remove)
                        investor_stats["ranging_levels_removed"] += len(ranging_levels_to_remove)
                        
                        # Track symbol details
                        symbol_detail = {
                            "ranging_levels": sorted(list(ranging_levels_to_remove)),
                            "bid_orders_before": 0,
                            "ask_orders_before": 0,
                            "bid_orders_after": 0,
                            "ask_orders_after": 0,
                            "bid_orders_removed": 0,
                            "ask_orders_removed": 0,
                            "bid_orders_promoted": 0,
                            "ask_orders_promoted": 0
                        }
                        
                        # Process BID ORDERS - remove any order at ranging levels
                        original_bid_count = len(symbol_signals.get('bid_orders', []))
                        symbol_detail["bid_orders_before"] = original_bid_count
                        
                        updated_bid_orders = []
                        
                        for order in symbol_signals.get('bid_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level bid order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["bid_orders_removed"] += 1
                                investor_stats["bid_orders_removed"] += 1
                                symbol_detail["bid_orders_removed"] += 1
                                
                                # Count as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should be promoted (counter at non-ranging level)
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price not in ranging_levels_to_remove:
                                        # Counter is at a non-ranging level - promote it
                                        counter = order['order_counter']
                                        counter['promoted_from_counter'] = True
                                        counter['original_order_removed_at'] = order_price
                                        counter['removal_reason'] = 'ranging_level_cleanup'
                                        updated_bid_orders.append(counter)
                                        stats["bid_orders_promoted"] += 1
                                        investor_stats["bid_orders_promoted"] += 1
                                        symbol_detail["bid_orders_promoted"] += 1
                                        print(f"          🔄 Promoted counter order at {counter_price} to main position")
                                    else:
                                        # Counter is also at ranging level - it will be removed
                                        print(f"          Counter order at {counter_price} also at ranging level - removed")
                                        stats["ranging_counter_orders_removed"] += 1
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept bid order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["bid_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_bid_orders.append(order)
                        
                        # Process ASK ORDERS - remove any order at ranging levels
                        original_ask_count = len(symbol_signals.get('ask_orders', []))
                        symbol_detail["ask_orders_before"] = original_ask_count
                        
                        updated_ask_orders = []
                        
                        for order in symbol_signals.get('ask_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level ask order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["ask_orders_removed"] += 1
                                investor_stats["ask_orders_removed"] += 1
                                symbol_detail["ask_orders_removed"] += 1
                                
                                # Count as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should be promoted (counter at non-ranging level)
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price not in ranging_levels_to_remove:
                                        # Counter is at a non-ranging level - promote it
                                        counter = order['order_counter']
                                        counter['promoted_from_counter'] = True
                                        counter['original_order_removed_at'] = order_price
                                        counter['removal_reason'] = 'ranging_level_cleanup'
                                        updated_ask_orders.append(counter)
                                        stats["ask_orders_promoted"] += 1
                                        investor_stats["ask_orders_promoted"] += 1
                                        symbol_detail["ask_orders_promoted"] += 1
                                        print(f"          🔄 Promoted counter order at {counter_price} to main position")
                                    else:
                                        # Counter is also at ranging level - it will be removed
                                        print(f"          Counter order at {counter_price} also at ranging level - removed")
                                        stats["ranging_counter_orders_removed"] += 1
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept ask order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["ask_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_ask_orders.append(order)
                        
                        # Update the orders after ranging cleanup
                        if updated_bid_orders:
                            symbol_signals['bid_orders'] = updated_bid_orders
                            symbol_detail["bid_orders_after"] = len(updated_bid_orders)
                        else:
                            if 'bid_orders' in symbol_signals:
                                del symbol_signals['bid_orders']
                                stats["empty_bid_arrays_removed"] += 1
                                print(f"      🗑️  Removed empty bid_orders array for {symbol}")
                                symbol_detail["bid_orders_after"] = 0
                        
                        if updated_ask_orders:
                            symbol_signals['ask_orders'] = updated_ask_orders
                            symbol_detail["ask_orders_after"] = len(updated_ask_orders)
                        else:
                            if 'ask_orders' in symbol_signals:
                                del symbol_signals['ask_orders']
                                stats["empty_ask_arrays_removed"] += 1
                                print(f"      🗑️  Removed empty ask_orders array for {symbol}")
                                symbol_detail["ask_orders_after"] = 0
                        
                        # Store symbol details
                        investor_stats["symbols"][symbol] = symbol_detail
                        changes_made = True
                        
                        print(f"      ✅ Ranging cleanup complete for {symbol}")
            
            # Save the updated signals.json if changes were made
            if changes_made:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"\n [{current_inv_id}] ✅ Updated signals.json with ranging level cleanup")
                
                # Store investor stats
                stats["investor_details"][current_inv_id] = investor_stats
                stats["investors_processed"] += 1
            else:
                print(f"\n [{current_inv_id}] ⏳ No ranging levels found - no changes needed")
            
        except Exception as e:
            print(f" [{current_inv_id}]  Error in ranging level cleanup: {e}")
            import traceback
            traceback.print_exc()
    
    # Print final summary
    print(f"\n{'='*10} 📊 RANGING LEVELS CLEANUP SUMMARY {'='*10}")
    print(f"  • Investors processed: {stats['investors_processed']}")
    print(f"  • Total symbols analyzed: {stats['total_symbols_analyzed']}")
    print(f"  • Symbols with ranging cleanup: {stats['symbols_with_ranging_cleanup']}")
    print(f"  • Ranging levels removed: {stats['ranging_levels_removed']}")
    print(f"  • Main orders removed: {stats['ranging_main_orders_removed']}")
    print(f"  • Counter orders removed: {stats['ranging_counter_orders_removed']}")
    print(f"  • Other orders at same entry: {stats['ranging_other_orders_same_entry']}")
    print(f"  • Bid orders removed: {stats['bid_orders_removed']}")
    print(f"  • Ask orders removed: {stats['ask_orders_removed']}")
    print(f"  • Bid orders promoted: {stats['bid_orders_promoted']}")
    print(f"  • Ask orders promoted: {stats['ask_orders_promoted']}")
    print(f"  • Empty bid arrays removed: {stats['empty_bid_arrays_removed']}")
    print(f"  • Empty ask arrays removed: {stats['empty_ask_arrays_removed']}")
    
    if stats["investor_details"]:
        print(f"\n  📋 DETAILS BY INVESTOR:")
        for inv, inv_stats in stats["investor_details"].items():
            print(f"    [{inv}]")
            print(f"      • Symbols processed: {inv_stats['symbols_processed']}")
            print(f"      • Symbols with ranging: {inv_stats['symbols_with_ranging']}")
            print(f"      • Ranging levels removed: {inv_stats['ranging_levels_removed']}")
            print(f"      • Bid orders removed/promoted: {inv_stats['bid_orders_removed']}/{inv_stats['bid_orders_promoted']}")
            print(f"      • Ask orders removed/promoted: {inv_stats['ask_orders_removed']}/{inv_stats['ask_orders_promoted']}")
    
    return stats

def orders_configuration(inv_id=None):
    """
    Configure orders based on liquidator candle colors from anylevel_liquidator analysis.
    
    EXECUTION ORDER:
    1. FIRST: Check accountmanagement.json settings.enable_orders_configuration flag
       - If false or missing, skip orders configuration for this investor
    
    2. SECOND: RANGING LEVELS CLEANUP - Remove all orders (both main and counter) at any price level
       that has first_ranging_levels: true flag. This completely removes the entire level including
       the main order and its counter.
    
    3. THIRD: Check if symbol has trapped candles OR crossed candles with liquidator age comparison:
       - If EITHER exists:
         * Check if liquidator exists and is OLDER than crossed candle
         * If YES (older) → PROCEED TO STEP 4 (execute liquidator)
         * If NO (younger or no liquidator) → SKIP (leave orders as is)
       - If NEITHER exists → PROCEED TO STEP 4
    
    4. FOURTH: EXECUTE LIQUIDATOR CONFIGURATION based on liquidator candle color:
        For GREEN liquidator flag (bullish candle):
        - Removes ALL SELL orders (sell_stop, sell_limit) that are AT or ABOVE the flagged SELL order's entry price
        - This includes the flagged order itself and any SELL orders with higher entry prices
        - Keeps SELL orders that are BELOW the flagged order's entry price
        - Keeps ALL BUY orders regardless of price
        
        For RED liquidator flag (bearish candle):
        - Removes ALL BUY orders (buy_stop, buy_limit) that are AT or BELOW the flagged BUY order's entry price
        - This includes the flagged order itself and any BUY orders with lower entry prices
        - Keeps BUY orders that are ABOVE the flagged order's entry price
        - Keeps ALL SELL orders regardless of price
    
    When a main order is removed:
    - If it has a counter order that should be kept, that counter order is promoted to become the main order
    - The promoted order retains all its original properties including flags
    
    When a counter order is removed:
    - The order_counter field is removed from the main order
    
    Empty bid_orders or ask_orders arrays are removed entirely to clean up the structure.
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the order configuration
    """
    print(f"\n{'='*10} 🚩 ORDERS CONFIGURATION - WITH LIQUIDATOR AGE CHECK {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        # Account config check stats
        "investors_skipped_due_to_settings": 0,
        # Ranging cleanup stats
        "symbols_with_ranging_cleanup": 0,
        "ranging_levels_removed": 0,
        "ranging_main_orders_removed": 0,
        "ranging_counter_orders_removed": 0,
        "ranging_other_orders_same_entry": 0,
        # Trapped/crossed stats with liquidator age comparison
        "symbols_with_trapped_or_crossed": 0,
        "symbols_with_trapped_or_crossed_and_liquidator_older": 0,  # These will execute liquidator
        "symbols_skipped_due_to_trapped": 0,
        "symbols_skipped_due_to_crossed": 0,
        "symbols_skipped_due_to_both": 0,
        "symbols_skipped_due_to_liquidator_younger": 0,  # New: had flags but younger
        "symbols_skipped_due_to_no_liquidator": 0,  # New: had flags but no liquidator
        "liquidator_age_comparisons": {
            "older_count": 0,
            "younger_count": 0,
            "sameage_count": 0,
            "no_liquidator_count": 0
        },
        # Liquidator processed stats
        "symbols_without_trapped_or_crossed": 0,
        "symbols_with_liquidator_flags": 0,
        "symbols_without_liquidator_flags": 0,
        "total_flagged_orders_found": 0,
        "green_flagged_orders": 0,
        "red_flagged_orders": 0,
        "removal_thresholds": {
            "green": {},  # Will store {symbol: threshold_price}
            "red": {}     # Will store {symbol: threshold_price}
        },
        "orders_modified": {
            "green": {
                "sells_removed": 0,
                "sells_preserved_below": 0,
                "promotions_to_main": 0
            },
            "red": {
                "buys_removed": 0,
                "buys_preserved_above": 0,
                "promotions_to_main": 0
            }
        },
        "bid_orders_removed": 0,
        "ask_orders_removed": 0,
        "bid_orders_preserved": 0,
        "ask_orders_preserved": 0,
        "bid_orders_promoted": 0,
        "ask_orders_promoted": 0,
        "empty_bid_arrays_removed": 0,
        "empty_ask_arrays_removed": 0,
        "symbol_details": {}
    }
    
    def determine_age_relationship(liquidator_candle_number, crossed_candle_number):
        """
        Determine if liquidator is older, younger, or same age as crossed candle.
        
        Age logic: Higher candle number = OLDER (further in past)
                  Lower candle number = YOUNGER (more recent)
        
        Args:
            liquidator_candle_number: Candle number of liquidator
            crossed_candle_number: Candle number of crossed candle
            
        Returns:
            str: 'older', 'younger', or 'sameage'
        """
        if liquidator_candle_number > crossed_candle_number:
            return "older"
        elif liquidator_candle_number < crossed_candle_number:
            return "younger"
        else:
            return "sameage"
    
    def get_liquidator_candle_info(symbols_prices_data, symbol):
        """
        Extract liquidator candle information from symbol's anylevel_liquidator_report.
        
        Args:
            symbols_prices_data: Complete symbols_prices.json data
            symbol: Symbol name
            
        Returns:
            dict: Liquidator candle info or None
        """
        report_key = f"{symbol}_anylevel_liquidator_report"
        if report_key in symbols_prices_data:
            report = symbols_prices_data[report_key]
            if report.get('liquidator_candle') and report.get('summary', {}).get('has_liquidator'):
                return {
                    'candle_number': report['liquidator_candle']['number'],
                    'candle_time': report['liquidator_candle']['time_str'],
                    'candle_color': report['liquidator_candle']['color'],
                    'flags_added': report.get('flags_added', {}),
                    'age_comparisons': report.get('age_comparisons', {})
                }
        return None
    
    def get_crossed_candle_info(symbols_prices_data, symbol):
        """
        Extract crossed candle information from symbol's crossedcandle_report.
        
        Args:
            symbols_prices_data: Complete symbols_prices.json data
            symbol: Symbol name
            
        Returns:
            dict: Crossed candle info or None
        """
        report_key = f"{symbol}_crossedcandle_report"
        if report_key in symbols_prices_data:
            report = symbols_prices_data[report_key]
            if report.get('winning_candle'):
                return {
                    'candle_number': report['winning_candle']['number'],
                    'candle_time': report['winning_candle']['time_str'],
                    'winner_type': report.get('race_summary', {}).get('winner_type'),
                    'cross_direction': report.get('race_summary', {}).get('cross_direction')
                }
        return None
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to accountmanagement.json, signals.json and symbols_prices.json
        acc_mgmt_path = inv_root / "accountmanagement.json"
        signals_path = prices_dir / "signals.json"
        symbols_prices_path = prices_dir / "symbols_prices.json"
        
        # =====================================================
        # STEP 1: Check account management settings
        # =====================================================
        print(f"\n [{inv_id}] 🔍 Checking account management settings...")
        
        if not acc_mgmt_path.exists():
            print(f" [{inv_id}] ⚠️ accountmanagement.json not found at {acc_mgmt_path}")
            print(f" [{inv_id}] ⏭️ Skipping orders configuration (no settings to check)")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_config = json.load(f)
            
            # Check if orders configuration is enabled
            settings = acc_config.get("settings", {})
            enable_orders_config = settings.get("enable_orders_configuration", False)
            
            if not enable_orders_config:
                print(f" [{inv_id}] ⏭️ Orders configuration is DISABLED in accountmanagement.json")
                print(f"     (settings.enable_orders_configuration = false)")
                stats["investors_skipped_due_to_settings"] += 1
                return stats
            
            print(f" [{inv_id}] ✅ Orders configuration is ENABLED - proceeding with processing")
            
        except json.JSONDecodeError as e:
            print(f" [{inv_id}]  Invalid JSON in accountmanagement.json: {e}")
            print(f" [{inv_id}] ⏭️ Skipping orders configuration due to config error")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        except Exception as e:
            print(f" [{inv_id}]  Error reading accountmanagement.json: {e}")
            print(f" [{inv_id}] ⏭️ Skipping orders configuration due to config error")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        
        # Continue with rest of processing if enabled
        if not signals_path.exists():
            print(f" [{inv_id}]  signals.json not found at {signals_path}")
            return stats
        
        if not symbols_prices_path.exists():
            print(f" [{inv_id}] ⚠️ symbols_prices.json not found - will skip liquidator age comparisons")
            symbols_prices_data = {}
        else:
            try:
                with open(symbols_prices_path, 'r', encoding='utf-8') as f:
                    symbols_prices_data = json.load(f)
                print(f" [{inv_id}] 📂 Loaded symbols_prices.json for liquidator age data")
            except:
                symbols_prices_data = {}
                print(f" [{inv_id}] ⚠️ Could not load symbols_prices.json - will skip liquidator age comparisons")
        
        try:
            print(f" [{inv_id}] 📂 Loading signals.json...")
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            # Track if any changes were made
            changes_made = False
            
            # Process each category and symbol
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Create a list of symbols to process
                symbols_to_process = list(symbols_in_category.keys())
                
                for symbol in symbols_to_process:
                    symbol_signals = symbols_in_category[symbol]
                    stats["total_symbols_analyzed"] += 1
                    
                    print(f"\n    🔍 Analyzing {symbol} for order configuration:")
                    
                    # =====================================================
                    # STEP 2: RANGING LEVELS CLEANUP - Remove all orders at levels with first_ranging_levels: true
                    # =====================================================
                    print(f"      🔍 STEP 2: Checking for ranging levels to clean up...")
                    
                    # Collect all entry prices that have first_ranging_levels: true
                    ranging_levels_to_remove = set()
                    
                    # Check bid orders for ranging flags
                    for order in symbol_signals.get('bid_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in bid_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in bid_orders counter")
                    
                    # Check ask orders for ranging flags
                    for order in symbol_signals.get('ask_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in ask_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in ask_orders counter")
                    
                    if ranging_levels_to_remove:
                        print(f"      🧹 Cleaning up {len(ranging_levels_to_remove)} ranging levels: {sorted(ranging_levels_to_remove)}")
                        stats["symbols_with_ranging_cleanup"] += 1
                        stats["ranging_levels_removed"] += len(ranging_levels_to_remove)
                        
                        # Process BID ORDERS - remove any order at ranging levels
                        original_bid_count = len(symbol_signals.get('bid_orders', []))
                        updated_bid_orders = []
                        
                        for order in symbol_signals.get('bid_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level bid order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["bid_orders_removed"] += 1
                                
                                # Also count any other orders at same price as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should also be removed
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price in ranging_levels_to_remove:
                                        # Counter is at same ranging level, it will be handled separately
                                        print(f"          Counter order at {counter_price} will be handled separately")
                                    # Don't promote counter - entire level is removed
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept bid order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["bid_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_bid_orders.append(order)
                        
                        # Process ASK ORDERS - remove any order at ranging levels
                        original_ask_count = len(symbol_signals.get('ask_orders', []))
                        updated_ask_orders = []
                        
                        for order in symbol_signals.get('ask_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level ask order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["ask_orders_removed"] += 1
                                
                                # Also count any other orders at same price as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should also be removed
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price in ranging_levels_to_remove:
                                        # Counter is at same ranging level, it will be handled separately
                                        print(f"          Counter order at {counter_price} will be handled separately")
                                    # Don't promote counter - entire level is removed
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept ask order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["ask_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_ask_orders.append(order)
                        
                        # Update the orders after ranging cleanup
                        if updated_bid_orders:
                            symbol_signals['bid_orders'] = updated_bid_orders
                        else:
                            if 'bid_orders' in symbol_signals:
                                del symbol_signals['bid_orders']
                                print(f"      🗑️  Removed empty bid_orders array for {symbol} after ranging cleanup")
                        
                        if updated_ask_orders:
                            symbol_signals['ask_orders'] = updated_ask_orders
                        else:
                            if 'ask_orders' in symbol_signals:
                                del symbol_signals['ask_orders']
                                print(f"      🗑️  Removed empty ask_orders array for {symbol} after ranging cleanup")
                        
                        changes_made = True
                        
                        # After cleanup, reload symbol_signals for next steps
                        print(f"      ✅ Ranging cleanup complete for {symbol}")
                    
                    # =====================================================
                    # STEP 3: Check if symbol has trapped candles OR crossed candles
                    # and apply liquidator age comparison
                    # =====================================================
                    has_trapped_candles = False
                    has_crossed_candle = False
                    has_liquidator_flag_in_orders = False
                    
                    # First check if there are any liquidator flags in orders
                    for order in symbol_signals.get('bid_orders', []):
                        if order.get('first_both_levels_green_liquidator') is True or \
                           order.get('first_both_levels_red_liquidator') is True:
                            has_liquidator_flag_in_orders = True
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True or \
                               order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                has_liquidator_flag_in_orders = True
                    
                    for order in symbol_signals.get('ask_orders', []):
                        if order.get('first_both_levels_green_liquidator') is True or \
                           order.get('first_both_levels_red_liquidator') is True:
                            has_liquidator_flag_in_orders = True
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True or \
                               order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                has_liquidator_flag_in_orders = True
                    
                    # Check for trapped/crossed flags
                    for order in symbol_signals.get('bid_orders', []):
                        if order.get('first_trapped_levels') is True:
                            has_trapped_candles = True
                            print(f"      🪤 Found order with first_trapped_levels: true in bid_orders")
                        if order.get('first_most_recent_crossed_candle_orders') is True:
                            has_crossed_candle = True
                            print(f"      🏁 Found order with first_most_recent_crossed_candle_orders: true in bid_orders")
                        
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_trapped_levels') is True:
                                has_trapped_candles = True
                                print(f"      🪤 Found counter order with first_trapped_levels: true in bid_orders")
                            if order['order_counter'].get('first_most_recent_crossed_candle_orders') is True:
                                has_crossed_candle = True
                                print(f"      🏁 Found counter order with first_most_recent_crossed_candle_orders: true in bid_orders")
                    
                    for order in symbol_signals.get('ask_orders', []):
                        if order.get('first_trapped_levels') is True:
                            has_trapped_candles = True
                            print(f"      🪤 Found order with first_trapped_levels: true in ask_orders")
                        if order.get('first_most_recent_crossed_candle_orders') is True:
                            has_crossed_candle = True
                            print(f"      🏁 Found order with first_most_recent_crossed_candle_orders: true in ask_orders")
                        
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_trapped_levels') is True:
                                has_trapped_candles = True
                                print(f"      🪤 Found counter order with first_trapped_levels: true in ask_orders")
                            if order['order_counter'].get('first_most_recent_crossed_candle_orders') is True:
                                has_crossed_candle = True
                                print(f"      🏁 Found counter order with first_most_recent_crossed_candle_orders: true in ask_orders")
                    
                    # =====================================================
                    # STEP 3B: Apply liquidator age comparison if trapped/crossed exist
                    # =====================================================
                    execute_liquidator = False
                    skip_reason = ""
                    
                    if has_trapped_candles or has_crossed_candle:
                        stats["symbols_with_trapped_or_crossed"] += 1
                        
                        # Get liquidator info for age comparison
                        liquidator_info = get_liquidator_candle_info(symbols_prices_data, symbol)
                        
                        # Get crossed candle info if crossed exists
                        crossed_info = get_crossed_candle_info(symbols_prices_data, symbol) if has_crossed_candle else None
                        
                        if has_liquidator_flag_in_orders and liquidator_info and crossed_info:
                            # We have both liquidator flags and crossed candle data
                            liquidator_candle_num = liquidator_info['candle_number']
                            crossed_candle_num = crossed_info['candle_number']
                            
                            age_relationship = determine_age_relationship(liquidator_candle_num, crossed_candle_num)
                            stats["liquidator_age_comparisons"][f"{age_relationship}_count"] += 1
                            
                            print(f"      🔍 Age comparison: Liquidator #{liquidator_candle_num} vs Crossed #{crossed_candle_num} = {age_relationship}")
                            
                            if age_relationship == "older":
                                # Liquidator is OLDER - EXECUTE liquidator despite trapped/crossed
                                execute_liquidator = True
                                stats["symbols_with_trapped_or_crossed_and_liquidator_older"] += 1
                                skip_reason = f"LIQUIDATOR OLDER (liquidator #{liquidator_candle_num} > crossed #{crossed_candle_num}) - EXECUTING"
                                print(f"      ✅ {skip_reason}")
                            elif age_relationship == "younger":
                                # Liquidator is YOUNGER - SKIP
                                execute_liquidator = False
                                stats["symbols_skipped_due_to_liquidator_younger"] += 1
                                skip_reason = f"LIQUIDATOR YOUNGER (liquidator #{liquidator_candle_num} < crossed #{crossed_candle_num}) - SKIPPING"
                                print(f"      ⏭️ {skip_reason}")
                            else:  # sameage
                                # Same age - consider as older? Let's execute to be safe
                                execute_liquidator = True
                                stats["symbols_with_trapped_or_crossed_and_liquidator_older"] += 1
                                skip_reason = f"LIQUIDATOR SAME AGE (liquidator #{liquidator_candle_num} = crossed #{crossed_candle_num}) - EXECUTING"
                                print(f"      ✅ {skip_reason}")
                        
                        elif has_liquidator_flag_in_orders and not crossed_info:
                            # Have liquidator flags but no crossed data - SKIP (can't compare)
                            execute_liquidator = False
                            stats["symbols_skipped_due_to_no_liquidator"] += 1
                            skip_reason = "LIQUIDATOR FLAGS EXIST BUT NO CROSSED DATA - SKIPPING"
                            print(f"      ⏭️ {skip_reason}")
                        
                        elif not has_liquidator_flag_in_orders:
                            # No liquidator flags at all - SKIP
                            execute_liquidator = False
                            stats["symbols_skipped_due_to_no_liquidator"] += 1
                            skip_reason = "NO LIQUIDATOR FLAGS - SKIPPING"
                            print(f"      ⏭️ {skip_reason}")
                        
                        else:
                            # Default case - SKIP
                            execute_liquidator = False
                            stats["symbols_skipped_due_to_no_liquidator"] += 1
                            skip_reason = "TRAPPED OR CROSSED EXISTS - SKIPPING (no liquidator age override)"
                            print(f"      ⏭️ {skip_reason}")
                        
                        # Store stats for skip reasons
                        if has_trapped_candles and has_crossed_candle:
                            stats["symbols_skipped_due_to_both"] += 1
                        elif has_trapped_candles:
                            stats["symbols_skipped_due_to_trapped"] += 1
                        elif has_crossed_candle:
                            stats["symbols_skipped_due_to_crossed"] += 1
                        
                        if not execute_liquidator:
                            # Store symbol details for skipped symbols
                            symbol_detail = stats["symbol_details"].get(symbol, {})
                            symbol_detail.update({
                                "action": "skipped_after_age_check",
                                "reason": skip_reason,
                                "has_trapped": has_trapped_candles,
                                "has_crossed": has_crossed_candle,
                                "has_liquidator_flags": has_liquidator_flag_in_orders,
                                "liquidator_candle_num": liquidator_info['candle_number'] if liquidator_info else None,
                                "crossed_candle_num": crossed_info['candle_number'] if crossed_info else None,
                                "age_relationship": age_relationship if 'age_relationship' in locals() else None,
                                "ranging_cleanup_performed": len(ranging_levels_to_remove) > 0 if 'ranging_levels_to_remove' in locals() else False,
                                "ranging_levels_removed": list(ranging_levels_to_remove) if 'ranging_levels_to_remove' in locals() else []
                            })
                            stats["symbol_details"][symbol] = symbol_detail
                            continue
                        # If execute_liquidator is True, we fall through to STEP 4
                    
                    if not (has_trapped_candles or has_crossed_candle):
                        # No trapped or crossed - proceed normally
                        stats["symbols_without_trapped_or_crossed"] += 1
                        print(f"      ✅ No trapped candles AND no crossed candle - Proceeding with liquidator configuration")
                        execute_liquidator = True
                    
                    if not execute_liquidator:
                        # This should not happen due to continue above, but just in case
                        continue
                    
                    # =====================================================
                    # STEP 4: Execute liquidator configuration
                    # =====================================================
                    
                    # Track thresholds for this symbol
                    green_threshold = None
                    red_threshold = None
                    symbol_has_liquidator_flags = False
                    
                    # Process BID ORDERS
                    bid_orders = symbol_signals.get('bid_orders', [])
                    
                    # Process ASK ORDERS
                    ask_orders = symbol_signals.get('ask_orders', [])
                    
                    # FIRST PASS: Find all flagged orders and determine thresholds
                    # For GREEN: Find the LOWEST flagged SELL order price (since we remove at and above)
                    # For RED: Find the HIGHEST flagged BUY order price (since we remove at and below)
                    
                    # Check bid orders for flags
                    for order in bid_orders:
                        # Check main order for flags
                        if order.get('first_both_levels_green_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["green_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            # For GREEN, we want the LOWEST price among flagged SELL orders
                            order_price = order.get('entry')
                            if green_threshold is None or order_price < green_threshold:
                                green_threshold = order_price
                                print(f"      🟢 GREEN threshold set to {green_threshold} (lowest flagged SELL)")
                        
                        if order.get('first_both_levels_red_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["red_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            # For RED, we want the HIGHEST price among flagged BUY orders
                            order_price = order.get('entry')
                            if red_threshold is None or order_price > red_threshold:
                                red_threshold = order_price
                                print(f"      🔴 RED threshold set to {red_threshold} (highest flagged BUY)")
                        
                        # Check counter order for flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["green_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                # Counter order price might be different
                                counter_price = order['order_counter'].get('entry')
                                if green_threshold is None or counter_price < green_threshold:
                                    green_threshold = counter_price
                                    print(f"      🟢 GREEN threshold set to {green_threshold} from counter order")
                            
                            if order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["red_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if red_threshold is None or counter_price > red_threshold:
                                    red_threshold = counter_price
                                    print(f"      🔴 RED threshold set to {red_threshold} from counter order")
                    
                    # Check ask orders for flags
                    for order in ask_orders:
                        # Check main order for flags
                        if order.get('first_both_levels_green_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["green_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            order_price = order.get('entry')
                            if green_threshold is None or order_price < green_threshold:
                                green_threshold = order_price
                                print(f"      🟢 GREEN threshold set to {green_threshold} from ask order")
                        
                        if order.get('first_both_levels_red_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["red_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            order_price = order.get('entry')
                            if red_threshold is None or order_price > red_threshold:
                                red_threshold = order_price
                                print(f"      🔴 RED threshold set to {red_threshold} from ask order")
                        
                        # Check counter order for flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["green_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if green_threshold is None or counter_price < green_threshold:
                                    green_threshold = counter_price
                                    print(f"      🟢 GREEN threshold set to {green_threshold} from ask counter")
                            
                            if order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["red_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if red_threshold is None or counter_price > red_threshold:
                                    red_threshold = counter_price
                                    print(f"      🔴 RED threshold set to {red_threshold} from ask counter")
                    
                    if not symbol_has_liquidator_flags:
                        print(f"      ⏳ No liquidator flags found in any orders for {symbol}")
                        stats["symbols_without_liquidator_flags"] += 1
                        
                        # Store symbol details for symbols without liquidator flags
                        symbol_detail = stats["symbol_details"].get(symbol, {})
                        symbol_detail.update({
                            "action": "no_liquidator_flags_after_ranging",
                            "green_threshold": green_threshold,
                            "red_threshold": red_threshold,
                            "ranging_cleanup_performed": len(ranging_levels_to_remove) > 0 if 'ranging_levels_to_remove' in locals() else False,
                            "ranging_levels_removed": list(ranging_levels_to_remove) if 'ranging_levels_to_remove' in locals() else []
                        })
                        stats["symbol_details"][symbol] = symbol_detail
                        continue
                    
                    stats["symbols_with_liquidator_flags"] += 1
                    
                    # Store thresholds
                    if green_threshold is not None:
                        stats["removal_thresholds"]["green"][symbol] = green_threshold
                        print(f"      🟢 FINAL GREEN threshold for {symbol}: remove SELL orders at or above {green_threshold}")
                    
                    if red_threshold is not None:
                        stats["removal_thresholds"]["red"][symbol] = red_threshold
                        print(f"      🔴 FINAL RED threshold for {symbol}: remove BUY orders at or below {red_threshold}")
                    
                    # SECOND PASS: Process BID ORDERS based on thresholds
                    updated_bid_orders = []
                    
                    for order in bid_orders:
                        current_order = order.copy() if order else None
                        if not current_order:
                            continue
                        
                        order_price = current_order.get('entry')
                        is_sell_order = current_order.get('order_type', '').startswith('sell_')
                        is_buy_order = current_order.get('order_type', '').startswith('buy_')
                        
                        # Track if we keep this order
                        keep_this_order = True
                        
                        # Apply GREEN threshold logic (remove SELL orders at or above threshold)
                        if green_threshold is not None and is_sell_order and order_price >= green_threshold:
                            keep_this_order = False
                            print(f"        🟢 GREEN: Removing SELL bid order at {order_price} (at or above threshold {green_threshold})")
                            stats["orders_modified"]["green"]["sells_removed"] += 1
                            stats["bid_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be BUY, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is BUY - we keep BUY orders for GREEN
                                if counter.get('order_type', '').startswith('buy_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_bid_orders.append(counter)
                                    stats["orders_modified"]["green"]["promotions_to_main"] += 1
                                    stats["bid_orders_promoted"] += 1
                                    print(f"          🔄 Promoted BUY counter order to main position")
                        
                        # Apply RED threshold logic (remove BUY orders at or below threshold)
                        elif red_threshold is not None and is_buy_order and order_price <= red_threshold:
                            keep_this_order = False
                            print(f"        🔴 RED: Removing BUY bid order at {order_price} (at or below threshold {red_threshold})")
                            stats["orders_modified"]["red"]["buys_removed"] += 1
                            stats["bid_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be SELL, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is SELL - we keep SELL orders for RED
                                if counter.get('order_type', '').startswith('sell_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_bid_orders.append(counter)
                                    stats["orders_modified"]["red"]["promotions_to_main"] += 1
                                    stats["bid_orders_promoted"] += 1
                                    print(f"          🔄 Promoted SELL counter order to main position")
                        
                        else:
                            # Keep this order, but check if we need to remove its counter
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                counter_price = counter.get('entry')
                                counter_is_sell = counter.get('order_type', '').startswith('sell_')
                                counter_is_buy = counter.get('order_type', '').startswith('buy_')
                                
                                # Check if counter should be removed based on thresholds
                                remove_counter = False
                                
                                # GREEN threshold affects SELL counters
                                if green_threshold is not None and counter_is_sell and counter_price >= green_threshold:
                                    remove_counter = True
                                    print(f"          🟢 GREEN: Removing SELL counter at {counter_price} from bid order")
                                    stats["orders_modified"]["green"]["sells_removed"] += 1
                                
                                # RED threshold affects BUY counters
                                elif red_threshold is not None and counter_is_buy and counter_price <= red_threshold:
                                    remove_counter = True
                                    print(f"          🔴 RED: Removing BUY counter at {counter_price} from bid order")
                                    stats["orders_modified"]["red"]["buys_removed"] += 1
                                
                                if remove_counter:
                                    # Remove the counter order
                                    del current_order['order_counter']
                                    changes_made = True
                            
                            # Add the (possibly modified) order to updated list
                            updated_bid_orders.append(current_order)
                            
                            # Count preserved orders
                            if is_sell_order:
                                stats["orders_modified"]["green"]["sells_preserved_below"] += 1
                                stats["bid_orders_preserved"] += 1
                            else:
                                stats["orders_modified"]["red"]["buys_preserved_above"] += 1
                                stats["bid_orders_preserved"] += 1
                    
                    # SECOND PASS: Process ASK ORDERS based on thresholds
                    updated_ask_orders = []
                    
                    for order in ask_orders:
                        current_order = order.copy() if order else None
                        if not current_order:
                            continue
                        
                        order_price = current_order.get('entry')
                        is_sell_order = current_order.get('order_type', '').startswith('sell_')
                        is_buy_order = current_order.get('order_type', '').startswith('buy_')
                        
                        # Track if we keep this order
                        keep_this_order = True
                        
                        # Apply GREEN threshold logic (remove SELL orders at or above threshold)
                        if green_threshold is not None and is_sell_order and order_price >= green_threshold:
                            keep_this_order = False
                            print(f"        🟢 GREEN: Removing SELL ask order at {order_price} (at or above threshold {green_threshold})")
                            stats["orders_modified"]["green"]["sells_removed"] += 1
                            stats["ask_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be BUY, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is BUY - we keep BUY orders for GREEN
                                if counter.get('order_type', '').startswith('buy_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_ask_orders.append(counter)
                                    stats["orders_modified"]["green"]["promotions_to_main"] += 1
                                    stats["ask_orders_promoted"] += 1
                                    print(f"          🔄 Promoted BUY counter order to main position")
                        
                        # Apply RED threshold logic (remove BUY orders at or below threshold)
                        elif red_threshold is not None and is_buy_order and order_price <= red_threshold:
                            keep_this_order = False
                            print(f"        🔴 RED: Removing BUY ask order at {order_price} (at or below threshold {red_threshold})")
                            stats["orders_modified"]["red"]["buys_removed"] += 1
                            stats["ask_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be SELL, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is SELL - we keep SELL orders for RED
                                if counter.get('order_type', '').startswith('sell_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_ask_orders.append(counter)
                                    stats["orders_modified"]["red"]["promotions_to_main"] += 1
                                    stats["ask_orders_promoted"] += 1
                                    print(f"          🔄 Promoted SELL counter order to main position")
                        
                        else:
                            # Keep this order, but check if we need to remove its counter
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                counter_price = counter.get('entry')
                                counter_is_sell = counter.get('order_type', '').startswith('sell_')
                                counter_is_buy = counter.get('order_type', '').startswith('buy_')
                                
                                # Check if counter should be removed based on thresholds
                                remove_counter = False
                                
                                # GREEN threshold affects SELL counters
                                if green_threshold is not None and counter_is_sell and counter_price >= green_threshold:
                                    remove_counter = True
                                    print(f"          🟢 GREEN: Removing SELL counter at {counter_price} from ask order")
                                    stats["orders_modified"]["green"]["sells_removed"] += 1
                                
                                # RED threshold affects BUY counters
                                elif red_threshold is not None and counter_is_buy and counter_price <= red_threshold:
                                    remove_counter = True
                                    print(f"          🔴 RED: Removing BUY counter at {counter_price} from ask order")
                                    stats["orders_modified"]["red"]["buys_removed"] += 1
                                
                                if remove_counter:
                                    # Remove the counter order
                                    del current_order['order_counter']
                                    changes_made = True
                            
                            # Add the (possibly modified) order to updated list
                            updated_ask_orders.append(current_order)
                            
                            # Count preserved orders
                            if is_sell_order:
                                stats["orders_modified"]["green"]["sells_preserved_below"] += 1
                                stats["ask_orders_preserved"] += 1
                            else:
                                stats["orders_modified"]["red"]["buys_preserved_above"] += 1
                                stats["ask_orders_preserved"] += 1
                    
                    # Store symbol details
                    symbol_detail = stats["symbol_details"].get(symbol, {})
                    symbol_detail.update({
                        "action": "processed_liquidator",
                        "execution_reason": skip_reason if 'skip_reason' in locals() else "no_trapped_or_crossed",
                        "has_liquidator_flags": True,
                        "green_threshold": green_threshold,
                        "red_threshold": red_threshold,
                        "bid_orders_before": len(bid_orders),
                        "ask_orders_before": len(ask_orders),
                        "bid_orders_after": len(updated_bid_orders),
                        "ask_orders_after": len(updated_ask_orders),
                        "bid_orders_removed": len(bid_orders) - len(updated_bid_orders),
                        "ask_orders_removed": len(ask_orders) - len(updated_ask_orders),
                        "ranging_cleanup_performed": len(ranging_levels_to_remove) > 0 if 'ranging_levels_to_remove' in locals() else False,
                        "ranging_levels_removed": list(ranging_levels_to_remove) if 'ranging_levels_to_remove' in locals() else []
                    })
                    stats["symbol_details"][symbol] = symbol_detail
                    
                    # Update the symbol's orders
                    if updated_bid_orders:
                        symbol_signals['bid_orders'] = updated_bid_orders
                    else:
                        # Remove empty bid_orders array
                        if 'bid_orders' in symbol_signals:
                            del symbol_signals['bid_orders']
                            stats["empty_bid_arrays_removed"] += 1
                            print(f"      🗑️  Removed empty bid_orders array for {symbol}")
                    
                    if updated_ask_orders:
                        symbol_signals['ask_orders'] = updated_ask_orders
                    else:
                        # Remove empty ask_orders array
                        if 'ask_orders' in symbol_signals:
                            del symbol_signals['ask_orders']
                            stats["empty_ask_arrays_removed"] += 1
                            print(f"      🗑️  Removed empty ask_orders array for {symbol}")
                    
                    changes_made = True
            
            # Save the updated signals.json if changes were made
            if changes_made:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"\n [{inv_id}] ✅ Updated signals.json with order configuration changes")
            else:
                print(f"\n [{inv_id}] ⏳ No changes needed in signals.json")
            
            # Print summary
            print(f"\n  📊 ORDERS CONFIGURATION SUMMARY:")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            
            print(f"\n    🔹 ACCOUNT CONFIGURATION CHECK:")
            print(f"      - Investors skipped (config disabled): {stats['investors_skipped_due_to_settings']}")
            
            print(f"\n    🔹 STEP 2: RANGING LEVELS CLEANUP RESULTS:")
            print(f"      - Symbols with ranging cleanup: {stats['symbols_with_ranging_cleanup']}")
            print(f"      - Ranging levels removed: {stats['ranging_levels_removed']}")
            print(f"      - Main orders removed: {stats['ranging_main_orders_removed']}")
            print(f"      - Counter orders removed: {stats['ranging_counter_orders_removed']}")
            print(f"      - Other orders at same entry: {stats['ranging_other_orders_same_entry']}")
            
            print(f"\n    🔹 STEP 3: TRAPPED/CROSSED CHECK WITH LIQUIDATOR AGE:")
            print(f"      - Symbols with trapped OR crossed candles: {stats['symbols_with_trapped_or_crossed']}")
            print(f"        • With liquidator OLDER (EXECUTED): {stats['symbols_with_trapped_or_crossed_and_liquidator_older']}")
            print(f"        • Skipped due to trapped only: {stats['symbols_skipped_due_to_trapped']}")
            print(f"        • Skipped due to crossed only: {stats['symbols_skipped_due_to_crossed']}")
            print(f"        • Skipped due to both: {stats['symbols_skipped_due_to_both']}")
            print(f"        • Skipped due to liquidator YOUNGER: {stats['symbols_skipped_due_to_liquidator_younger']}")
            print(f"        • Skipped due to no liquidator data: {stats['symbols_skipped_due_to_no_liquidator']}")
            
            print(f"\n      📊 LIQUIDATOR AGE COMPARISONS:")
            print(f"        - Older: {stats['liquidator_age_comparisons']['older_count']}")
            print(f"        - Younger: {stats['liquidator_age_comparisons']['younger_count']}")
            print(f"        - Same age: {stats['liquidator_age_comparisons']['sameage_count']}")
            print(f"        - No liquidator data: {stats['liquidator_age_comparisons']['no_liquidator_count']}")
            
            print(f"\n    🔹 STEP 4: LIQUIDATOR PROCESSING RESULTS:")
            print(f"      - Symbols without trapped OR crossed: {stats['symbols_without_trapped_or_crossed']}")
            print(f"        • With liquidator flags: {stats['symbols_with_liquidator_flags']}")
            print(f"        • Without liquidator flags: {stats['symbols_without_liquidator_flags']}")
            print(f"      - Total flagged orders found: {stats['total_flagged_orders_found']}")
            print(f"        • GREEN flagged orders: {stats['green_flagged_orders']}")
            print(f"        • RED flagged orders: {stats['red_flagged_orders']}")
            
            if stats["removal_thresholds"]["green"]:
                print(f"\n    🟢 GREEN THRESHOLDS (removing SELL at or above):")
                for sym, threshold in stats["removal_thresholds"]["green"].items():
                    print(f"      - {sym}: {threshold}")
            
            if stats["removal_thresholds"]["red"]:
                print(f"\n    🔴 RED THRESHOLDS (removing BUY at or below):")
                for sym, threshold in stats["removal_thresholds"]["red"].items():
                    print(f"      - {sym}: {threshold}")
            
            print(f"\n    • Orders modified by color:")
            print(f"      🟢 GREEN (bullish - removing SELL orders at or above threshold):")
            print(f"        - SELL orders removed: {stats['orders_modified']['green']['sells_removed']}")
            print(f"        - SELL orders preserved (below threshold): {stats['orders_modified']['green']['sells_preserved_below']}")
            print(f"        - Promotions to main (BUY counters promoted): {stats['orders_modified']['green']['promotions_to_main']}")
            
            print(f"      🔴 RED (bearish - removing BUY orders at or below threshold):")
            print(f"        - BUY orders removed: {stats['orders_modified']['red']['buys_removed']}")
            print(f"        - BUY orders preserved (above threshold): {stats['orders_modified']['red']['buys_preserved_above']}")
            print(f"        - Promotions to main (SELL counters promoted): {stats['orders_modified']['red']['promotions_to_main']}")
            
            print(f"\n    • By order location:")
            print(f"      - Bid orders removed: {stats['bid_orders_removed']}")
            print(f"      - Bid orders preserved: {stats['bid_orders_preserved']}")
            print(f"      - Ask orders removed: {stats['ask_orders_removed']}")
            print(f"      - Ask orders preserved: {stats['ask_orders_preserved']}")
            print(f"      - Bid orders promoted: {stats['bid_orders_promoted']}")
            print(f"      - Ask orders promoted: {stats['ask_orders_promoted']}")
            
            print(f"\n    • Empty arrays removed:")
            print(f"      - Empty bid_orders arrays: {stats['empty_bid_arrays_removed']}")
            print(f"      - Empty ask_orders arrays: {stats['empty_ask_arrays_removed']}")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in orders configuration: {e}")
            import traceback
            traceback.print_exc()
    
    return stats

def liquidator_configuration(inv_id=None):
    """
    Configure orders based on liquidator candle colors from anylevel_liquidator analysis.
    
    EXECUTION ORDER:
    1. FIRST: Check accountmanagement.json settings.enable_liquidator_configuration flag
       - If false or missing, skip liquidator configuration for this investor
    
    2. SECOND: RANGING LEVELS CLEANUP - Remove all orders (both main and counter) at any price level
       that has first_ranging_levels: true flag. This completely removes the entire level including
       the main order and its counter.
    
    3. THIRD: EXECUTE LIQUIDATOR CONFIGURATION based on liquidator candle color:
        For GREEN liquidator flag (bullish candle):
        - Removes ALL SELL orders (sell_stop, sell_limit) that are AT or ABOVE the flagged SELL order's entry price
        - This includes the flagged order itself and any SELL orders with higher entry prices
        - Keeps SELL orders that are BELOW the flagged order's entry price
        - Keeps ALL BUY orders regardless of price
        
        For RED liquidator flag (bearish candle):
        - Removes ALL BUY orders (buy_stop, buy_limit) that are AT or BELOW the flagged BUY order's entry price
        - This includes the flagged order itself and any BUY orders with lower entry prices
        - Keeps BUY orders that are ABOVE the flagged order's entry price
        - Keeps ALL SELL orders regardless of price
    
    4. FOURTH: KEEP ONLY THE SPECIAL ORDER
        For GREEN liquidator:
        - Keep only the lowest BUY order (the one with the lowest entry price)
        - Remove all other orders (all SELL orders and any other BUY orders)
        
        For RED liquidator:
        - Keep only the highest SELL order (the one with the highest entry price)
        - Remove all other orders (all BUY orders and any other SELL orders)
    
    When a main order is removed:
    - If it has a counter order that should be kept, that counter order is promoted to become the main order
    - The promoted order retains all its original properties including flags
    
    When a counter order is removed:
    - The order_counter field is removed from the main order
    
    Empty bid_orders or ask_orders arrays are removed entirely to clean up the structure.
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the liquidator configuration
    """
    print(f"\n{'='*10} 🚩 LIQUIDATOR CONFIGURATION {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "total_symbols_analyzed": 0,
        # Account config check stats
        "investors_skipped_due_to_settings": 0,
        # Ranging cleanup stats
        "symbols_with_ranging_cleanup": 0,
        "ranging_levels_removed": 0,
        "ranging_main_orders_removed": 0,
        "ranging_counter_orders_removed": 0,
        "ranging_other_orders_same_entry": 0,
        # Order correction stats
        "orders_corrected": {
            "buy_in_bid_moved_to_ask": 0,
            "sell_in_ask_moved_to_bid": 0
        },
        # Liquidator processed stats
        "symbols_with_liquidator_flags": 0,
        "symbols_without_liquidator_flags": 0,
        "total_flagged_orders_found": 0,
        "green_flagged_orders": 0,
        "red_flagged_orders": 0,
        "removal_thresholds": {
            "green": {},  # Will store {symbol: threshold_price}
            "red": {}     # Will store {symbol: threshold_price}
        },
        "orders_modified": {
            "green": {
                "sells_removed": 0,
                "sells_preserved_below": 0,
                "buys_preserved": 0,
                "promotions_to_main": 0
            },
            "red": {
                "buys_removed": 0,
                "buys_preserved_above": 0,
                "sells_preserved": 0,
                "promotions_to_main": 0
            }
        },
        "final_single_order": {
            "green": {
                "symbols_with_single_buy": 0,
                "buys_kept": 0,
                "all_other_orders_removed": 0
            },
            "red": {
                "symbols_with_single_sell": 0,
                "sells_kept": 0,
                "all_other_orders_removed": 0
            }
        },
        "bid_orders_removed": 0,
        "ask_orders_removed": 0,
        "bid_orders_preserved": 0,
        "ask_orders_preserved": 0,
        "bid_orders_promoted": 0,
        "ask_orders_promoted": 0,
        "empty_bid_arrays_removed": 0,
        "empty_ask_arrays_removed": 0,
        "symbol_details": {}
    }
    
    # If inv_id is provided, process only that investor
    if inv_id:
        inv_root = Path(INV_PATH) / inv_id
        prices_dir = inv_root / "prices"
        
        # Path to accountmanagement.json and signals.json
        acc_mgmt_path = inv_root / "accountmanagement.json"
        signals_path = prices_dir / "signals.json"
        
        # =====================================================
        # STEP 1: Check account management settings
        # =====================================================
        print(f"\n [{inv_id}] 🔍 Checking account management settings...")
        
        if not acc_mgmt_path.exists():
            print(f" [{inv_id}] ⚠️ accountmanagement.json not found at {acc_mgmt_path}")
            print(f" [{inv_id}] ⏭️ Skipping liquidator configuration (no settings to check)")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_config = json.load(f)
            
            # Check if liquidator configuration is enabled
            settings = acc_config.get("settings", {})
            enable_liquidator_config = settings.get("enable_liquidator_configuration", False)
            
            if not enable_liquidator_config:
                print(f" [{inv_id}] ⏭️ Liquidator configuration is DISABLED in accountmanagement.json")
                print(f"     (settings.enable_liquidator_configuration = false)")
                stats["investors_skipped_due_to_settings"] += 1
                return stats
            
            print(f" [{inv_id}] ✅ Liquidator configuration is ENABLED - proceeding with processing")
            
        except json.JSONDecodeError as e:
            print(f" [{inv_id}]  Invalid JSON in accountmanagement.json: {e}")
            print(f" [{inv_id}] ⏭️ Skipping liquidator configuration due to config error")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        except Exception as e:
            print(f" [{inv_id}]  Error reading accountmanagement.json: {e}")
            print(f" [{inv_id}] ⏭️ Skipping liquidator configuration due to config error")
            stats["investors_skipped_due_to_settings"] += 1
            return stats
        
        # Continue with rest of processing if enabled
        if not signals_path.exists():
            print(f" [{inv_id}]  signals.json not found at {signals_path}")
            return stats
        
        try:
            print(f" [{inv_id}] 📂 Loading signals.json...")
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            
            # Track if any changes were made
            changes_made = False
            
            # Process each category and symbol
            for category_name, category_data in signals_data.get('categories', {}).items():
                symbols_in_category = category_data.get('symbols', {})
                
                # Create a list of symbols to process
                symbols_to_process = list(symbols_in_category.keys())
                
                for symbol in symbols_to_process:
                    symbol_signals = symbols_in_category[symbol]
                    stats["total_symbols_analyzed"] += 1
                    
                    print(f"\n    🔍 Analyzing {symbol} for liquidator configuration:")
                    
                    # =====================================================
                    # STEP 0: CORRECT ORDER PLACEMENT - Move orders to correct arrays
                    # =====================================================
                    print(f"      🔍 STEP 0: Checking for misplaced orders...")
                    
                    # Initialize corrected arrays
                    corrected_bid_orders = []
                    corrected_ask_orders = []
                    
                    # Get current orders
                    current_bid_orders = symbol_signals.get('bid_orders', [])
                    current_ask_orders = symbol_signals.get('ask_orders', [])
                    
                    # Process bid_orders - should contain BUY orders
                    for order in current_bid_orders:
                        order_type = order.get('order_type', '').lower()
                        
                        # If this is a SELL order in bid_orders, move it to ask_orders
                        if order_type.startswith('sell_'):
                            print(f"        🔄 Moving SELL order from bid_orders to ask_orders: {order_type} @ {order.get('entry')}")
                            corrected_ask_orders.append(order)
                            stats["orders_corrected"]["sell_in_ask_moved_to_bid"] += 1
                            changes_made = True
                        else:
                            # Keep BUY orders in bid_orders
                            corrected_bid_orders.append(order)
                    
                    # Process ask_orders - should contain SELL orders
                    for order in current_ask_orders:
                        order_type = order.get('order_type', '').lower()
                        
                        # If this is a BUY order in ask_orders, move it to bid_orders
                        if order_type.startswith('buy_'):
                            print(f"        🔄 Moving BUY order from ask_orders to bid_orders: {order_type} @ {order.get('entry')}")
                            corrected_bid_orders.append(order)
                            stats["orders_corrected"]["buy_in_bid_moved_to_ask"] += 1
                            changes_made = True
                        else:
                            # Keep SELL orders in ask_orders
                            corrected_ask_orders.append(order)
                    
                    # Update the symbol signals with corrected arrays
                    if corrected_bid_orders:
                        symbol_signals['bid_orders'] = corrected_bid_orders
                    else:
                        if 'bid_orders' in symbol_signals:
                            del symbol_signals['bid_orders']
                    
                    if corrected_ask_orders:
                        symbol_signals['ask_orders'] = corrected_ask_orders
                    else:
                        if 'ask_orders' in symbol_signals:
                            del symbol_signals['ask_orders']
                    
                    # =====================================================
                    # STEP 2: RANGING LEVELS CLEANUP - Remove all orders at levels with first_ranging_levels: true
                    # =====================================================
                    print(f"      🔍 STEP 2: Checking for ranging levels to clean up...")
                    
                    # Collect all entry prices that have first_ranging_levels: true
                    ranging_levels_to_remove = set()
                    
                    # Check bid orders for ranging flags
                    for order in symbol_signals.get('bid_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in bid_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in bid_orders counter")
                    
                    # Check ask orders for ranging flags
                    for order in symbol_signals.get('ask_orders', []):
                        if order.get('first_ranging_levels') is True:
                            ranging_levels_to_remove.add(order.get('entry'))
                            print(f"        🎯 Found ranging level at {order.get('entry')} in ask_orders")
                        
                        # Check counter order for ranging flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_ranging_levels') is True:
                                ranging_levels_to_remove.add(order['order_counter'].get('entry'))
                                print(f"        🎯 Found ranging level at {order['order_counter'].get('entry')} in ask_orders counter")
                    
                    if ranging_levels_to_remove:
                        print(f"      🧹 Cleaning up {len(ranging_levels_to_remove)} ranging levels: {sorted(ranging_levels_to_remove)}")
                        stats["symbols_with_ranging_cleanup"] += 1
                        stats["ranging_levels_removed"] += len(ranging_levels_to_remove)
                        
                        # Process BID ORDERS - remove any order at ranging levels
                        updated_bid_orders = []
                        
                        for order in symbol_signals.get('bid_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level bid order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["bid_orders_removed"] += 1
                                
                                # Also count any other orders at same price as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should also be removed
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price in ranging_levels_to_remove:
                                        # Counter is at same ranging level, it will be handled separately
                                        print(f"          Counter order at {counter_price} will be handled separately")
                                    # Don't promote counter - entire level is removed
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept bid order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["bid_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_bid_orders.append(order)
                        
                        # Process ASK ORDERS - remove any order at ranging levels
                        updated_ask_orders = []
                        
                        for order in symbol_signals.get('ask_orders', []):
                            order_price = order.get('entry')
                            
                            # Check if this order is at a ranging level
                            if order_price in ranging_levels_to_remove:
                                print(f"        🗑️  Removing ranging level ask order at {order_price}")
                                stats["ranging_main_orders_removed"] += 1
                                stats["ask_orders_removed"] += 1
                                
                                # Also count any other orders at same price as "other orders same entry"
                                stats["ranging_other_orders_same_entry"] += 1
                                
                                # Check if it has a counter order that should also be removed
                                if 'order_counter' in order:
                                    counter_price = order['order_counter'].get('entry')
                                    if counter_price in ranging_levels_to_remove:
                                        # Counter is at same ranging level, it will be handled separately
                                        print(f"          Counter order at {counter_price} will be handled separately")
                                    # Don't promote counter - entire level is removed
                                continue
                            
                            # If order is kept, check if its counter needs to be removed
                            if 'order_counter' in order:
                                counter_price = order['order_counter'].get('entry')
                                if counter_price in ranging_levels_to_remove:
                                    print(f"        🗑️  Removing counter order at {counter_price} from kept ask order")
                                    del order['order_counter']
                                    stats["ranging_counter_orders_removed"] += 1
                                    stats["ask_orders_removed"] += 1  # Counter counted as removal
                                    changes_made = True
                            
                            updated_ask_orders.append(order)
                        
                        # Update the orders after ranging cleanup
                        if updated_bid_orders:
                            symbol_signals['bid_orders'] = updated_bid_orders
                        else:
                            if 'bid_orders' in symbol_signals:
                                del symbol_signals['bid_orders']
                                print(f"      🗑️  Removed empty bid_orders array for {symbol} after ranging cleanup")
                        
                        if updated_ask_orders:
                            symbol_signals['ask_orders'] = updated_ask_orders
                        else:
                            if 'ask_orders' in symbol_signals:
                                del symbol_signals['ask_orders']
                                print(f"      🗑️  Removed empty ask_orders array for {symbol} after ranging cleanup")
                        
                        changes_made = True
                        
                        # After cleanup, reload symbol_signals for next steps
                        print(f"      ✅ Ranging cleanup complete for {symbol}")
                    
                    # =====================================================
                    # STEP 3: Execute liquidator configuration (initial filtering)
                    # =====================================================
                    
                    # Track thresholds for this symbol
                    green_threshold = None
                    red_threshold = None
                    symbol_has_liquidator_flags = False
                    
                    # Get current orders (after ranging cleanup)
                    bid_orders = symbol_signals.get('bid_orders', [])
                    ask_orders = symbol_signals.get('ask_orders', [])
                    
                    # FIRST PASS: Find all flagged orders and determine thresholds
                    # For GREEN: Find the LOWEST flagged SELL order price (since we remove at and above)
                    # For RED: Find the HIGHEST flagged BUY order price (since we remove at and below)
                    
                    # Check bid orders for flags
                    for order in bid_orders:
                        # Check main order for flags
                        if order.get('first_both_levels_green_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["green_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            # For GREEN, we want the LOWEST price among flagged SELL orders
                            order_price = order.get('entry')
                            if green_threshold is None or order_price < green_threshold:
                                green_threshold = order_price
                                print(f"      🟢 GREEN threshold set to {green_threshold} (lowest flagged SELL)")
                        
                        if order.get('first_both_levels_red_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["red_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            # For RED, we want the HIGHEST price among flagged BUY orders
                            order_price = order.get('entry')
                            if red_threshold is None or order_price > red_threshold:
                                red_threshold = order_price
                                print(f"      🔴 RED threshold set to {red_threshold} (highest flagged BUY)")
                        
                        # Check counter order for flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["green_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                # Counter order price might be different
                                counter_price = order['order_counter'].get('entry')
                                if green_threshold is None or counter_price < green_threshold:
                                    green_threshold = counter_price
                                    print(f"      🟢 GREEN threshold set to {green_threshold} from counter order")
                            
                            if order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["red_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if red_threshold is None or counter_price > red_threshold:
                                    red_threshold = counter_price
                                    print(f"      🔴 RED threshold set to {red_threshold} from counter order")
                    
                    # Check ask orders for flags
                    for order in ask_orders:
                        # Check main order for flags
                        if order.get('first_both_levels_green_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["green_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            order_price = order.get('entry')
                            if green_threshold is None or order_price < green_threshold:
                                green_threshold = order_price
                                print(f"      🟢 GREEN threshold set to {green_threshold} from ask order")
                        
                        if order.get('first_both_levels_red_liquidator') is True:
                            symbol_has_liquidator_flags = True
                            stats["red_flagged_orders"] += 1
                            stats["total_flagged_orders_found"] += 1
                            
                            order_price = order.get('entry')
                            if red_threshold is None or order_price > red_threshold:
                                red_threshold = order_price
                                print(f"      🔴 RED threshold set to {red_threshold} from ask order")
                        
                        # Check counter order for flags
                        if 'order_counter' in order:
                            if order['order_counter'].get('first_both_levels_green_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["green_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if green_threshold is None or counter_price < green_threshold:
                                    green_threshold = counter_price
                                    print(f"      🟢 GREEN threshold set to {green_threshold} from ask counter")
                            
                            if order['order_counter'].get('first_both_levels_red_liquidator') is True:
                                symbol_has_liquidator_flags = True
                                stats["red_flagged_orders"] += 1
                                stats["total_flagged_orders_found"] += 1
                                
                                counter_price = order['order_counter'].get('entry')
                                if red_threshold is None or counter_price > red_threshold:
                                    red_threshold = counter_price
                                    print(f"      🔴 RED threshold set to {red_threshold} from ask counter")
                    
                    if not symbol_has_liquidator_flags:
                        print(f"      ⏳ No liquidator flags found in any orders for {symbol}")
                        stats["symbols_without_liquidator_flags"] += 1
                        
                        # Store symbol details for symbols without liquidator flags
                        symbol_detail = stats["symbol_details"].get(symbol, {})
                        symbol_detail.update({
                            "action": "no_liquidator_flags_after_ranging",
                            "green_threshold": green_threshold,
                            "red_threshold": red_threshold,
                            "orders_corrected": {
                                "buy_in_bid_moved_to_ask": stats["orders_corrected"]["buy_in_bid_moved_to_ask"],
                                "sell_in_ask_moved_to_bid": stats["orders_corrected"]["sell_in_ask_moved_to_bid"]
                            },
                            "ranging_cleanup_performed": len(ranging_levels_to_remove) > 0 if 'ranging_levels_to_remove' in locals() else False,
                            "ranging_levels_removed": list(ranging_levels_to_remove) if 'ranging_levels_to_remove' in locals() else []
                        })
                        stats["symbol_details"][symbol] = symbol_detail
                        continue
                    
                    stats["symbols_with_liquidator_flags"] += 1
                    
                    # Store thresholds
                    if green_threshold is not None:
                        stats["removal_thresholds"]["green"][symbol] = green_threshold
                        print(f"      🟢 FINAL GREEN threshold for {symbol}: remove SELL orders at or above {green_threshold}")
                    
                    if red_threshold is not None:
                        stats["removal_thresholds"]["red"][symbol] = red_threshold
                        print(f"      🔴 FINAL RED threshold for {symbol}: remove BUY orders at or below {red_threshold}")
                    
                    # SECOND PASS: Process BID ORDERS based on thresholds
                    updated_bid_orders = []
                    
                    for order in bid_orders:
                        current_order = order.copy() if order else None
                        if not current_order:
                            continue
                        
                        order_price = current_order.get('entry')
                        is_sell_order = current_order.get('order_type', '').startswith('sell_')
                        is_buy_order = current_order.get('order_type', '').startswith('buy_')
                        
                        # Track if we keep this order
                        keep_this_order = True
                        
                        # Apply GREEN threshold logic (remove SELL orders at or above threshold)
                        if green_threshold is not None and is_sell_order and order_price >= green_threshold:
                            keep_this_order = False
                            print(f"        🟢 GREEN: Removing SELL bid order at {order_price} (at or above threshold {green_threshold})")
                            stats["orders_modified"]["green"]["sells_removed"] += 1
                            stats["bid_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be BUY, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is BUY - we keep BUY orders for GREEN
                                if counter.get('order_type', '').startswith('buy_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_bid_orders.append(counter)
                                    stats["orders_modified"]["green"]["promotions_to_main"] += 1
                                    stats["bid_orders_promoted"] += 1
                                    print(f"          🔄 Promoted BUY counter order to main position")
                        
                        # Apply RED threshold logic (remove BUY orders at or below threshold)
                        elif red_threshold is not None and is_buy_order and order_price <= red_threshold:
                            keep_this_order = False
                            print(f"        🔴 RED: Removing BUY bid order at {order_price} (at or below threshold {red_threshold})")
                            stats["orders_modified"]["red"]["buys_removed"] += 1
                            stats["bid_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be SELL, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is SELL - we keep SELL orders for RED
                                if counter.get('order_type', '').startswith('sell_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_bid_orders.append(counter)
                                    stats["orders_modified"]["red"]["promotions_to_main"] += 1
                                    stats["bid_orders_promoted"] += 1
                                    print(f"          🔄 Promoted SELL counter order to main position")
                        
                        else:
                            # Keep this order, but check if we need to remove its counter
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                counter_price = counter.get('entry')
                                counter_is_sell = counter.get('order_type', '').startswith('sell_')
                                counter_is_buy = counter.get('order_type', '').startswith('buy_')
                                
                                # Check if counter should be removed based on thresholds
                                remove_counter = False
                                
                                # GREEN threshold affects SELL counters
                                if green_threshold is not None and counter_is_sell and counter_price >= green_threshold:
                                    remove_counter = True
                                    print(f"          🟢 GREEN: Removing SELL counter at {counter_price} from bid order")
                                    stats["orders_modified"]["green"]["sells_removed"] += 1
                                
                                # RED threshold affects BUY counters
                                elif red_threshold is not None and counter_is_buy and counter_price <= red_threshold:
                                    remove_counter = True
                                    print(f"          🔴 RED: Removing BUY counter at {counter_price} from bid order")
                                    stats["orders_modified"]["red"]["buys_removed"] += 1
                                
                                if remove_counter:
                                    # Remove the counter order
                                    del current_order['order_counter']
                                    changes_made = True
                            
                            # Add the (possibly modified) order to updated list
                            updated_bid_orders.append(current_order)
                            
                            # Count preserved orders
                            if is_sell_order:
                                stats["orders_modified"]["green"]["sells_preserved_below"] += 1
                                stats["bid_orders_preserved"] += 1
                            else:
                                stats["orders_modified"]["red"]["buys_preserved_above"] += 1
                                stats["bid_orders_preserved"] += 1
                    
                    # SECOND PASS: Process ASK ORDERS based on thresholds
                    updated_ask_orders = []
                    
                    for order in ask_orders:
                        current_order = order.copy() if order else None
                        if not current_order:
                            continue
                        
                        order_price = current_order.get('entry')
                        is_sell_order = current_order.get('order_type', '').startswith('sell_')
                        is_buy_order = current_order.get('order_type', '').startswith('buy_')
                        
                        # Track if we keep this order
                        keep_this_order = True
                        
                        # Apply GREEN threshold logic (remove SELL orders at or above threshold)
                        if green_threshold is not None and is_sell_order and order_price >= green_threshold:
                            keep_this_order = False
                            print(f"        🟢 GREEN: Removing SELL ask order at {order_price} (at or above threshold {green_threshold})")
                            stats["orders_modified"]["green"]["sells_removed"] += 1
                            stats["ask_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be BUY, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is BUY - we keep BUY orders for GREEN
                                if counter.get('order_type', '').startswith('buy_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_ask_orders.append(counter)
                                    stats["orders_modified"]["green"]["promotions_to_main"] += 1
                                    stats["ask_orders_promoted"] += 1
                                    print(f"          🔄 Promoted BUY counter order to main position")
                        
                        # Apply RED threshold logic (remove BUY orders at or below threshold)
                        elif red_threshold is not None and is_buy_order and order_price <= red_threshold:
                            keep_this_order = False
                            print(f"        🔴 RED: Removing BUY ask order at {order_price} (at or below threshold {red_threshold})")
                            stats["orders_modified"]["red"]["buys_removed"] += 1
                            stats["ask_orders_removed"] += 1
                            
                            # Check if it has a counter order to promote (counter would be SELL, which we keep)
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                # Counter is SELL - we keep SELL orders for RED
                                if counter.get('order_type', '').startswith('sell_'):
                                    # Promote counter order to main order
                                    counter['promoted_from_counter'] = True
                                    counter['original_order_removed_at'] = order_price
                                    updated_ask_orders.append(counter)
                                    stats["orders_modified"]["red"]["promotions_to_main"] += 1
                                    stats["ask_orders_promoted"] += 1
                                    print(f"          🔄 Promoted SELL counter order to main position")
                        
                        else:
                            # Keep this order, but check if we need to remove its counter
                            if 'order_counter' in current_order:
                                counter = current_order['order_counter']
                                counter_price = counter.get('entry')
                                counter_is_sell = counter.get('order_type', '').startswith('sell_')
                                counter_is_buy = counter.get('order_type', '').startswith('buy_')
                                
                                # Check if counter should be removed based on thresholds
                                remove_counter = False
                                
                                # GREEN threshold affects SELL counters
                                if green_threshold is not None and counter_is_sell and counter_price >= green_threshold:
                                    remove_counter = True
                                    print(f"          🟢 GREEN: Removing SELL counter at {counter_price} from ask order")
                                    stats["orders_modified"]["green"]["sells_removed"] += 1
                                
                                # RED threshold affects BUY counters
                                elif red_threshold is not None and counter_is_buy and counter_price <= red_threshold:
                                    remove_counter = True
                                    print(f"          🔴 RED: Removing BUY counter at {counter_price} from ask order")
                                    stats["orders_modified"]["red"]["buys_removed"] += 1
                                
                                if remove_counter:
                                    # Remove the counter order
                                    del current_order['order_counter']
                                    changes_made = True
                            
                            # Add the (possibly modified) order to updated list
                            updated_ask_orders.append(current_order)
                            
                            # Count preserved orders
                            if is_sell_order:
                                stats["orders_modified"]["green"]["sells_preserved_below"] += 1
                                stats["ask_orders_preserved"] += 1
                            else:
                                stats["orders_modified"]["red"]["buys_preserved_above"] += 1
                                stats["ask_orders_preserved"] += 1
                    
                    # =====================================================
                    # STEP 4: KEEP ONLY THE SINGLE SPECIAL ORDER
                    # =====================================================
                    
                    # Combine all orders for easier processing
                    all_orders = []
                    
                    # Add all bid orders with location info
                    for order in updated_bid_orders:
                        order_copy = order.copy()
                        order_copy['_location'] = 'bid'
                        all_orders.append(order_copy)
                    
                    # Add all ask orders with location info
                    for order in updated_ask_orders:
                        order_copy = order.copy()
                        order_copy['_location'] = 'ask'
                        all_orders.append(order_copy)
                    
                    # Separate buys and sells
                    buy_orders = [o for o in all_orders if o.get('order_type', '').startswith('buy_')]
                    sell_orders = [o for o in all_orders if o.get('order_type', '').startswith('sell_')]
                    
                    # Sort buys by entry price (ascending - lowest first)
                    buy_orders.sort(key=lambda x: x.get('entry', 0))
                    
                    # Sort sells by entry price (descending - highest first)
                    sell_orders.sort(key=lambda x: x.get('entry', 0), reverse=True)
                    
                    # Determine which color we're processing
                    is_green = green_threshold is not None
                    is_red = red_threshold is not None and not is_green
                    
                    # Track the single order we'll keep
                    single_order_to_keep = None
                    
                    if is_green and buy_orders:
                        # For GREEN: Keep only the lowest BUY order
                        lowest_buy = buy_orders[0]  # Lowest entry price
                        
                        print(f"        🟢 GREEN: Keeping lowest BUY order at {lowest_buy.get('entry')} (exit: {lowest_buy.get('exit')})")
                        
                        # This is the order we'll keep
                        single_order_to_keep = lowest_buy
                        stats["final_single_order"]["green"]["symbols_with_single_buy"] += 1
                        stats["final_single_order"]["green"]["buys_kept"] += 1
                    
                    elif is_red and sell_orders:
                        # For RED: Keep only the highest SELL order
                        highest_sell = sell_orders[0]  # Highest entry price
                        
                        print(f"        🔴 RED: Keeping highest SELL order at {highest_sell.get('entry')} (exit: {highest_sell.get('exit')})")
                        
                        # This is the order we'll keep
                        single_order_to_keep = highest_sell
                        stats["final_single_order"]["red"]["symbols_with_single_sell"] += 1
                        stats["final_single_order"]["red"]["sells_kept"] += 1
                    
                    # Reset the order arrays
                    final_bid_orders = []
                    final_ask_orders = []
                    
                    if single_order_to_keep:
                        # Remove the temporary _location field
                        if '_location' in single_order_to_keep:
                            location = single_order_to_keep.pop('_location')
                            
                            # Add the single order to the appropriate array
                            if location == 'bid':
                                final_bid_orders.append(single_order_to_keep)
                                print(f"        ✅ Keeping single bid order at {single_order_to_keep.get('entry')}")
                            else:
                                final_ask_orders.append(single_order_to_keep)
                                print(f"        ✅ Keeping single ask order at {single_order_to_keep.get('entry')}")
                            
                            # Count removed orders
                            orders_removed_count = len(all_orders) - 1
                            if is_green:
                                stats["final_single_order"]["green"]["all_other_orders_removed"] += orders_removed_count
                            else:
                                stats["final_single_order"]["red"]["all_other_orders_removed"] += orders_removed_count
                            
                            # Update the stats for removed orders (they're all being removed except the one we keep)
                            # This is in addition to the removals already counted in STEP 3
                            stats["bid_orders_removed"] += (len(updated_bid_orders) - len(final_bid_orders))
                            stats["ask_orders_removed"] += (len(updated_ask_orders) - len(final_ask_orders))
                        
                        changes_made = True
                    else:
                        # If no single order to keep (shouldn't happen if we had flags), keep all orders
                        print(f"        ⚠️ No single order identified to keep - preserving all orders")
                        final_bid_orders = updated_bid_orders
                        final_ask_orders = updated_ask_orders
                    
                    # Store symbol details
                    symbol_detail = stats["symbol_details"].get(symbol, {})
                    symbol_detail.update({
                        "action": "processed_liquidator_with_single_order" if single_order_to_keep else "processed_liquidator",
                        "has_liquidator_flags": True,
                        "green_threshold": green_threshold,
                        "red_threshold": red_threshold,
                        "orders_corrected": {
                            "buy_in_bid_moved_to_ask": stats["orders_corrected"]["buy_in_bid_moved_to_ask"],
                            "sell_in_ask_moved_to_bid": stats["orders_corrected"]["sell_in_ask_moved_to_bid"]
                        },
                        "bid_orders_before": len(bid_orders),
                        "ask_orders_before": len(ask_orders),
                        "bid_orders_after_step3": len(updated_bid_orders),
                        "ask_orders_after_step3": len(updated_ask_orders),
                        "bid_orders_after_step4": len(final_bid_orders),
                        "ask_orders_after_step4": len(final_ask_orders),
                        "bid_orders_removed": len(bid_orders) - len(final_bid_orders),
                        "ask_orders_removed": len(ask_orders) - len(final_ask_orders),
                        "ranging_cleanup_performed": len(ranging_levels_to_remove) > 0 if 'ranging_levels_to_remove' in locals() else False,
                        "ranging_levels_removed": list(ranging_levels_to_remove) if 'ranging_levels_to_remove' in locals() else [],
                        "final_single_order": {
                            "entry": single_order_to_keep.get('entry') if single_order_to_keep else None,
                            "order_type": single_order_to_keep.get('order_type') if single_order_to_keep else None,
                            "exit": single_order_to_keep.get('exit') if single_order_to_keep else None
                        } if single_order_to_keep else None
                    })
                    stats["symbol_details"][symbol] = symbol_detail
                    
                    # Update the symbol's orders with the final single order
                    if final_bid_orders:
                        symbol_signals['bid_orders'] = final_bid_orders
                    else:
                        # Remove empty bid_orders array
                        if 'bid_orders' in symbol_signals:
                            del symbol_signals['bid_orders']
                            stats["empty_bid_arrays_removed"] += 1
                            print(f"      🗑️  Removed empty bid_orders array for {symbol}")
                    
                    if final_ask_orders:
                        symbol_signals['ask_orders'] = final_ask_orders
                    else:
                        # Remove empty ask_orders array
                        if 'ask_orders' in symbol_signals:
                            del symbol_signals['ask_orders']
                            stats["empty_ask_arrays_removed"] += 1
                            print(f"      🗑️  Removed empty ask_orders array for {symbol}")
                    
                    changes_made = True
            
            # Save the updated signals.json if changes were made
            if changes_made:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"\n [{inv_id}] ✅ Updated signals.json with liquidator configuration changes")
            else:
                print(f"\n [{inv_id}] ⏳ No changes needed in signals.json")
            
            # Print summary
            print(f"\n  📊 LIQUIDATOR CONFIGURATION SUMMARY:")
            print(f"    • Symbols analyzed: {stats['total_symbols_analyzed']}")
            
            print(f"\n    🔹 ACCOUNT CONFIGURATION CHECK:")
            print(f"      - Investors skipped (config disabled): {stats['investors_skipped_due_to_settings']}")
            
            print(f"\n    🔹 STEP 0: ORDER CORRECTION RESULTS:")
            print(f"      - BUY orders moved from ask_orders to bid_orders: {stats['orders_corrected']['buy_in_bid_moved_to_ask']}")
            print(f"      - SELL orders moved from bid_orders to ask_orders: {stats['orders_corrected']['sell_in_ask_moved_to_bid']}")
            
            print(f"\n    🔹 STEP 2: RANGING LEVELS CLEANUP RESULTS:")
            print(f"      - Symbols with ranging cleanup: {stats['symbols_with_ranging_cleanup']}")
            print(f"      - Ranging levels removed: {stats['ranging_levels_removed']}")
            print(f"      - Main orders removed: {stats['ranging_main_orders_removed']}")
            print(f"      - Counter orders removed: {stats['ranging_counter_orders_removed']}")
            print(f"      - Other orders at same entry: {stats['ranging_other_orders_same_entry']}")
            
            print(f"\n    🔹 STEP 3: LIQUIDATOR FILTERING RESULTS:")
            print(f"      • With liquidator flags: {stats['symbols_with_liquidator_flags']}")
            print(f"      • Without liquidator flags: {stats['symbols_without_liquidator_flags']}")
            print(f"      • Total flagged orders found: {stats['total_flagged_orders_found']}")
            print(f"        - GREEN flagged orders: {stats['green_flagged_orders']}")
            print(f"        - RED flagged orders: {stats['red_flagged_orders']}")
            
            if stats["removal_thresholds"]["green"]:
                print(f"\n    🟢 GREEN THRESHOLDS (removing SELL at or above):")
                for sym, threshold in stats["removal_thresholds"]["green"].items():
                    print(f"      - {sym}: {threshold}")
            
            if stats["removal_thresholds"]["red"]:
                print(f"\n    🔴 RED THRESHOLDS (removing BUY at or below):")
                for sym, threshold in stats["removal_thresholds"]["red"].items():
                    print(f"      - {sym}: {threshold}")
            
            print(f"\n    • Orders modified by color (STEP 3):")
            print(f"      🟢 GREEN (bullish - removing SELL orders at or above threshold):")
            print(f"        - SELL orders removed: {stats['orders_modified']['green']['sells_removed']}")
            print(f"        - SELL orders preserved (below threshold): {stats['orders_modified']['green']['sells_preserved_below']}")
            print(f"        - BUY orders preserved: {stats['orders_modified']['green']['buys_preserved']}")
            print(f"        - Promotions to main (BUY counters promoted): {stats['orders_modified']['green']['promotions_to_main']}")
            
            print(f"      🔴 RED (bearish - removing BUY orders at or below threshold):")
            print(f"        - BUY orders removed: {stats['orders_modified']['red']['buys_removed']}")
            print(f"        - BUY orders preserved (above threshold): {stats['orders_modified']['red']['buys_preserved_above']}")
            print(f"        - SELL orders preserved: {stats['orders_modified']['red']['sells_preserved']}")
            print(f"        - Promotions to main (SELL counters promoted): {stats['orders_modified']['red']['promotions_to_main']}")
            
            print(f"\n    🔹 STEP 4: FINAL SINGLE ORDER CONFIGURATION:")
            print(f"      🟢 GREEN symbols with single BUY: {stats['final_single_order']['green']['symbols_with_single_buy']}")
            print(f"        - BUY orders kept (original exit): {stats['final_single_order']['green']['buys_kept']}")
            print(f"        - All other orders removed: {stats['final_single_order']['green']['all_other_orders_removed']}")
            print(f"      🔴 RED symbols with single SELL: {stats['final_single_order']['red']['symbols_with_single_sell']}")
            print(f"        - SELL orders kept (original exit): {stats['final_single_order']['red']['sells_kept']}")
            print(f"        - All other orders removed: {stats['final_single_order']['red']['all_other_orders_removed']}")
            
            print(f"\n    • By order location (total including STEP 4 cleanup):")
            print(f"      - Bid orders removed: {stats['bid_orders_removed']}")
            print(f"      - Bid orders preserved: {stats['bid_orders_preserved']}")
            print(f"      - Ask orders removed: {stats['ask_orders_removed']}")
            print(f"      - Ask orders preserved: {stats['ask_orders_preserved']}")
            print(f"      - Bid orders promoted: {stats['bid_orders_promoted']}")
            print(f"      - Ask orders promoted: {stats['ask_orders_promoted']}")
            
            print(f"\n    • Empty arrays removed:")
            print(f"      - Empty bid_orders arrays: {stats['empty_bid_arrays_removed']}")
            print(f"      - Empty ask_orders arrays: {stats['empty_ask_arrays_removed']}")
            
        except Exception as e:
            print(f" [{inv_id}]  Error in liquidator configuration: {e}")
            import traceback
            traceback.print_exc()
    
    return stats
    
def timeframe_countdown(inv_id=None):
    """
    Countdown to candle close based on strategy_timeframe.
    Locks onto the FIRST valid symbol found per user and sticks to it.
    EXITS IMMEDIATELY once the candle closes.
    """

    print(f"\n{'='*10} ⏰ TIMEFRAME COUNTDOWN: CANDLE CLOSE MONITOR {'='*10}")
    
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "timeframe_used": None,
        "countdown_completed": False,
        "processing_success": False,
    }
    
    def clean(s): 
        return str(s).replace(" ", "").replace("_", "").replace("/", "").replace(".", "").upper() if s else ""
    
    def is_symbol_market_open(symbol):
        if not mt5.symbol_select(symbol, True):
            return False, None
        symbol_info = mt5.symbol_info(symbol)
        if not symbol_info or symbol_info.trade_mode == mt5.SYMBOL_TRADE_MODE_DISABLED:
            return False, None
        return True, symbol_info

    def get_all_symbols_from_config(config):
        all_symbols = []
        for category, symbols in config.get("symbols_dictionary", {}).items():
            all_symbols.extend(symbols)
        for strategy in config.get("strategies", []):
            s = strategy.get("symbol")
            if s and s not in all_symbols: all_symbols.append(s)
        return list(set(all_symbols))

    # Determine which users to process
    investors_to_process = [inv_id] if inv_id else list(usersdictionary.keys())

    for user_brokerid in investors_to_process:
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg: continue
        
        acc_mgmt_path = Path(INV_PATH) / user_brokerid / "accountmanagement.json"
        if not acc_mgmt_path.exists(): continue

        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            strategy_tf = config.get("settings", {}).get("strategy_timeframe")
            if not strategy_tf: continue
            
            # --- TIMEFRAME MAPPING ---
            TIMEFRAME_MAP = {
                "1m": mt5.TIMEFRAME_M1, "2m": mt5.TIMEFRAME_M2, "3m": mt5.TIMEFRAME_M3,
                "5m": mt5.TIMEFRAME_M5, "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30,
                "1h": mt5.TIMEFRAME_H1, "4h": mt5.TIMEFRAME_H4, "1d": mt5.TIMEFRAME_D1
            }
            
            def _get_sec(tf_str):
                m = re.match(r'(\d+)([a-z]+)', tf_str)
                mult = {'m': 60, 'h': 3600, 'd': 86400}
                return int(m.group(1)) * mult.get(m.group(2), 60) if m else 60

            timeframe = TIMEFRAME_MAP.get(strategy_tf, mt5.TIMEFRAME_M1)
            period_sec = _get_sec(strategy_tf)
            stats["timeframe_used"] = strategy_tf

            # --- ONE-TIME SYMBOL LOCK ---
            # Search once for a valid symbol and stick to it
            all_symbols = get_all_symbols_from_config(config)
            selected_symbol, s_info = None, None
            
            for s in all_symbols:
                is_open, info = is_symbol_market_open(clean(s))
                if is_open:
                    selected_symbol, s_info = clean(s), info
                    break # FOUND ONE - STOP SEARCHING

            if not selected_symbol:
                print(f" └─ ⚠️ No open symbols found for {user_brokerid}. Skipping.")
                continue

            # --- INITIAL STATE CAPTURE ---
            rates = mt5.copy_rates_from_pos(selected_symbol, timeframe, 0, 1)
            if rates is None or len(rates) == 0:
                continue
            
            initial_candle_time = int(rates[0]['time'])
            target_close_time = initial_candle_time + period_sec

            print(f"⏳ Monitoring {strategy_tf} on {selected_symbol}. Target Close: {time.strftime('%H:%M:%S', time.localtime(target_close_time))}")

            # --- THE RIGID COUNTDOWN LOOP ---
            while True:
                # 1. Get current MT5 server time from the locked symbol
                tick = mt5.symbol_info_tick(selected_symbol)
                if not tick:
                    time.sleep(0.1)
                    continue

                current_mt5_time = tick.time
                remaining = target_close_time - current_mt5_time

                # 2. Verify candle rollover (safety check)
                check_rates = mt5.copy_rates_from_pos(selected_symbol, timeframe, 0, 1)
                new_candle_time = int(check_rates[0]['time']) if (check_rates is not None and len(check_rates) > 0) else initial_candle_time

                # UI Update
                m, s = divmod(max(0, int(remaining)), 60)
                time_str = f"{m:02d}:{s:02d}"
                print(f"\r    ⏱️  Remaining: {time_str} | Bid: {tick.bid:.{s_info.digits}f}", end="", flush=True)

                # EXIT CONDITION: Server time passed target OR MT5 generated a new candle
                if remaining <= 0 or new_candle_time > initial_candle_time:
                    print(f"\n\n🎯 CANDLE CLOSED - TERMINATING.")
                    stats["countdown_completed"] = True
                    stats["processing_success"] = True
                    return stats 

                time.sleep(0.2) # High frequency polling for accuracy

        except Exception as e:
            print(f"\n └─  Error processing {user_brokerid}: {e}")
            continue

    print(f"\n{'='*10} 🏁 FUNCTION ENDED {'='*10}\n")
    return stats

def place_signals_orders_accounts(inv_id=None):
    """
    Place orders from signals.json for specified investor(s).
    
    EXECUTION LOGIC:
    1. Check accountmanagement.json settings:
       - If enable_auto_trading is false, skip order placement
       - Check allow_order_type_conversion for conversion permission
       - Check opposite_order_restriction for opposite-direction proximity checks
    
    2. For each symbol with signals:
       - Process ALL orders from both bid_orders and ask_orders arrays
       - For each order, check if an identical order already exists in:
         * Pending orders (same symbol, order_type, entry price, volume)
         * Open positions (same symbol, order_type, entry price, volume)
       - Skip if identical order exists
       
    3. Check if order is too close to any running position using risk-based detection:
       - Get all open positions for the symbol
       - For each position, calculate the risk if the pending order were to be triggered
       
       A. SAME DIRECTION CHECK (SELL vs SELL, BUY vs BUY):
          - Compare this risk against the position's own risk
          - If calculated risk < position_risk/2, orders are too close - SKIP placement
          - If calculated risk >= position_risk/2, orders are sufficiently far - ALLOW placement
       
       B. OPPOSITE DIRECTION CHECK (SELL vs BUY, BUY vs SELL) - NEW:
          - Only applies if opposite_order_restriction = true in settings
          - Calculate risk if opposite orders were to trigger toward each other
          - If calculated risk < position_risk/2, orders are too close - SKIP placement
          - If opposite_order_restriction = false, skip this check entirely
    
    4. Place new order if no duplicate found and not too close to positions
       - If order has order_counter, also place the counter order IMMEDIATELY
       - Pending orders are placed with TRADE_ACTION_PENDING and will auto-execute when price reaches them
       - If order fails with "Invalid price" (code 10015) AND conversion is allowed:
         * Convert order type with adjusted price
         * NEW CONVERSION LOGIC: buy_stop → sell_limit, sell_stop → buy_limit
         * Original stop order is NOT placed, only the converted limit order is placed
    
    5. Track placement statistics
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about order placement
    """
    print(f"\n{'='*10} 🎯 PLACE SIGNALS ORDERS {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "investors_skipped_due_to_settings": 0,
        "investors_skipped_due_to_position_flag": 0,
        "symbols_processed": 0,
        "orders_attempted": 0,
        "orders_placed": 0,
        "orders_skipped_duplicate_pending": 0,
        "orders_skipped_duplicate_position": 0,
        "orders_skipped_too_close_same_direction": 0,
        "orders_skipped_too_close_opposite_direction": 0,
        "orders_failed": 0,
        "orders_converted_and_placed": 0,
        "orders_conversion_skipped_permission": 0,
        "orders_cancelled_during_regulation": 0,
        "main_orders_placed": 0,
        "counter_orders_placed": 0,
        "counter_orders_skipped": 0,
        "counter_orders_failed": 0,
        "total_active_pending": 0,
        "total_open_positions": 0,
        "placement_errors": [],
        "symbol_details": {}
    }
    
    def get_all_existing_pending_orders():
        """
        Get ALL pending orders from MT5 for this account (no symbol filter).
        
        Returns:
            dict: Dictionary of existing orders keyed by unique identifier
        """
        existing_orders = {}
        
        # Get all pending orders (no symbol parameter to avoid errors)
        orders = mt5.orders_get()
        if orders is None:
            error_code = mt5.last_error()
            print(f"        ℹ️ Could not fetch pending orders: {error_code}")
            return existing_orders
        
        for order in orders:
            try:
                # For pending orders, use price (not price_open)
                # Different MT5 versions might use different attribute names
                order_price = getattr(order, 'price', None)
                if order_price is None:
                    order_price = getattr(order, 'price_open', 0)
                
                order_volume = getattr(order, 'volume_current', getattr(order, 'volume_initial', 0))
                
                # Create a unique key for this order
                order_key = f"{order.symbol}_{order.type}_{order_price}_{order_volume}"
                existing_orders[order_key] = {
                    'ticket': order.ticket,
                    'symbol': order.symbol,
                    'type': order.type,
                    'price': order_price,
                    'volume': order_volume,
                    'sl': order.sl,
                    'time_setup': getattr(order, 'time_setup', 0),
                    'type_string': "PENDING"
                }
            except Exception as e:
                print(f"        ⚠️ Error processing pending order: {e}")
                continue
        
        print(f"        📋 Found {len(existing_orders)} existing pending orders")
        return existing_orders
    
    def get_all_existing_positions():
        """
        Get ALL open positions from MT5 for this account (no symbol filter).
        
        Returns:
            dict: Dictionary of existing positions keyed by unique identifier
        """
        existing_positions = {}
        
        # Get all open positions (no symbol parameter to avoid errors)
        positions = mt5.positions_get()
        if positions is None:
            error_code = mt5.last_error()
            print(f"        ℹ️ Could not fetch open positions: {error_code}")
            return existing_positions
        
        for position in positions:
            try:
                # For positions, use price_open
                position_key = f"{position.symbol}_{position.type}_{position.price_open}_{position.volume}"
                existing_positions[position_key] = {
                    'ticket': position.ticket,
                    'symbol': position.symbol,
                    'type': position.type,
                    'price': position.price_open,
                    'volume': position.volume,
                    'sl': position.sl,
                    'time': getattr(position, 'time', 0),
                    'type_string': "POSITION"
                }
            except Exception as e:
                print(f"        ⚠️ Error processing position: {e}")
                continue
        
        print(f"        📋 Found {len(existing_positions)} existing open positions")
        return existing_positions
    
    def regulate_and_authorize_orders(symbol, pending_orders_to_keep, magic_number):
        """
        Cancel/delete any pending orders in the account that are not in signals.json.
        
        Args:
            symbol: Symbol to regulate
            pending_orders_to_keep: List of order keys that should be kept
            magic_number: Magic number for orders
            
        Returns:
            int: Number of orders cancelled
        """
        cancelled_count = 0
        
        # Get all pending orders for this symbol
        orders = mt5.orders_get(symbol=symbol)
        if orders is None:
            print(f"      ℹ️ No pending orders found for {symbol} to regulate")
            return cancelled_count
        
        for order in orders:
            try:
                # Only manage orders with our magic number
                if order.magic != magic_number:
                    continue
                
                # Create key for this order
                order_price = getattr(order, 'price', getattr(order, 'price_open', 0))
                order_volume = getattr(order, 'volume_current', getattr(order, 'volume_initial', 0))
                order_key = f"{order.symbol}_{order.type}_{order_price}_{order_volume}"
                
                # Check if this order should be kept
                if order_key not in pending_orders_to_keep:
                    print(f"      🔍 Found unauthorized pending order: Ticket #{order.ticket} - {order_key}")
                    
                    # Prepare cancel request
                    request = {
                        "action": mt5.TRADE_ACTION_REMOVE,
                        "order": order.ticket,
                        "magic": magic_number,
                        "comment": "Cancelled by regulation"
                    }
                    
                    # Send cancel request
                    result = mt5.order_send(request)
                    
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"      ✅ Cancelled unauthorized order: Ticket #{order.ticket}")
                        cancelled_count += 1
                    else:
                        error_msg = result.comment if result else "Unknown error"
                        print(f"       Failed to cancel order #{order.ticket}: {error_msg}")
                        
            except Exception as e:
                print(f"      ⚠️ Error processing order for cancellation: {e}")
                continue
        
        if cancelled_count > 0:
            print(f"      🧹 Regulation complete: Cancelled {cancelled_count} unauthorized orders")
        
        return cancelled_count
    
    def is_order_too_close_to_positions(order_to_check, existing_positions, check_opposite=False):
        """
        Check if a pending order is too close to any existing open position
        using risk-based proximity detection with halved risk threshold.
        
        Performs two types of checks:
        1. SAME DIRECTION: Compares SELL orders with SELL positions, BUY with BUY
        2. OPPOSITE DIRECTION (optional): Compares SELL orders with BUY positions,
           BUY orders with SELL positions (if check_opposite=True)
        
        The risk threshold is HALF (position_risk / 2) for both check types.
        
        Args:
            order_to_check: Order dictionary from signals.json
            existing_positions: Dictionary of ALL existing positions from MT5
            check_opposite: Boolean, whether to check opposite directions
            
        Returns:
            tuple: (is_too_close, closest_position_details, reason, check_type)
        """
        symbol = order_to_check.get('symbol', '')
        order_type = order_to_check.get('order_type', '')
        entry_price = order_to_check.get('entry', 0)
        volume = order_to_check.get('volume', 0.01)
        
        # Determine direction of the pending order
        is_pending_buy = 'buy' in order_type.lower()
        is_pending_sell = 'sell' in order_type.lower()
        
        if not is_pending_buy and not is_pending_sell:
            return False, None, "Not a buy/sell order", "none"
        
        # Filter positions for this symbol only
        symbol_positions = []
        for pos_key, position in existing_positions.items():
            if position['symbol'] == symbol:
                symbol_positions.append(position)
        
        if not symbol_positions:
            return False, None, "No positions to check against", "none"
        
        print(f"          🔍 Checking against {len(symbol_positions)} existing positions for closeness...")
        
        # Check each position for closeness
        for position in symbol_positions:
            position_type = position['type']  # mt5.ORDER_TYPE_BUY or mt5.ORDER_TYPE_SELL
            position_entry = position['price']
            position_sl = position['sl']
            position_volume = position['volume']
            position_ticket = position['ticket']
            
            is_position_buy = (position_type == mt5.ORDER_TYPE_BUY)
            is_position_sell = (position_type == mt5.ORDER_TYPE_SELL)
            
            # =====================================================
            # DETERMINE WHICH CHECK TO PERFORM
            # =====================================================
            
            # CASE 1: SAME DIRECTION CHECK
            if (is_pending_buy and is_position_buy) or (is_pending_sell and is_position_sell):
                check_type = "same_direction"
                print(f"            ✅ Same direction: {'BUY' if is_pending_buy else 'SELL'} pending with {'BUY' if is_position_buy else 'SELL'} position #{position_ticket}")
                
                # Calculate position's own risk
                position_risk = 0
                if position_sl and position_sl > 0:
                    if is_position_buy:
                        # For buy position, risk is entry - SL
                        position_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, position_volume,
                            position_entry, position_sl
                        )
                    else:  # sell position
                        # For sell position, risk is SL - entry
                        position_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, position_volume,
                            position_entry, position_sl
                        )
                    
                    if position_risk_profit is not None:
                        position_risk = abs(position_risk_profit)
                
                if position_risk == 0:
                    print(f"            ⚠️ Position #{position_ticket} has no SL or risk calc failed - skipping closeness check")
                    continue
                
                # HALVE THE POSITION RISK FOR THRESHOLD
                risk_threshold = position_risk / 2
                
                print(f"            Position #{position_ticket}: {'BUY' if is_position_buy else 'SELL'} @ {position_entry}, Risk: ${position_risk:.2f}")
                print(f"            📊 Risk threshold (50%): ${risk_threshold:.2f}")
                
                # SAME DIRECTION RISK CALCULATION
                if is_pending_sell and is_position_sell:
                    if entry_price < position_entry:
                        # SELL pending below SELL position
                        print(f"            📐 SELL pending @ {entry_price} below SELL position @ {position_entry}")
                        print(f"            🧮 Calculating risk: Entry={entry_price}, Exit={position_entry}")
                        
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, volume,
                            entry_price, position_entry
                        )
                    else:
                        # SELL pending above SELL position
                        print(f"            📐 SELL pending @ {entry_price} above SELL position @ {position_entry}")
                        print(f"            🧮 Calculating risk: Entry={position_entry}, Exit={entry_price}")
                        
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, volume,
                            position_entry, entry_price
                        )
                    
                    if potential_risk_profit is not None:
                        potential_risk = abs(potential_risk_profit)
                        print(f"            💰 Potential risk if triggered: ${potential_risk:.2f}")
                        print(f"            📊 Position risk threshold: ${risk_threshold:.2f}")
                        
                        if potential_risk < risk_threshold:
                            print(f"             TOO CLOSE (same direction): ${potential_risk:.2f} < ${risk_threshold:.2f}")
                            return True, position, f"Too close to same-direction {('BUY' if is_position_buy else 'SELL')} position #{position_ticket}", check_type
                        else:
                            print(f"            ✅ SAFE DISTANCE: ${potential_risk:.2f} >= ${risk_threshold:.2f}")
                
                elif is_pending_buy and is_position_buy:
                    if entry_price > position_entry:
                        # BUY pending above BUY position
                        print(f"            📐 BUY pending @ {entry_price} above BUY position @ {position_entry}")
                        print(f"            🧮 Calculating risk: Entry={position_entry}, Exit={entry_price}")
                        
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, volume,
                            position_entry, entry_price
                        )
                    else:
                        # BUY pending below BUY position
                        print(f"            📐 BUY pending @ {entry_price} below BUY position @ {position_entry}")
                        print(f"            🧮 Calculating risk: Entry={entry_price}, Exit={position_entry}")
                        
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, volume,
                            entry_price, position_entry
                        )
                    
                    if potential_risk_profit is not None:
                        potential_risk = abs(potential_risk_profit)
                        print(f"            💰 Potential risk if triggered: ${potential_risk:.2f}")
                        print(f"            📊 Position risk threshold: ${risk_threshold:.2f}")
                        
                        if potential_risk < risk_threshold:
                            print(f"             TOO CLOSE (same direction): ${potential_risk:.2f} < ${risk_threshold:.2f}")
                            return True, position, f"Too close to same-direction {('BUY' if is_position_buy else 'SELL')} position #{position_ticket}", check_type
                        else:
                            print(f"            ✅ SAFE DISTANCE: ${potential_risk:.2f} >= ${risk_threshold:.2f}")
            
            # =====================================================
            # CASE 2: OPPOSITE DIRECTION CHECK (if enabled)
            # =====================================================
            elif check_opposite and ((is_pending_buy and is_position_sell) or (is_pending_sell and is_position_buy)):
                check_type = "opposite_direction"
                print(f"            🔄 Opposite direction: {'BUY' if is_pending_buy else 'SELL'} pending with {'SELL' if is_position_sell else 'BUY'} position #{position_ticket}")
                
                # Calculate position's own risk
                position_risk = 0
                if position_sl and position_sl > 0:
                    if is_position_buy:
                        # For buy position, risk is entry - SL
                        position_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, position_volume,
                            position_entry, position_sl
                        )
                    else:  # sell position
                        # For sell position, risk is SL - entry
                        position_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, position_volume,
                            position_entry, position_sl
                        )
                    
                    if position_risk_profit is not None:
                        position_risk = abs(position_risk_profit)
                
                if position_risk == 0:
                    print(f"            ⚠️ Position #{position_ticket} has no SL or risk calc failed - skipping closeness check")
                    continue
                
                # HALVE THE POSITION RISK FOR THRESHOLD
                risk_threshold = position_risk / 2
                
                print(f"            Position #{position_ticket}: {'BUY' if is_position_buy else 'SELL'} @ {position_entry}, Risk: ${position_risk:.2f}")
                print(f"            📊 Risk threshold (50%): ${risk_threshold:.2f}")
                
                # OPPOSITE DIRECTION RISK CALCULATION
                # We calculate the risk if both orders were to move toward each other
                
                if is_pending_buy and is_position_sell:
                    # Pending BUY order vs existing SELL position
                    # They are moving in opposite directions
                    # The risk scenario: BUY pending triggers, price goes down to SELL position's entry
                    
                    if entry_price > position_entry:
                        # BUY pending above SELL position
                        print(f"            📐 BUY pending @ {entry_price} above SELL position @ {position_entry}")
                        print(f"            🧮 Calculating opposite risk: Entry={position_entry}, Exit={entry_price}")
                        
                        # Risk = from SELL position's entry to BUY pending's entry
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        )
                    else:
                        # BUY pending below SELL position
                        print(f"            📐 BUY pending @ {entry_price} below SELL position @ {position_entry}")
                        print(f"            🧮 Calculating opposite risk: Entry={entry_price}, Exit={position_entry}")
                        
                        # Risk = from BUY pending's entry to SELL position's entry
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        )
                    
                    if potential_risk_profit is not None:
                        potential_risk = abs(potential_risk_profit)
                        print(f"            💰 Potential opposite risk: ${potential_risk:.2f}")
                        print(f"            📊 Position risk threshold: ${risk_threshold:.2f}")
                        
                        if potential_risk < risk_threshold:
                            print(f"             TOO CLOSE (opposite direction): ${potential_risk:.2f} < ${risk_threshold:.2f}")
                            return True, position, f"Too close to opposite-direction SELL position #{position_ticket}", check_type
                        else:
                            print(f"            ✅ SAFE DISTANCE (opposite): ${potential_risk:.2f} >= ${risk_threshold:.2f}")
                
                elif is_pending_sell and is_position_buy:
                    # Pending SELL order vs existing BUY position
                    
                    if entry_price < position_entry:
                        # SELL pending below BUY position
                        print(f"            📐 SELL pending @ {entry_price} below BUY position @ {position_entry}")
                        print(f"            🧮 Calculating opposite risk: Entry={entry_price}, Exit={position_entry}")
                        
                        # Risk = from SELL pending's entry to BUY position's entry
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_SELL, symbol, min(volume, position_volume),
                            entry_price, position_entry
                        )
                    else:
                        # SELL pending above BUY position
                        print(f"            📐 SELL pending @ {entry_price} above BUY position @ {position_entry}")
                        print(f"            🧮 Calculating opposite risk: Entry={position_entry}, Exit={entry_price}")
                        
                        # Risk = from BUY position's entry to SELL pending's entry
                        potential_risk_profit = mt5.order_calc_profit(
                            mt5.ORDER_TYPE_BUY, symbol, min(volume, position_volume),
                            position_entry, entry_price
                        )
                    
                    if potential_risk_profit is not None:
                        potential_risk = abs(potential_risk_profit)
                        print(f"            💰 Potential opposite risk: ${potential_risk:.2f}")
                        print(f"            📊 Position risk threshold: ${risk_threshold:.2f}")
                        
                        if potential_risk < risk_threshold:
                            print(f"             TOO CLOSE (opposite direction): ${potential_risk:.2f} < ${risk_threshold:.2f}")
                            return True, position, f"Too close to opposite-direction BUY position #{position_ticket}", check_type
                        else:
                            print(f"            ✅ SAFE DISTANCE (opposite): ${potential_risk:.2f} >= ${risk_threshold:.2f}")
            
            else:
                # Skip if not matching either check type
                continue
        
        return False, None, "All positions are at safe distance", "none"

    def order_exists(order_to_check, existing_orders, existing_positions):
        """
        Check if an order already exists in MT5 (either pending or as open position).
        
        Args:
            order_to_check: Order dictionary from signals.json
            existing_orders: Dictionary of ALL existing pending orders from MT5
            existing_positions: Dictionary of ALL existing positions from MT5
            
        Returns:
            tuple: (exists, type) where type is 'pending', 'position', or None
        """
        order_type = order_to_check.get('order_type', '')
        entry_price = order_to_check.get('entry', 0)
        volume = order_to_check.get('volume', 0.01)
        symbol = order_to_check.get('symbol', '')
        
        # Round volume to avoid floating point precision issues
        volume = round(volume, 2)
        
        # Map order type string to MT5 order type for pending orders
        mt5_pending_type = None
        if order_type == 'buy_stop':
            mt5_pending_type = mt5.ORDER_TYPE_BUY_STOP
        elif order_type == 'buy_limit':
            mt5_pending_type = mt5.ORDER_TYPE_BUY_LIMIT
        elif order_type == 'sell_stop':
            mt5_pending_type = mt5.ORDER_TYPE_SELL_STOP
        elif order_type == 'sell_limit':
            mt5_pending_type = mt5.ORDER_TYPE_SELL_LIMIT
        
        # Map order type string to MT5 position type
        mt5_position_type = None
        if order_type in ['buy_stop', 'buy_limit']:
            mt5_position_type = mt5.ORDER_TYPE_BUY
        elif order_type in ['sell_stop', 'sell_limit']:
            mt5_position_type = mt5.ORDER_TYPE_SELL
        
        # Check pending orders first
        if mt5_pending_type:
            pending_key = f"{symbol}_{mt5_pending_type}_{entry_price}_{volume}"
            if pending_key in existing_orders:
                print(f"          🔍 Found duplicate PENDING: {symbol} {order_type} @ {entry_price}")
                return True, 'pending'
        
        # Check open positions
        if mt5_position_type:
            position_key = f"{symbol}_{mt5_position_type}_{entry_price}_{volume}"
            if position_key in existing_positions:
                print(f"          🔍 Found duplicate POSITION: {symbol} {order_type} @ {entry_price}")
                return True, 'position'
        
        return False, None
    
    def convert_order_type_logic(order_type):
        """
        Convert order type according to new logic:
        - buy_stop → sell_limit
        - sell_stop → buy_limit
        - buy_limit → sell_stop (if needed for other conversions)
        - sell_limit → buy_stop (if needed for other conversions)
        
        Args:
            order_type: Original order type string
            
        Returns:
            str: Converted order type
        """
        conversion_map = {
            'buy_stop': 'sell_limit',
            'sell_stop': 'buy_limit',
            'buy_limit': 'sell_stop',
            'sell_limit': 'buy_stop'
        }
        return conversion_map.get(order_type, order_type)
    
    def get_valid_price_for_conversion(original_order_type, target_order_type, current_price, original_entry):
        """
        Get a valid price for the converted order type.
        
        Args:
            original_order_type: Original order type
            target_order_type: Target order type after conversion
            current_price: Current market price (bid or ask as appropriate)
            original_entry: Original entry price
            
        Returns:
            float: Valid price for the target order type
        """
        # For buy orders (now converted to sell orders)
        if target_order_type == 'sell_limit':
            # Sell limit must be above bid - use original price if it's above bid, otherwise use bid + 1 pip
            if original_entry > current_price:
                return original_entry
            else:
                # Use current price plus a small buffer (1 point)
                return current_price + 0.01
        
        elif target_order_type == 'sell_stop':
            # Sell stop must be below bid - use original price if it's below bid, otherwise use bid - 1 pip
            if original_entry < current_price:
                return original_entry
            else:
                # Use current price minus a small buffer (1 point)
                return current_price - 0.01
        
        # For sell orders (now converted to buy orders)
        elif target_order_type == 'buy_limit':
            # Buy limit must be below ask - use original price if it's below ask, otherwise use ask - 1 pip
            if original_entry < current_price:
                return original_entry
            else:
                # Use current price minus a small buffer (1 point)
                return current_price - 0.01
        
        elif target_order_type == 'buy_stop':
            # Buy stop must be above ask - use original price if it's above ask, otherwise use ask + 1 pip
            if original_entry > current_price:
                return original_entry
            else:
                # Use current price plus a small buffer (1 point)
                return current_price + 0.01
        
        return original_entry
    
    def get_current_price(symbol):
        """
        Get current market price for a symbol.
        
        Args:
            symbol: Symbol name
            
        Returns:
            tuple: (bid, ask) current prices or (None, None) if error
        """
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            return None, None
        return tick.bid, tick.ask
    
    def place_exact_order_type(order_data, is_counter=False, is_converted=False):
        """
        Place an order in MT5 with the exact order type and price.
        
        Args:
            order_data: Dictionary with order parameters
            is_counter: Boolean indicating if this is a counter order
            is_converted: Boolean indicating if this is a converted order type
            
        Returns:
            tuple: (success, order_result, error_message, final_order_type, final_price)
        """
        try:
            # Prepare order request
            symbol = order_data.get('symbol')
            order_type = order_data.get('order_type')
            volume = order_data.get('volume', 0.01)
            entry_price = order_data.get('entry')
            stoploss = order_data.get('exit')
            comment = order_data.get('comment', '')
            magic = order_data.get('magic', 0)
            
            # Map order type string to MT5 order type
            if order_type == 'buy_stop':
                mt5_type = mt5.ORDER_TYPE_BUY_STOP
                order_direction = "BUY STOP"
            elif order_type == 'buy_limit':
                mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                order_direction = "BUY LIMIT"
            elif order_type == 'sell_stop':
                mt5_type = mt5.ORDER_TYPE_SELL_STOP
                order_direction = "SELL STOP"
            elif order_type == 'sell_limit':
                mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                order_direction = "SELL LIMIT"
            else:
                return False, None, f"Invalid order type: {order_type}", order_type, entry_price
            
            # Get symbol info
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                return False, None, f"Cannot get symbol info for {symbol}", order_type, entry_price
            
            # Prepare the request
            request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": float(volume),
                "type": mt5_type,
                "price": float(entry_price),
                "sl": float(stoploss) if stoploss else 0.0,
                "tp": 0.0,
                "deviation": 20,
                "magic": magic,
                "comment": comment[:31] if comment else f"{'COUNTER' if is_counter else 'MAIN'}{' (CONVERTED)' if is_converted else ''}",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_RETURN,
            }
            
            # Send order
            result = mt5.order_send(request)
            
            # Check if result is None
            if result is None:
                error_code = mt5.last_error()
                error_msg = f"Order send failed: {error_code}"
                print(f"           {error_msg}")
                return False, None, error_msg, order_type, entry_price
            
            if result.retcode != mt5.TRADE_RETCODE_DONE:
                error_msg = f"Order failed: {result.comment} (code: {result.retcode})"
                print(f"           {error_msg}")
                return False, None, error_msg, order_type, entry_price
            
            print(f"          ✅ {order_direction} placed successfully: Ticket #{result.order}{' (CONVERTED)' if is_converted else ''} at {entry_price:.2f}")
            print(f"          ℹ️ This is a PENDING ORDER - will auto-execute when price reaches {entry_price}")
            return True, result, None, order_type, entry_price
            
        except Exception as e:
            error_msg = f"Exception placing order: {str(e)}"
            print(f"           {error_msg}")
            return False, None, error_msg, order_data.get('order_type', 'unknown'), order_data.get('entry', 0)
    
    def place_counter_order(main_order, main_ticket, symbol, magic_number, existing_orders, existing_positions, allow_conversion, check_opposite):
        """
        Place the counter order for a successfully placed main order.
        
        Args:
            main_order: Original main order dictionary that contains order_counter
            main_ticket: Ticket number of the successfully placed main order
            symbol: Symbol name
            magic_number: Magic number for orders
            existing_orders: Dictionary of existing pending orders
            existing_positions: Dictionary of existing positions
            allow_conversion: Whether order type conversion is allowed
            check_opposite: Whether opposite direction checks are enabled
            
        Returns:
            tuple: (success, result, error_message, was_skipped, skip_reason, was_converted)
        """
        print(f"\n          🔄 PLACING COUNTER ORDER for main ticket {main_ticket}...")
        
        # Check if counter order exists in the main order
        if 'order_counter' not in main_order:
            print(f"          ℹ️ No counter order found in main order data")
            return False, None, None, True, 'no_counter', False
        
        counter_order = main_order['order_counter'].copy()
        
        # Add required fields to counter order
        counter_order['symbol'] = symbol
        counter_order['magic'] = magic_number
        
        # Check if counter order already exists
        exists, exists_type = order_exists(counter_order, existing_orders, existing_positions)
        
        if exists:
            order_type = counter_order.get('order_type', 'unknown')
            entry = counter_order.get('entry', 0)
            if exists_type == 'pending':
                print(f"          ⏭️ Counter {order_type} @ {entry} - SKIP (already exists as PENDING order)")
                return False, None, None, True, 'pending', False
            else:  # position
                print(f"          ⏭️ Counter {order_type} @ {entry} - SKIP (already exists as OPEN POSITION)")
                return False, None, None, True, 'position', False
        
        # Check if counter order is too close to positions (both same and opposite direction if enabled)
        is_too_close_same, close_position_same, close_reason_same, check_type_same = is_order_too_close_to_positions(
            counter_order, existing_positions, check_opposite=False
        )
        
        if is_too_close_same:
            order_type = counter_order.get('order_type', 'unknown')
            entry = counter_order.get('entry', 0)
            print(f"          ⏭️ Counter {order_type} @ {entry} - SKIP (too close to same-direction position)")
            print(f"          📋 Reason: {close_reason_same}")
            return False, None, None, True, 'too_close_same', False
        
        # Check opposite direction if enabled
        if check_opposite:
            is_too_close_opposite, close_position_opposite, close_reason_opposite, check_type_opposite = is_order_too_close_to_positions(
                counter_order, existing_positions, check_opposite=True
            )
            
            if is_too_close_opposite:
                order_type = counter_order.get('order_type', 'unknown')
                entry = counter_order.get('entry', 0)
                print(f"          ⏭️ Counter {order_type} @ {entry} - SKIP (too close to opposite-direction position)")
                print(f"          📋 Reason: {close_reason_opposite}")
                return False, None, None, True, 'too_close_opposite', False
        
        # Place the counter order
        order_type = counter_order.get('order_type', 'unknown')
        entry = counter_order.get('entry', 0)
        print(f"          📤 Placing counter {order_type} @ {entry}...")
        
        # Try to place the exact order type
        success, result, error, final_type, final_price = place_exact_order_type(counter_order, True, False)
        
        # If failed due to invalid price, check if conversion is allowed
        if not success and error and ("Invalid price" in error or "10015" in error):
            if allow_conversion:
                print(f"          🔄 Invalid price detected for counter order and conversion is ALLOWED, attempting conversion...")
                success, result, error, was_converted, final_type, final_price = convert_and_place_order(
                    counter_order, error, True, 0
                )
                return success, result, error, False, None, was_converted
            else:
                print(f"          ⚠️ Invalid price detected but conversion is NOT ALLOWED (allow_order_type_conversion=false)")
                print(f"           Counter order failed permanently: {error}")
                return False, None, error, False, None, False
        
        return success, result, error, False, None, False
    
    def convert_and_place_order(order_data, original_error, is_counter=False, conversion_attempts=0, existing_pending_orders=None, existing_positions=None):
        """
        Convert order type and place with adjusted price when original order fails.
        NEW LOGIC: Convert stop orders to limit orders:
        - buy_stop → sell_limit
        - sell_stop → buy_limit
        
        Also handles existing stop orders by modifying them to limit orders.
        
        Args:
            order_data: Original order dictionary
            original_error: Original error message
            is_counter: Boolean indicating if this is a counter order
            conversion_attempts: Number of conversion attempts already made
            existing_pending_orders: Dictionary of existing pending orders to check/modify
            existing_positions: Dictionary of existing positions to check/modify
            
        Returns:
            tuple: (success, order_result, error_message, was_converted, final_order_type, final_price)
        """
        if conversion_attempts >= 2:
            return False, None, f"Max conversion attempts reached. Original error: {original_error}", False, order_data.get('order_type'), order_data.get('entry')
        
        symbol = order_data.get('symbol')
        original_order_type = order_data.get('order_type')
        entry_price = order_data.get('entry')
        stoploss = order_data.get('exit')
        volume = round(order_data.get('volume', 0.01), 2)
        magic = order_data.get('magic', 0)
        
        # Get current price for conversion
        bid, ask = get_current_price(symbol)
        
        # Convert order type using new logic
        converted_type = convert_order_type_logic(original_order_type)
        
        # Determine which price to use based on converted order type
        if 'sell' in converted_type:
            # For sell orders, use bid price
            current_price = bid
        else:
            # For buy orders, use ask price
            current_price = ask
        
        # Get valid price for converted order
        valid_price = get_valid_price_for_conversion(original_order_type, converted_type, current_price, entry_price)
        
        print(f"          🔄 CONVERTING {original_order_type.upper()} @ {entry_price:.2f} → {converted_type.upper()} @ {valid_price:.2f}")
        print(f"          ℹ️ Original stop order will NOT be placed - only converted limit order will be placed")
        
        # =====================================================
        # CHECK FOR EXISTING STOP ORDERS TO CONVERT
        # =====================================================
        existing_stop_orders_found = []
        
        # Check if there are existing pending stop orders of the original type
        if existing_pending_orders:
            # Map original order type to MT5 order type
            original_mt5_type = None
            if original_order_type == 'buy_stop':
                original_mt5_type = mt5.ORDER_TYPE_BUY_STOP
            elif original_order_type == 'sell_stop':
                original_mt5_type = mt5.ORDER_TYPE_SELL_STOP
            elif original_order_type == 'buy_limit':
                original_mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
            elif original_order_type == 'sell_limit':
                original_mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
            
            # Look for existing pending stop orders with the same parameters
            if original_mt5_type:
                for order_key, pending_order in existing_pending_orders.items():
                    if (pending_order['symbol'] == symbol and 
                        pending_order['type'] == original_mt5_type and
                        abs(pending_order['price'] - entry_price) < 0.01 and  # Small tolerance for price
                        abs(pending_order['volume'] - volume) < 0.01):  # Small tolerance for volume
                        
                        existing_stop_orders_found.append(pending_order)
                        print(f"          🔍 Found existing pending stop order: Ticket #{pending_order['ticket']}")
        
        # =====================================================
        # HANDLE EXISTING STOP ORDERS BY MODIFYING THEM
        # =====================================================
        if existing_stop_orders_found:
            print(f"          🔄 Found {len(existing_stop_orders_found)} existing stop orders to convert/modify")
            
            # Process each existing stop order
            for existing_order in existing_stop_orders_found:
                print(f"          📝 Modifying existing order Ticket #{existing_order['ticket']}...")
                
                # Create modification request to change order type and price
                # Note: MT5 doesn't allow changing order type directly, so we need to:
                # 1. Remove the existing order
                # 2. Place a new converted order
                
                # Step 1: Remove the existing order
                remove_request = {
                    "action": mt5.TRADE_ACTION_REMOVE,
                    "order": existing_order['ticket'],
                    "magic": magic,
                    "comment": "Removed for conversion"
                }
                
                remove_result = mt5.order_send(remove_request)
                
                if remove_result and remove_result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"          ✅ Removed existing stop order Ticket #{existing_order['ticket']} for conversion")
                    
                    # Step 2: Create converted order data
                    converted_order = order_data.copy()
                    converted_order['order_type'] = converted_type
                    converted_order['entry'] = valid_price
                    
                    # Adjust stoploss proportionally if present
                    if stoploss:
                        original_distance = abs(entry_price - stoploss)
                        if 'sell' in converted_type:
                            converted_order['exit'] = valid_price + original_distance
                        else:
                            converted_order['exit'] = valid_price - original_distance
                    
                    # Step 3: Place the new converted order
                    print(f"          📤 Placing converted order: {converted_type} @ {valid_price:.2f}")
                    success, result, error, final_type, final_price = place_exact_order_type(
                        converted_order, is_counter, True
                    )
                    
                    if success:
                        print(f"          ✅ SUCCESSFULLY CONVERTED EXISTING STOP ORDER: Ticket #{existing_order['ticket']} → {converted_type.upper()} @ {valid_price:.2f}")
                        
                        # Update existing_pending_orders dictionary
                        if existing_pending_orders is not None:
                            # Remove old order from dictionary
                            old_key = f"{symbol}_{original_mt5_type}_{entry_price}_{volume}"
                            if old_key in existing_pending_orders:
                                del existing_pending_orders[old_key]
                            
                            # Add new order to dictionary
                            new_mt5_type = None
                            if converted_type == 'buy_stop':
                                new_mt5_type = mt5.ORDER_TYPE_BUY_STOP
                            elif converted_type == 'buy_limit':
                                new_mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                            elif converted_type == 'sell_stop':
                                new_mt5_type = mt5.ORDER_TYPE_SELL_STOP
                            elif converted_type == 'sell_limit':
                                new_mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                            
                            if new_mt5_type:
                                new_key = f"{symbol}_{new_mt5_type}_{valid_price}_{volume}"
                                existing_pending_orders[new_key] = {
                                    'ticket': result.order,
                                    'symbol': symbol,
                                    'type': new_mt5_type,
                                    'price': valid_price,
                                    'volume': volume,
                                    'type_string': "PENDING"
                                }
                        
                        return success, result, error, True, final_type, final_price
                    else:
                        print(f"           Failed to place converted order after removing existing stop order: {error}")
                        return False, None, f"Failed to place converted order after removal: {error}", False, original_order_type, entry_price
                else:
                    error_msg = remove_result.comment if remove_result else "Unknown error"
                    print(f"           Failed to remove existing stop order Ticket #{existing_order['ticket']}: {error_msg}")
                    return False, None, f"Failed to remove existing stop order: {error_msg}", False, original_order_type, entry_price
        
        # =====================================================
        # CHECK FOR EXISTING OPEN POSITIONS THAT ARE STOP ORDERS
        # =====================================================
        # Note: Open positions don't have "stop order" types, but we can check if they were 
        # created from stop orders and should be managed differently
        
        existing_stop_positions_found = []
        
        # Check for positions that might need conversion (this is less common)
        if existing_positions:
            for pos_key, position in existing_positions.items():
                # We can't convert open positions directly, but we might want to place a protective order
                if position['symbol'] == symbol:
                    # Check if this position might have been created from a stop order
                    # This is a simple heuristic - you may want to add more logic
                    if abs(position['price'] - entry_price) < 0.01:
                        existing_stop_positions_found.append(position)
                        print(f"          🔍 Found existing position #{position['ticket']} that might need protection")
        
        # =====================================================
        # CREATE NEW CONVERTED ORDER (NO EXISTING STOP ORDERS)
        # =====================================================
        # Create converted order data
        converted_order = order_data.copy()
        converted_order['order_type'] = converted_type
        converted_order['entry'] = valid_price
        
        # Adjust stoploss proportionally if present
        if stoploss:
            original_distance = abs(entry_price - stoploss)
            if 'sell' in converted_type:
                converted_order['exit'] = valid_price + original_distance
            else:
                converted_order['exit'] = valid_price - original_distance
        
        # Try to place the converted order
        success, result, error, final_type, final_price = place_exact_order_type(
            converted_order, is_counter, True
        )
        
        if success:
            print(f"          ✅ SUCCESSFULLY CONVERTED AND PLACED: {original_order_type.upper()} → {converted_type.upper()}")
            return success, result, error, True, final_type, final_price
        else:
            # If conversion fails, try one more time with different adjustment
            if conversion_attempts < 1:
                print(f"          🔄 First conversion attempt failed, trying alternative adjustment...")
                # Adjust price differently based on converted order type
                if converted_type in ['buy_limit', 'sell_stop']:
                    # Need lower price
                    valid_price = current_price - 0.02
                else:
                    # Need higher price
                    valid_price = current_price + 0.02
                
                converted_order['entry'] = valid_price
                if stoploss:
                    original_distance = abs(entry_price - stoploss)
                    if 'sell' in converted_type:
                        converted_order['exit'] = valid_price + original_distance
                    else:
                        converted_order['exit'] = valid_price - original_distance
                
                success, result, error, final_type, final_price = place_exact_order_type(
                    converted_order, is_counter, True
                )
                
                if success:
                    print(f"          ✅ SUCCESSFULLY CONVERTED AND PLACED (alternative): {original_order_type.upper()} → {converted_type.upper()}")
                    return success, result, error, True, final_type, final_price
            
            return False, None, f"Conversion failed: {error}", False, original_order_type, entry_price

    def convert_existing_stop_orders(symbol, magic_number, existing_orders, allow_conversion=True):
        """
        Helper function to convert all existing stop orders to limit orders.
        Scans all pending orders and converts buy_stop → sell_limit and sell_stop → buy_limit.
        
        Args:
            symbol: Symbol to check
            magic_number: Magic number to identify orders
            existing_orders: Dictionary of existing pending orders
            allow_conversion: Whether conversion is allowed
            
        Returns:
            int: Number of orders converted
        """
        if not allow_conversion:
            return 0
        
        converted_count = 0
        orders_to_convert = []
        
        # Get all pending orders for this symbol and magic number
        orders = mt5.orders_get(symbol=symbol)
        if orders is None:
            return 0
        
        for order in orders:
            # Only manage orders with our magic number
            if order.magic != magic_number:
                continue
            
            # Check if this is a stop order that needs conversion
            order_type = order.type
            should_convert = False
            target_type = None
            
            if order_type == mt5.ORDER_TYPE_BUY_STOP:
                should_convert = True
                target_type = mt5.ORDER_TYPE_SELL_LIMIT
                print(f"      🔄 Found BUY_STOP order #{order.ticket} to convert to SELL_LIMIT")
            elif order_type == mt5.ORDER_TYPE_SELL_STOP:
                should_convert = True
                target_type = mt5.ORDER_TYPE_BUY_LIMIT
                print(f"      🔄 Found SELL_STOP order #{order.ticket} to convert to BUY_LIMIT")
            
            if should_convert:
                orders_to_convert.append({
                    'order': order,
                    'target_type': target_type
                })
        
        # Convert each order
        for convert_info in orders_to_convert:
            order = convert_info['order']
            target_type = convert_info['target_type']
            
            # Get current price for conversion
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                print(f"      ⚠️ Could not get price for {symbol}, skipping conversion")
                continue
            
            current_price = tick.ask if target_type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else tick.bid
            
            # Calculate new price (simple conversion - use same price or adjust)
            original_price = order.price
            new_price = original_price
            
            # Ensure price is valid for the target order type
            if target_type == mt5.ORDER_TYPE_SELL_LIMIT:
                # Sell limit must be above current price
                if new_price <= current_price:
                    new_price = current_price + 0.01  # Add 1 point buffer
            elif target_type == mt5.ORDER_TYPE_BUY_LIMIT:
                # Buy limit must be below current price
                if new_price >= current_price:
                    new_price = current_price - 0.01  # Subtract 1 point buffer
            
            print(f"      🔄 Converting order #{order.ticket}: {order_type_to_string(order.type)} @ {original_price} → {order_type_to_string(target_type)} @ {new_price}")
            
            # Step 1: Remove existing order
            remove_request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order.ticket,
                "magic": magic_number,
                "comment": "Removed for conversion"
            }
            
            remove_result = mt5.order_send(remove_request)
            
            if remove_result and remove_result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"      ✅ Removed original stop order #{order.ticket}")
                
                # Step 2: Place new converted order
                request = {
                    "action": mt5.TRADE_ACTION_PENDING,
                    "symbol": symbol,
                    "volume": order.volume_current,
                    "type": target_type,
                    "price": new_price,
                    "sl": order.sl,
                    "tp": order.tp,
                    "deviation": 20,
                    "magic": magic_number,
                    "comment": f"Converted from {order_type_to_string(order.type)}",
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": mt5.ORDER_FILLING_RETURN,
                }
                
                result = mt5.order_send(request)
                
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    print(f"      ✅ Successfully placed converted order: Ticket #{result.order}")
                    converted_count += 1
                    
                    # Update the existing_orders dictionary
                    if existing_orders is not None:
                        # Remove old order from dict
                        old_key = f"{symbol}_{order.type}_{original_price}_{round(order.volume_current, 2)}"
                        if old_key in existing_orders:
                            del existing_orders[old_key]
                        
                        # Add new order to dict
                        new_key = f"{symbol}_{target_type}_{new_price}_{round(order.volume_current, 2)}"
                        existing_orders[new_key] = {
                            'ticket': result.order,
                            'symbol': symbol,
                            'type': target_type,
                            'price': new_price,
                            'volume': order.volume_current,
                            'type_string': "PENDING"
                        }
                else:
                    error_msg = result.comment if result else "Unknown error"
                    print(f"       Failed to place converted order: {error_msg}")
            else:
                error_msg = remove_result.comment if remove_result else "Unknown error"
                print(f"       Failed to remove order #{order.ticket}: {error_msg}")
        
        return converted_count

    def order_type_to_string(order_type):
        """Helper to convert MT5 order type to string"""
        type_map = {
            mt5.ORDER_TYPE_BUY_STOP: "BUY_STOP",
            mt5.ORDER_TYPE_SELL_STOP: "SELL_STOP",
            mt5.ORDER_TYPE_BUY_LIMIT: "BUY_LIMIT",
            mt5.ORDER_TYPE_SELL_LIMIT: "SELL_LIMIT",
            mt5.ORDER_TYPE_BUY: "BUY",
            mt5.ORDER_TYPE_SELL: "SELL"
        }
        return type_map.get(order_type, "UNKNOWN")

    def process_single_order(order, symbol, magic_number, existing_orders, existing_positions, 
                        is_counter=False, allow_conversion=False, check_opposite=False):
        """
        Process and place a single order with enhanced conversion that handles existing orders.
        """
        # Add symbol and magic to order data
        order['symbol'] = symbol
        order['magic'] = magic_number
        
        # STEP 0: Convert any existing stop orders before processing
        if allow_conversion:
            print(f"          🔄 Checking for existing stop orders to convert...")
            converted_count = convert_existing_stop_orders(symbol, magic_number, existing_orders, allow_conversion)
            if converted_count > 0:
                print(f"          ✅ Converted {converted_count} existing stop orders to limit orders")
                # Refresh existing_orders after conversion
                existing_orders = get_all_existing_pending_orders()
        
        # STEP 1: Check if order already exists (either pending or as position)
        exists, exists_type = order_exists(order, existing_orders, existing_positions)
        
        if exists:
            order_type = order.get('order_type', 'unknown')
            entry = order.get('entry', 0)
            if exists_type == 'pending':
                print(f"          ⏭️ {'Counter' if is_counter else 'Main'} {order_type} @ {entry} - SKIP (already exists as PENDING order)")
                return False, None, None, True, 'pending', False, entry, 'duplicate_pending'
            else:  # position
                print(f"          ⏭️ {'Counter' if is_counter else 'Main'} {order_type} @ {entry} - SKIP (already exists as OPEN POSITION)")
                return False, None, None, True, 'position', False, entry, 'duplicate_position'
        
        # STEP 2: Check if order is too close to any existing position - SAME DIRECTION first
        is_too_close_same, close_position_same, close_reason_same, check_type_same = is_order_too_close_to_positions(
            order, existing_positions, check_opposite=False
        )
        
        if is_too_close_same:
            order_type = order.get('order_type', 'unknown')
            entry = order.get('entry', 0)
            print(f"          ⏭️ {'Counter' if is_counter else 'Main'} {order_type} @ {entry} - SKIP (too close to same-direction position)")
            print(f"          📋 Reason: {close_reason_same}")
            return False, None, None, True, 'too_close_same', False, entry, 'same_direction'
        
        # STEP 3: Check opposite direction if enabled
        if check_opposite:
            is_too_close_opposite, close_position_opposite, close_reason_opposite, check_type_opposite = is_order_too_close_to_positions(
                order, existing_positions, check_opposite=True
            )
            
            if is_too_close_opposite:
                order_type = order.get('order_type', 'unknown')
                entry = order.get('entry', 0)
                print(f"          ⏭️ {'Counter' if is_counter else 'Main'} {order_type} @ {entry} - SKIP (too close to opposite-direction position)")
                print(f"          📋 Reason: {close_reason_opposite}")
                return False, None, None, True, 'too_close_opposite', False, entry, 'opposite_direction'
        
        # STEP 4: Place the order with potential conversion BEFORE placement
        order_type = order.get('order_type', 'unknown')
        entry = order.get('entry', 0)
        
        # Check if this is a stop order that needs conversion BEFORE placement attempt
        should_convert_preemptively = allow_conversion and (order_type in ['buy_stop', 'sell_stop'])
        
        if should_convert_preemptively:
            print(f"          🔄 CONVERSION MODE ACTIVE: {order_type.upper()} will be converted to opposite limit order")
            print(f"          ℹ️ Original stop order will NOT be placed - only converted limit order will be attempted")
            
            # Convert the order before trying to place (pass existing_orders and existing_positions for conversion)
            success, result, error, was_converted, final_type, final_price = convert_and_place_order(
                order, "Preemptive conversion", is_counter, 0, existing_orders, existing_positions
            )
            return success, result, error, False, None, was_converted, final_price, 'pre_converted'
        
        else:
            # No pre-conversion needed, try to place the original order type
            print(f"          📤 Placing {'counter' if is_counter else 'main'} {order_type} @ {entry}...")
            
            # Try to place the exact order type
            success, result, error, final_type, final_price = place_exact_order_type(order, is_counter, False)
            
            # If failed due to invalid price, check if conversion is allowed (for non-stop orders)
            if not success and error and ("Invalid price" in error or "10015" in error):
                if allow_conversion:
                    print(f"          🔄 Invalid price detected and conversion is ALLOWED, attempting conversion...")
                    success, result, error, was_converted, final_type, final_price = convert_and_place_order(
                        order, error, is_counter, 0, existing_orders, existing_positions
                    )
                    return success, result, error, False, None, was_converted, final_price, 'converted'
                else:
                    print(f"          ⚠️ Invalid price detected but conversion is NOT ALLOWED (allow_order_type_conversion=false)")
                    print(f"           Order failed permanently: {error}")
                    return False, None, error, False, None, False, final_price, 'conversion_skipped'
            
            return success, result, error, False, None, False, final_price, 'placed'
        
        # If inv_id is provided, process only that investor

    def main():
        if inv_id:
            inv_root = Path(INV_PATH) / inv_id
            prices_dir = inv_root / "prices"
            
            # Path to accountmanagement.json and signals.json
            acc_mgmt_path = inv_root / "accountmanagement.json"
            signals_path = prices_dir / "signals.json"
            
            # =====================================================
            # STEP 1: CALL manage_single_position_and_pending FIRST
            # =====================================================
            print(f"\n [{inv_id}] 🔍 Checking single position/pending management flag...")
            
            # Call the function to get the upload_orders flag
            try:
                # Import the function (assuming it's in the same module or imported)
                # If it's in the same file, just call it directly
                management_result = manage_single_position_and_pending(inv_id=inv_id)
                upload_orders_flag = management_result.get("upload_orders", True)
                stats["upload_orders_flag"] = upload_orders_flag
                
                print(f" [{inv_id}] 🚩 upload_orders flag: {upload_orders_flag}")
                
                # If upload_orders is False, skip order placement entirely
                if not upload_orders_flag:
                    print(f" [{inv_id}] ⏭️ SKIPPING order placement - position with close opposite order exists")
                    print(f"     Reason: A position with a close opposite pending order was found and kept")
                    stats["investors_skipped_due_to_position_flag"] += 1
                    return stats
                else:
                    print(f" [{inv_id}] ✅ PROCEEDING with order placement - no conflicting position/order pair found")
                    
            except Exception as e:
                print(f" [{inv_id}] ⚠️ Could not check manage_single_position_and_pending: {e}")
                print(f" [{inv_id}] ⏭️ Proceeding with order placement by default (upload_orders assumed True)")
                stats["upload_orders_flag"] = True  # Default to True if function fails
            
            # =====================================================
            # STEP 2: Check account management settings
            # =====================================================
            print(f"\n [{inv_id}] 🔍 Checking account management settings...")
            
            if not acc_mgmt_path.exists():
                print(f" [{inv_id}] ⚠️ accountmanagement.json not found at {acc_mgmt_path}")
                print(f" [{inv_id}] ⏭️ Skipping order placement (no settings to check)")
                stats["investors_skipped_due_to_settings"] += 1
                return stats
            
            try:
                with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                    acc_config = json.load(f)
                
                # Check if auto trading is enabled
                settings = acc_config.get("settings", {})
                enable_auto_trading = settings.get("enable_auto_trading", False)
                
                if not enable_auto_trading:
                    print(f" [{inv_id}] ⏭️ Auto trading is DISABLED in accountmanagement.json")
                    print(f"     (settings.enable_auto_trading = false)")
                    stats["investors_skipped_due_to_settings"] += 1
                    return stats
                
                # Check if order type conversion is allowed
                allow_order_type_conversion = settings.get("enable_order_type_conversion", False)
                
                # Check if opposite order restriction is enabled
                opposite_order_restriction = settings.get("opposite_order_restriction", False)
                
                # Get trading parameters
                max_spread = settings.get("max_spread", 50)
                max_slippage = settings.get("max_slippage", 20)
                order_ttl = settings.get("order_ttl", 3600)
                
                print(f" [{inv_id}] ✅ Auto trading is ENABLED")
                print(f"     • Order type conversion: {'ALLOWED' if allow_order_type_conversion else 'NOT ALLOWED'}")
                print(f"     • Opposite order restriction: {'ENABLED' if opposite_order_restriction else 'DISABLED'}")
                print(f"     • Max spread: {max_spread} points")
                print(f"     • Max slippage: {max_slippage} points")
                print(f"     • Order TTL: {order_ttl} seconds")
                print(f"     • Pending orders: Will auto-execute when price reaches entry level")
                print(f"     • Conversion logic: STOP → LIMIT (buy_stop→sell_limit, sell_stop→buy_limit)")
                
                stats["investors_processed"] += 1
                
            except json.JSONDecodeError as e:
                print(f" [{inv_id}]  Invalid JSON in accountmanagement.json: {e}")
                print(f" [{inv_id}] ⏭️ Skipping order placement due to config error")
                stats["investors_skipped_due_to_settings"] += 1
                return stats
            except Exception as e:
                print(f" [{inv_id}]  Error reading accountmanagement.json: {e}")
                print(f" [{inv_id}] ⏭️ Skipping order placement due to config error")
                stats["investors_skipped_due_to_settings"] += 1
                return stats
            
            # Continue with order placement if enabled
            if not signals_path.exists():
                print(f" [{inv_id}]  signals.json not found at {signals_path}")
                return stats
            
            try:
                print(f" [{inv_id}] 📂 Loading signals.json...")
                with open(signals_path, 'r', encoding='utf-8') as f:
                    signals_data = json.load(f)
                
                # Get ALL existing pending orders for this account (no symbol filter)
                print(f" [{inv_id}] 🔍 Checking ALL existing pending orders...")
                existing_orders = get_all_existing_pending_orders()
                stats["total_active_pending"] = len(existing_orders)
                
                # Get ALL existing open positions for this account (no symbol filter)
                print(f" [{inv_id}] 🔍 Checking ALL existing open positions...")
                existing_positions = get_all_existing_positions()
                stats["total_open_positions"] = len(existing_positions)
                
                print(f" [{inv_id}] 📊 Account summary: {stats['total_active_pending']} pending, {stats['total_open_positions']} positions")
                
                # Magic number from investor ID
                magic_number = int(inv_id) if inv_id.isdigit() else 0
                
                # Process each category and symbol
                for category_name, category_data in signals_data.get('categories', {}).items():
                    symbols_in_category = category_data.get('symbols', {})
                    
                    for symbol, symbol_signals in symbols_in_category.items():
                        stats["symbols_processed"] += 1
                        
                        print(f"\n    🔍 Processing {symbol} for order placement:")
                        
                        # Initialize symbol details
                        symbol_detail = {
                            "symbol": symbol,
                            "category": category_name,
                            "orders_attempted": 0,
                            "orders_placed": 0,
                            "orders_converted": 0,
                            "orders_conversion_skipped_permission": 0,
                            "orders_skipped_pending": 0,
                            "orders_skipped_position": 0,
                            "orders_skipped_too_close_same": 0,
                            "orders_skipped_too_close_opposite": 0,
                            "orders_failed": 0,
                            "orders_cancelled_during_regulation": 0,
                            "main_orders_placed": 0,
                            "counter_orders_placed": 0,
                            "counter_orders_skipped": 0,
                            "counter_orders_failed": 0,
                            "errors": []
                        }
                        
                        # Check spread for this symbol
                        symbol_info = mt5.symbol_info(symbol)
                        if symbol_info:
                            current_spread = symbol_info.spread
                            print(f"      📊 Current spread for {symbol}: {current_spread} points")
                            
                            if current_spread > max_spread:
                                print(f"      ⚠️ Spread {current_spread} exceeds max allowed {max_spread} - may affect order placement")
                        
                        # Get current prices for reference
                        bid, ask = get_current_price(symbol)
                        if bid and ask:
                            print(f"      💵 Current prices - Bid: {bid:.2f}, Ask: {ask:.2f}")
                        
                        # COLLECT ALL ORDERS FROM BOTH ARRAYS - INCLUDING COUNTERS
                        all_orders_to_process = []
                        pending_orders_to_keep = set()  # Track orders that should be kept for regulation
                        
                        # Helper function to add an order to processing list
                        def add_order_to_process(order_obj, source, is_counter_val):
                            all_orders_to_process.append({
                                'order': order_obj,
                                'source': source,
                                'is_counter': is_counter_val
                            })
                            
                            # Generate key for regulation - but note: if conversion happens, we need to keep the converted order type
                            ord_type = order_obj.get('order_type', '')
                            ord_entry = order_obj.get('entry', 0)
                            ord_volume = round(order_obj.get('volume', 0.01), 2)
                            
                            # If conversion is allowed and this is a stop order, we need to keep the limit order instead
                            if allow_order_type_conversion and ord_type in ['buy_stop', 'sell_stop']:
                                # Convert the type for regulation key
                                converted_type = convert_order_type_logic(ord_type)
                                mt5_type = None
                                if converted_type == 'buy_stop':
                                    mt5_type = mt5.ORDER_TYPE_BUY_STOP
                                elif converted_type == 'buy_limit':
                                    mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                                elif converted_type == 'sell_stop':
                                    mt5_type = mt5.ORDER_TYPE_SELL_STOP
                                elif converted_type == 'sell_limit':
                                    mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                                
                                if mt5_type:
                                    pending_orders_to_keep.add(f"{symbol}_{mt5_type}_{ord_entry}_{ord_volume}")
                            else:
                                mt5_type = None
                                if ord_type == 'buy_stop':
                                    mt5_type = mt5.ORDER_TYPE_BUY_STOP
                                elif ord_type == 'buy_limit':
                                    mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
                                elif ord_type == 'sell_stop':
                                    mt5_type = mt5.ORDER_TYPE_SELL_STOP
                                elif ord_type == 'sell_limit':
                                    mt5_type = mt5.ORDER_TYPE_SELL_LIMIT
                                
                                if mt5_type:
                                    pending_orders_to_keep.add(f"{symbol}_{mt5_type}_{ord_entry}_{ord_volume}")
                        
                        # Process bid_orders
                        bid_orders = symbol_signals.get('bid_orders', [])
                        for order in bid_orders:
                            # Add main bid order
                            add_order_to_process(order, 'bid_orders', False)
                            
                            # Add counter order if present
                            if 'order_counter' in order and order['order_counter']:
                                add_order_to_process(order['order_counter'], 'bid_orders_counter', True)
                        
                        # Process ask_orders
                        ask_orders = symbol_signals.get('ask_orders', [])
                        for order in ask_orders:
                            # Add main ask order
                            add_order_to_process(order, 'ask_orders', False)
                            
                            # Add counter order if present
                            if 'order_counter' in order and order['order_counter']:
                                add_order_to_process(order['order_counter'], 'ask_orders_counter', True)
                        
                        print(f"      📦 Found {len(all_orders_to_process)} total orders to process (including counters)")
                        
                        # =====================================================
                        # STEP 3: Regulate and authorize orders
                        # =====================================================
                        print(f"      🧹 Regulating pending orders for {symbol}...")
                        cancelled_count = regulate_and_authorize_orders(symbol, pending_orders_to_keep, magic_number)
                        symbol_detail["orders_cancelled_during_regulation"] = cancelled_count
                        stats["orders_cancelled_during_regulation"] += cancelled_count
                        
                        # Process ALL orders (main and counter) in a single pass
                        for idx, order_info in enumerate(all_orders_to_process):
                            order = order_info['order']
                            is_counter = order_info['is_counter']
                            
                            stats["orders_attempted"] += 1
                            symbol_detail["orders_attempted"] += 1
                            
                            # Process the order with opposite restriction setting
                            success, result, error, was_skipped, skip_reason, was_converted, final_price, check_type = process_single_order(
                                order, symbol, magic_number, existing_orders, existing_positions, 
                                is_counter, allow_order_type_conversion, opposite_order_restriction
                            )
                            
                            if was_skipped:
                                if skip_reason == 'pending':
                                    stats["orders_skipped_duplicate_pending"] += 1
                                    symbol_detail["orders_skipped_pending"] += 1
                                elif skip_reason == 'position':
                                    stats["orders_skipped_duplicate_position"] += 1
                                    symbol_detail["orders_skipped_position"] += 1
                                elif skip_reason == 'too_close_same':
                                    stats["orders_skipped_too_close_same_direction"] += 1
                                    symbol_detail["orders_skipped_too_close_same"] += 1
                                elif skip_reason == 'too_close_opposite':
                                    stats["orders_skipped_too_close_opposite_direction"] += 1
                                    symbol_detail["orders_skipped_too_close_opposite"] += 1
                                
                                # Track counter order skips separately
                                if is_counter:
                                    stats["counter_orders_skipped"] += 1
                                    symbol_detail["counter_orders_skipped"] += 1
                                
                            elif success:
                                stats["orders_placed"] += 1
                                symbol_detail["orders_placed"] += 1
                                
                                if was_converted:
                                    stats["orders_converted_and_placed"] += 1
                                    symbol_detail["orders_converted"] += 1
                                
                                if is_counter:
                                    stats["counter_orders_placed"] += 1
                                    symbol_detail["counter_orders_placed"] += 1
                                else:
                                    stats["main_orders_placed"] += 1
                                    symbol_detail["main_orders_placed"] += 1
                                
                                # Map order type string to MT5 order type for the key
                                # Use the final order type that was actually placed
                                final_order_type = order.get('final_order_type', order.get('order_type', ''))
                                if was_converted:
                                    # If converted, use the converted type
                                    final_order_type = convert_order_type_logic(order.get('order_type', ''))
                                
                                mt5_pending_type = None
                                
                                if final_order_type == 'buy_stop':
                                    mt5_pending_type = mt5.ORDER_TYPE_BUY_STOP
                                elif final_order_type == 'buy_limit':
                                    mt5_pending_type = mt5.ORDER_TYPE_BUY_LIMIT
                                elif final_order_type == 'sell_stop':
                                    mt5_pending_type = mt5.ORDER_TYPE_SELL_STOP
                                elif final_order_type == 'sell_limit':
                                    mt5_pending_type = mt5.ORDER_TYPE_SELL_LIMIT
                                
                                # Round volume for key
                                volume_rounded = round(order.get('volume', 0.01), 2)
                                
                                # Add to existing_orders to prevent duplicate placement in same run
                                if mt5_pending_type:
                                    pending_key = f"{symbol}_{mt5_pending_type}_{final_price}_{volume_rounded}"
                                    existing_orders[pending_key] = {
                                        'ticket': result.order,
                                        'symbol': symbol,
                                        'type': mt5_pending_type,
                                        'price': final_price,
                                        'volume': volume_rounded,
                                        'type_string': "PENDING"
                                    }
                                    print(f"          🔒 Added to duplicate prevention cache: {pending_key}")
                                
                            else:
                                stats["orders_failed"] += 1
                                symbol_detail["orders_failed"] += 1
                                
                                if is_counter:
                                    stats["counter_orders_failed"] += 1
                                    symbol_detail["counter_orders_failed"] += 1
                                
                                # Track if failure was due to conversion not being allowed
                                if error and "conversion is NOT ALLOWED" in error:
                                    stats["orders_conversion_skipped_permission"] += 1
                                    symbol_detail["orders_conversion_skipped_permission"] += 1
                                
                                if error:
                                    symbol_detail["errors"].append(f"{'Counter' if is_counter else 'Main'} order {idx+1}: {error}")
                        
                        # Store symbol details
                        stats["symbol_details"][symbol] = symbol_detail
                
                # Print summary
                print(f"\n  📊 ORDER PLACEMENT SUMMARY:")
                print(f"    • Upload Orders Flag: {stats['upload_orders_flag']}")
                print(f"    • Investors skipped due to position flag: {stats['investors_skipped_due_to_position_flag']}")
                print(f"    • Symbols processed: {stats['symbols_processed']}")
                print(f"    • Total orders attempted: {stats['orders_attempted']}")
                print(f"    • Orders placed: {stats['orders_placed']}")
                print(f"      - Main orders: {stats['main_orders_placed']}")
                print(f"      - Counter orders: {stats['counter_orders_placed']}")
                print(f"      - Converted orders: {stats['orders_converted_and_placed']}")
                print(f"    • Counter orders skipped: {stats.get('counter_orders_skipped', 0)}")
                print(f"    • Counter orders failed: {stats['counter_orders_failed']}")
                print(f"    • Orders cancelled during regulation: {stats['orders_cancelled_during_regulation']}")
                print(f"    • Orders skipped (duplicate pending): {stats['orders_skipped_duplicate_pending']}")
                print(f"    • Orders skipped (duplicate position): {stats['orders_skipped_duplicate_position']}")
                print(f"    • Orders skipped (too close - same direction): {stats.get('orders_skipped_too_close_same_direction', 0)}")
                print(f"    • Orders skipped (too close - opposite direction): {stats.get('orders_skipped_too_close_opposite_direction', 0)}")
                print(f"    • Orders conversion skipped (permission denied): {stats['orders_conversion_skipped_permission']}")
                print(f"    • Orders failed: {stats['orders_failed']}")
                print(f"    • Total active pending orders: {stats['total_active_pending']}")
                print(f"    • Total open positions: {stats['total_open_positions']}")
                
                if stats['orders_failed'] > 0:
                    print(f"\n   FAILED ORDER DETAILS:")
                    for symbol, detail in stats["symbol_details"].items():
                        if detail["orders_failed"] > 0:
                            print(f"    • {symbol}: {detail['orders_failed']} failures")
                            for error in detail["errors"][:5]:
                                print(f"        • {error}")
                
            except Exception as e:
                print(f" [{inv_id}]  Error in order placement: {e}")
                import traceback
                traceback.print_exc()
                stats["placement_errors"].append(str(e))
        
        return stats
    
    return main()

def manage_single_position_and_pending(inv_id=None):
    """
    Function: Manages positions and pending orders to ensure only one position exists PER SYMBOL
    and only the opposite pending order with entry price matching the position's SL price remains.
    
    If enable_single_position_and_pending is True:
    - For each symbol with a position:
        - For a SELL position: Keeps only the BUY order with entry price EQUAL to the SELL position's SL price
        - For a BUY position: Keeps only the SELL order with entry price EQUAL to the BUY position's SL price
        - Deletes all other pending orders for that symbol
    
    - For each symbol WITHOUT a position but WITH pending orders:
        - First, identify the closest BUY order to current price and closest SELL order to current price
        - Then, check if these two orders have a valid SL relationship:
          * BUY entry price equals SELL SL price, OR
          * SELL entry price equals BUY SL price
        - If valid SL relationship exists: keep these two orders, delete all others
        - If no valid SL relationship: delete ALL pending orders
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the processing including 'upload_orders' flag
    """
    print(f"\n{'='*10} 🎯 SINGLE POSITION & PENDING ORDER MANAGEMENT (PER SYMBOL) {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "positions_found": 0,
        "pending_orders_found": 0,
        "symbols_processed": 0,
        "orders_kept": 0,
        "orders_deleted": 0,
        "errors": 0,
        "processing_success": False,
        "upload_orders": True,  # Will be set to False ONLY if ANY symbol has valid SL match
        "symbol_status": {}  # Track per symbol status for debugging
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking single position/pending configuration...")
        
        # Reset per-investor flags
        investor_has_valid_pair = False
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─  No broker config found")
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
            
            # Check if single position and pending management is enabled
            settings = config.get("settings", {})
            if not settings.get("enable_single_position_and_pending", False):
                print(f"  └─ ⏭️  Single position/pending management disabled in settings. Skipping.")
                continue
            
            print(f"  └─ ✅ Single position/pending management ENABLED (SL PRICE MATCHING MODE)")
            
        except Exception as e:
            print(f"  └─  Failed to read config: {e}")
            stats["errors"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Initialize MT5 connection if needed
        if not mt5.initialize(path=mt5_path):
            print(f"  └─  MT5 initialization failed")
            stats["errors"] += 1
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─  Login failed: {error}")
                stats["errors"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        # --- GET ALL POSITIONS AND PENDING ORDERS ---
        positions = mt5.positions_get()
        pending_orders = mt5.orders_get()
        
        stats["investors_processed"] += 1
        stats["positions_found"] += len(positions) if positions else 0
        stats["pending_orders_found"] += len(pending_orders) if pending_orders else 0
        
        # Count total trading items (positions + pending orders)
        total_positions = len(positions) if positions else 0
        total_pending = len(pending_orders) if pending_orders else 0
        total_items = total_positions + total_pending
        
        print(f"\n  └─ 📊 Account Summary:")
        print(f"      • Positions: {total_positions}")
        print(f"      • Pending Orders: {total_pending}")
        print(f"      • Total Items: {total_items}")
        
        # CRITICAL RULE: If there's only ONE item total (either one position OR one pending order)
        if total_items == 1:
            print(f"\n  └─ 🚩 Only 1 trading item found (total positions + pending orders = 1)")
            print(f"  └─ 🚩 upload_orders = True (insufficient items for management)")
            stats["upload_orders"] = True
            stats["processing_success"] = True
            continue
        
        # --- GROUP POSITIONS BY SYMBOL ---
        positions_by_symbol = {}
        if positions:
            for pos in positions:
                symbol = pos.symbol
                if symbol not in positions_by_symbol:
                    positions_by_symbol[symbol] = []
                positions_by_symbol[symbol].append(pos)
        
        # Group pending orders by symbol
        orders_by_symbol = {}
        if pending_orders:
            for order in pending_orders:
                symbol = order.symbol
                if symbol not in orders_by_symbol:
                    orders_by_symbol[symbol] = []
                orders_by_symbol[symbol].append(order)
        
        print(f"\n  └─ 📊 Symbols with positions: {list(positions_by_symbol.keys())}")
        print(f"  └─ 📊 Symbols with pending orders: {list(orders_by_symbol.keys())}")
        
        # --- GET CURRENT PRICES FOR ALL SYMBOLS ---
        current_prices = {}
        all_symbols = set(positions_by_symbol.keys()) | set(orders_by_symbol.keys())
        for symbol in all_symbols:
            tick = mt5.symbol_info_tick(symbol)
            if tick:
                current_prices[symbol] = tick.ask  # Use ask for calculations
            else:
                print(f"      ⚠️  Could not get current price for {symbol}")
                current_prices[symbol] = None
        
        # --- PROCESS EACH SYMBOL INDEPENDENTLY ---
        symbol_results = []
        
        # Process ALL symbols that have pending orders (with or without positions)
        all_symbols_with_orders = set(orders_by_symbol.keys())
        
        for symbol in all_symbols_with_orders:
            print(f"\n  └─ 🔄 Processing symbol: {symbol}")
            
            has_position = symbol in positions_by_symbol
            symbol_orders = orders_by_symbol.get(symbol, [])
            current_price = current_prices.get(symbol)
            
            symbol_stats = {
                "symbol": symbol,
                "has_position": has_position,
                "positions_count": len(positions_by_symbol.get(symbol, [])),
                "pending_orders_count": len(symbol_orders),
                "has_valid_pair": False,
                "orders_kept": 0,
                "orders_deleted": 0,
                "action_taken": False,
                "management_type": "WITH_POSITION" if has_position else "NO_POSITION"
            }
            
            if has_position:
                # --- CASE 1: Symbol HAS a position ---
                print(f"      📍 Symbol has a position - checking for opposite order with matching SL price")
                
                symbol_positions = positions_by_symbol[symbol]
                
                # Check if multiple positions exist for this symbol
                if len(symbol_positions) > 1:
                    print(f"      ⚠️  WARNING: Found {len(symbol_positions)} open positions for {symbol}!")
                    print(f"      This function expects a single position per symbol. Processing based on the first position only.")
                
                # Get the first position for this symbol
                position = symbol_positions[0]
                
                # Determine position type
                is_buy_position = position.type == mt5.POSITION_TYPE_BUY
                position_type_str = "BUY" if is_buy_position else "SELL"
                
                print(f"      • Position Ticket: {position.ticket}")
                print(f"      • Type: {position_type_str}")
                print(f"      • Volume: {position.volume}")
                print(f"      • Entry Price: {position.price_open}")
                print(f"      • SL Price: {position.sl}")
                print(f"      • Current Price: {position.price_current}")
                print(f"      • Found {len(symbol_orders)} pending orders for {symbol}")
                
                # Check if position has SL
                if position.sl is None or position.sl == 0:
                    print(f"      ⚠️  Position has no SL price set. Deleting ALL pending orders.")
                    # Delete all pending orders for this symbol
                    orders_to_delete = symbol_orders.copy()
                    symbol_stats["has_valid_pair"] = False
                else:
                    # Define opposite order type based on position direction
                    opposite_order_types = []
                    target_sl_price = position.sl
                    
                    if is_buy_position:
                        # For BUY position, look for SELL orders with entry price EQUAL to BUY SL
                        opposite_order_types = [mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP]
                        print(f"      • Looking for SELL order with entry price = BUY SL ({target_sl_price})")
                    else:
                        # For SELL position, look for BUY orders with entry price EQUAL to SELL SL
                        opposite_order_types = [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP]
                        print(f"      • Looking for BUY order with entry price = SELL SL ({target_sl_price})")
                    
                    # Find opposite orders with entry price exactly matching SL
                    matching_orders = []
                    other_orders = []
                    
                    for order in symbol_orders:
                        if order.type in opposite_order_types:
                            if abs(order.price_open - target_sl_price) < 0.00001:  # Float comparison with tolerance
                                matching_orders.append(order)
                                print(f"      ✅ Found matching order #{order.ticket} with entry price {order.price_open} = SL {target_sl_price}")
                            else:
                                other_orders.append(order)
                                print(f"       Order #{order.ticket} entry price {order.price_open} does NOT match SL {target_sl_price}")
                        else:
                            # Orders in same direction or other types
                            other_orders.append(order)
                    
                    # Keep only the first matching order (if any), delete all others
                    if matching_orders:
                        # Keep the first matching order
                        order_to_keep = matching_orders[0]
                        orders_to_keep = [order_to_keep]
                        orders_to_delete = matching_orders[1:] + other_orders
                        
                        order_type_name = "SELL" if is_buy_position else "BUY"
                        print(f"      🎯 Keeping opposite {order_type_name} order #{order_to_keep.ticket} with entry price matching SL")
                        symbol_stats["has_valid_pair"] = True
                        investor_has_valid_pair = True
                        symbol_stats["orders_kept"] = len(orders_to_keep)
                        stats["orders_kept"] += len(orders_to_keep)
                    else:
                        # No matching order found
                        orders_to_delete = symbol_orders.copy()
                        print(f"       No opposite order found with entry price matching SL ({target_sl_price})")
                        symbol_stats["has_valid_pair"] = False
                
                # Delete all non-matching orders
                if orders_to_delete:
                    print(f"      🗑️  Deleting {len(orders_to_delete)} order(s)...")
                    symbol_stats["action_taken"] = True
                    
                    for order in orders_to_delete:
                        order_type_name = "UNKNOWN"
                        if order.type == mt5.ORDER_TYPE_BUY_LIMIT:
                            order_type_name = "BUY LIMIT"
                        elif order.type == mt5.ORDER_TYPE_SELL_LIMIT:
                            order_type_name = "SELL LIMIT"
                        elif order.type == mt5.ORDER_TYPE_BUY_STOP:
                            order_type_name = "BUY STOP"
                        elif order.type == mt5.ORDER_TYPE_SELL_STOP:
                            order_type_name = "SELL STOP"
                        
                        print(f"        • Deleting order #{order.ticket} ({order_type_name} @ {order.price_open})...")
                        
                        delete_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket,
                        }
                        
                        result = mt5.order_send(delete_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            symbol_stats["orders_deleted"] += 1
                            stats["orders_deleted"] += 1
                            print(f"          ✅ Deleted successfully")
                        else:
                            stats["errors"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"           Deletion failed: {error_msg}")
            
            else:
                # --- CASE 2: Symbol has NO position, only pending orders ---
                print(f"      📍 Symbol has NO position - finding closest BUY/SELL orders to current price")
                
                if not current_price:
                    print(f"      ⚠️  Cannot get current price for {symbol}. Deleting all pending orders.")
                    orders_to_delete = symbol_orders.copy()
                    symbol_stats["has_valid_pair"] = False
                    symbol_stats["orders_deleted"] = len(orders_to_delete)
                    stats["orders_deleted"] += len(orders_to_delete)
                    symbol_stats["action_taken"] = True if orders_to_delete else False
                    
                    # Delete all orders
                    for order in orders_to_delete:
                        delete_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket,
                        }
                        result = mt5.order_send(delete_request)
                        if result and result.retcode != mt5.TRADE_RETCODE_DONE:
                            stats["errors"] += 1
                    
                    symbol_results.append(symbol_stats)
                    stats["symbols_processed"] += 1
                    continue
                
                print(f"      • Current Price: {current_price}")
                print(f"      • Found {len(symbol_orders)} pending orders for {symbol}")
                
                # Separate BUY and SELL orders
                buy_orders = []
                sell_orders = []
                
                for order in symbol_orders:
                    if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP]:
                        buy_orders.append(order)
                    elif order.type in [mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP]:
                        sell_orders.append(order)
                
                print(f"      • Found {len(buy_orders)} BUY orders")
                print(f"      • Found {len(sell_orders)} SELL orders")
                
                # STEP 1: Find closest BUY order to current price
                closest_buy = None
                if buy_orders:
                    closest_buy = min(buy_orders, key=lambda o: abs(o.price_open - current_price))
                    buy_distance = abs(closest_buy.price_open - current_price)
                    print(f"      🎯 Closest BUY order to CURRENT PRICE ({current_price}):")
                    print(f"      • Ticket: {closest_buy.ticket}")
                    print(f"      • Type: {'BUY LIMIT' if closest_buy.type == mt5.ORDER_TYPE_BUY_LIMIT else 'BUY STOP'}")
                    print(f"      • Entry: {closest_buy.price_open}")
                    print(f"      • SL: {closest_buy.sl}")
                    print(f"      • Distance: {buy_distance:.5f}")
                else:
                    print(f"      ℹ️  No BUY orders found")
                
                # STEP 2: Find closest SELL order to current price
                closest_sell = None
                if sell_orders:
                    closest_sell = min(sell_orders, key=lambda o: abs(o.price_open - current_price))
                    sell_distance = abs(closest_sell.price_open - current_price)
                    print(f"      🎯 Closest SELL order to CURRENT PRICE ({current_price}):")
                    print(f"      • Ticket: {closest_sell.ticket}")
                    print(f"      • Type: {'SELL LIMIT' if closest_sell.type == mt5.ORDER_TYPE_SELL_LIMIT else 'SELL STOP'}")
                    print(f"      • Entry: {closest_sell.price_open}")
                    print(f"      • SL: {closest_sell.sl}")
                    print(f"      • Distance: {sell_distance:.5f}")
                else:
                    print(f"      ℹ️  No SELL orders found")
                
                # STEP 3: Check if we have both a BUY and SELL order
                if closest_buy and closest_sell:
                    # Check for valid SL relationship
                    valid_sl_match = False
                    
                    # Check if BUY entry equals SELL SL
                    if closest_buy.sl is not None and abs(closest_buy.sl - closest_sell.price_open) < 0.00001:
                        valid_sl_match = True
                        print(f"      ✅ Valid SL match: BUY SL ({closest_buy.sl}) = SELL entry ({closest_sell.price_open})")
                    
                    # Check if SELL entry equals BUY SL
                    elif closest_sell.sl is not None and abs(closest_sell.sl - closest_buy.price_open) < 0.00001:
                        valid_sl_match = True
                        print(f"      ✅ Valid SL match: SELL SL ({closest_sell.sl}) = BUY entry ({closest_buy.price_open})")
                    
                    if valid_sl_match:
                        # Keep both closest orders, delete all others
                        orders_to_keep = [closest_buy, closest_sell]
                        orders_to_delete = [o for o in symbol_orders if o.ticket not in [closest_buy.ticket, closest_sell.ticket]]
                        
                        print(f"      🎯 Keeping valid BUY/SELL pair (closest to current price with SL match)")
                        symbol_stats["has_valid_pair"] = True
                        investor_has_valid_pair = True
                        symbol_stats["orders_kept"] = len(orders_to_keep)
                        stats["orders_kept"] += len(orders_to_keep)
                    else:
                        # No valid SL relationship - delete ALL orders
                        orders_to_delete = symbol_orders.copy()
                        orders_to_keep = []
                        print(f"       Closest orders do NOT have valid SL relationship - deleting ALL orders")
                        symbol_stats["has_valid_pair"] = False
                else:
                    # Missing either BUY or SELL order - delete ALL orders
                    orders_to_delete = symbol_orders.copy()
                    orders_to_keep = []
                    print(f"       Missing either BUY or SELL order - deleting ALL orders")
                    symbol_stats["has_valid_pair"] = False
                
                # Delete orders
                if orders_to_delete:
                    print(f"      🗑️  Deleting {len(orders_to_delete)} order(s)...")
                    symbol_stats["action_taken"] = True
                    
                    for order in orders_to_delete:
                        order_type_name = "UNKNOWN"
                        if order.type == mt5.ORDER_TYPE_BUY_LIMIT:
                            order_type_name = "BUY LIMIT"
                        elif order.type == mt5.ORDER_TYPE_SELL_LIMIT:
                            order_type_name = "SELL LIMIT"
                        elif order.type == mt5.ORDER_TYPE_BUY_STOP:
                            order_type_name = "BUY STOP"
                        elif order.type == mt5.ORDER_TYPE_SELL_STOP:
                            order_type_name = "SELL STOP"
                        
                        print(f"        • Deleting order #{order.ticket} ({order_type_name} @ {order.price_open})...")
                        
                        delete_request = {
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket,
                        }
                        
                        result = mt5.order_send(delete_request)
                        
                        if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                            symbol_stats["orders_deleted"] += 1
                            stats["orders_deleted"] += 1
                            print(f"          ✅ Deleted successfully")
                        else:
                            stats["errors"] += 1
                            error_msg = result.comment if result else f"Error code: {result.retcode if result else 'Unknown'}"
                            print(f"           Deletion failed: {error_msg}")
            
            symbol_results.append(symbol_stats)
            stats["symbols_processed"] += 1
            
            # Display per-symbol summary
            print(f"      📊 {symbol} Summary:")
            print(f"      • Management Type: {symbol_stats['management_type']}")
            print(f"      • Has valid SL match: {'✅ YES' if symbol_stats['has_valid_pair'] else ' NO'}")
            print(f"      • Orders kept: {symbol_stats['orders_kept']}")
            print(f"      • Orders deleted: {symbol_stats['orders_deleted']}")
            
            # Store status
            if has_position:
                stats["symbol_status"][symbol] = "VALID_SL_MATCH" if symbol_stats["has_valid_pair"] else "NO_SL_MATCH"
            else:
                if symbol_stats["has_valid_pair"]:
                    stats["symbol_status"][symbol] = "VALID_PAIR_CLOSEST_WITH_SL_MATCH"
                else:
                    stats["symbol_status"][symbol] = "NO_VALID_PAIR_ALL_DELETED"
        
        # --- DETERMINE FINAL upload_orders FLAG ---
        # CRITICAL: upload_orders should be:
        # - True if NO symbol has a valid SL match
        # - False if AT LEAST ONE symbol has a valid SL match
        
        if investor_has_valid_pair:
            stats["upload_orders"] = False
            print(f"\n  └─ 🚩 upload_orders = False (At least one symbol has valid SL match)")
        else:
            stats["upload_orders"] = True
            print(f"\n  └─ 🚩 upload_orders = True (No symbol has valid SL match)")
        
        stats["processing_success"] = True
        
        # --- INVESTOR SUMMARY ---
        print(f"\n  └─ 📊 Management Results for {user_brokerid}:")
        print(f"      • Symbols processed: {len(symbol_results)}")
        print(f"      • Total positions: {total_positions}")
        print(f"      • Total pending orders: {total_pending}")
        print(f"      • Orders kept: {stats['orders_kept']}")
        print(f"      • Orders deleted: {stats['orders_deleted']}")
        print(f"      • upload_orders flag: {stats['upload_orders']}")
        
        # Show per-symbol breakdown
        if symbol_results:
            print(f"      • Per-symbol breakdown:")
            for sym_result in symbol_results:
                if sym_result['has_position']:
                    status = "✅ VALID SL MATCH" if sym_result['has_valid_pair'] else " NO SL MATCH"
                else:
                    if sym_result['has_valid_pair']:
                        status = "✅ VALID PAIR (closest orders with SL match)"
                    else:
                        status = " NO VALID PAIR (all orders deleted)"
                print(f"        - {sym_result['symbol']}: {status} (kept: {sym_result['orders_kept']}, deleted: {sym_result['orders_deleted']})")
        
        if stats['errors'] > 0:
            print(f"      • Errors: {stats['errors']}")
        else:
            print(f"      ✅ Management completed successfully")

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 SINGLE POSITION & PENDING ORDER MANAGEMENT SUMMARY (PER SYMBOL) {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Investors processed: {stats['investors_processed']}")
    print(f"   Total positions found: {stats['positions_found']}")
    print(f"   Total pending orders found: {stats['pending_orders_found']}")
    print(f"   Symbols processed: {stats['symbols_processed']}")
    print(f"   Orders kept: {stats['orders_kept']}")
    print(f"   Orders deleted: {stats['orders_deleted']}")
    print(f"   Errors: {stats['errors']}")
    print(f"   FINAL upload_orders flag: {stats['upload_orders']}")
    
    # Show symbol status summary
    if stats['symbol_status']:
        print(f"\n   Symbol Status Summary:")
        for symbol, status in stats['symbol_status'].items():
            print(f"      • {symbol}: {status}")
    
    if stats['orders_deleted'] > 0 or stats['orders_kept'] > 0:
        print(f"\n   Management Action: {'✅ COMPLETED' if stats['processing_success'] else '⚠️  PARTIAL'}")
    else:
        print(f"\n   Management Action: ℹ️  No action needed")
    
    print(f"\n{'='*10} 🏁 SINGLE POSITION & PENDING ORDER MANAGEMENT COMPLETE {'='*10}\n")
    return stats

def martingale_old(inv_id=None):

    """
    Function: Checks daily loss and martingale status for the day.
    
    Calculates:
    - Starting balance for the day (balance at market open or start of day)
    - Current balance
    - Total loss incurred today
    - Martingale lookback analysis based on maximum_martingale_lookback setting
    - Modifies signals.json volumes based on safe volume that respects martingale_maximum_risk
    - Validates risk for one order using EXACT same method as check_pending_orders_risk()
    - Symbol-specific martingale tracking (each symbol tracked separately)
    - Loss recovery with percentage adder based on martingale_loss_recovery_adder_percentage
    - Optionally syncs volume changes to existing pending orders if martingale_for_position_order_scale is enabled
    - PRE-SCALING: Modifies signals.json volumes to account for expected losses from positions with SL
    - SAFETY: Cancels any pending orders in MT5 that don't match the volumes in signals.json
    - DAYS LOOKBACK: Limits search to specified number of days (1 = today only, 2 = today and yesterday, etc.)
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the daily loss and martingale status
    """
    print(f"\n{'='*10} 🎰 MARTINGALE STATUS CHECK {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "martingale_enabled": False,
        "maximum_martingale_lookback": 0,
        "martingale_days_lookback": 0,
        "martingale_maximum_risk": 0,
        "martingale_loss_recovery_adder_percentage": 0,
        "martingale_for_position_order_scale": False,
        "martingale_pre_scaling": False,
        "has_loss": False,
        "daily_loss": 0.0,
        "starting_balance": 0.0,
        "current_balance": 0.0,
        "loss_percentage": 0.0,
        "errors": 0,
        "processing_success": False,
        "symbols_with_loss": [],
        "symbol_analysis": {},
        "signals_modified": False,
        "pending_orders_modified": False,
        "risk_check_passed": False,
        "risk_exceeded": False,
        "order_risk_validation": {},
        "pending_order_sync_results": {},
        "pre_scaling_applied": False,
        "pre_scaling_details": {},
        "safety_cancellations": {},
        "safety_cancellations_count": 0
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking martingale status...")
        
        # Reset per-investor variables
        pre_scaling_details = {}
        safety_cancellations = {}
        safety_cancellations_count = 0
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─  No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK MARTINGALE SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check both old and new config structure
            settings = config.get("settings", {})
            martingale_config = settings.get("martingale_config", {})
            
            # If martingale_config exists, use it, otherwise fall back to old structure
            if martingale_config:
                martingale_enabled = martingale_config.get("enable_martingale", False)
                maximum_lookback = martingale_config.get("maximum_martingale_lookback", 3)
                days_lookback = martingale_config.get("martingale_days_lookback", 1)
                recovery_adder_str = martingale_config.get("martingale_loss_recovery_adder_percentage", "0%")
                martingale_for_position_order_scale = martingale_config.get("martingale_for_position_order_scale", False)
                martingale_pre_scaling = martingale_config.get("martingale_pre_scaling", False)
            else:
                # Fall back to old structure
                martingale_enabled = settings.get("enable_martingale", False)
                maximum_lookback = settings.get("maximum_martingale_lookback", 3)
                days_lookback = settings.get("martingale_days_lookback", 1)
                recovery_adder_str = settings.get("martingale_loss_recovery_adder_percentage", "0%")
                martingale_for_position_order_scale = settings.get("martingale_for_position_order_scale", False)
                martingale_pre_scaling = settings.get("martingale_pre_scaling", False)
            
            # Ensure days_lookback is at least 1
            try:
                days_lookback = int(days_lookback)
                if days_lookback < 1:
                    days_lookback = 1
            except (ValueError, TypeError):
                days_lookback = 1
            
            # Parse percentage (remove % sign and convert to float)
            recovery_adder_percentage = 0
            if recovery_adder_str:
                try:
                    recovery_adder_percentage = float(recovery_adder_str.replace('%', ''))
                except:
                    recovery_adder_percentage = 0
            
            stats["martingale_enabled"] = martingale_enabled
            stats["maximum_martingale_lookback"] = maximum_lookback
            stats["martingale_days_lookback"] = days_lookback
            stats["martingale_loss_recovery_adder_percentage"] = recovery_adder_percentage
            stats["martingale_for_position_order_scale"] = martingale_for_position_order_scale
            stats["martingale_pre_scaling"] = martingale_pre_scaling
            
            if not martingale_enabled:
                print(f"  └─ ⏭️  Martingale DISABLED in settings. Skipping.")
                stats["processing_success"] = True
                continue
            
            print(f"  └─ ✅ Martingale ENABLED")
            print(f"  └─ 🔢 Maximum Martingale Lookback: {maximum_lookback} losses")
            print(f"  └─ 📅 Days Lookback: {days_lookback} day(s)")
            print(f"  └─ 📈 Loss Recovery Adder: {recovery_adder_percentage}%")
            print(f"  └─ 🔄 Sync to Pending Orders: {'✅ ENABLED' if martingale_for_position_order_scale else ' DISABLED'}")
            print(f"  └─ 🎯 PRE-SCALING: {'✅ ENABLED' if martingale_pre_scaling else ' DISABLED'}")
            print(f"  └─ 🛡️ SAFETY: Will cancel MT5 orders that don't match signals.json")
            
        except Exception as e:
            print(f"  └─  Failed to read config: {e}")
            stats["errors"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Initialize MT5 connection if needed
        if not mt5.initialize(path=mt5_path):
            print(f"  └─  MT5 initialization failed")
            stats["errors"] += 1
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─  Login failed: {error}")
                stats["errors"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        # --- GET CURRENT BALANCE ---
        account_info = mt5.account_info()
        if not account_info:
            print(f"  └─  Failed to get account information")
            stats["errors"] += 1
            continue
        
        current_balance = account_info.balance
        stats["current_balance"] = current_balance
        
        print(f"      • Current Balance: ${current_balance:.2f}")

        # --- GET MARTINGALE RISK BASED ON CURRENT BALANCE ---
        martingale_risk_map = config.get("martingale_risk_management", {})
        martingale_max_risk = None
        
        if martingale_risk_map:
            print(f"  └─ 📊 Determining Martingale Risk based on balance ${current_balance:.2f}...")
            
            for range_str, risk_value in martingale_risk_map.items():
                try:
                    raw_range = range_str.split("_")[0]
                    low_str, high_str = raw_range.split("-")
                    low = float(low_str)
                    high = float(high_str)
                    
                    if low <= current_balance <= high:
                        martingale_max_risk = float(risk_value)
                        print(f"      • Found matching range: {range_str}")
                        print(f"      • Risk limit: ${martingale_max_risk:.2f}")
                        break
                except Exception as e:
                    print(f"      ⚠️  Error parsing range '{range_str}': {e}")
                    continue
            
            if martingale_max_risk is None:
                print(f"      ⚠️  No risk mapping found for balance ${current_balance:.2f}")
                print(f"      Using default risk: $500")
                martingale_max_risk = 500
        else:
            print(f"      ⚠️  No martingale_risk_management section found in config")
            print(f"      Using default risk: $500")
            martingale_max_risk = 500
        
        stats["martingale_maximum_risk"] = martingale_max_risk
        print(f"  └─ 💰 Martingale Maximum Risk: ${martingale_max_risk:.2f}")

        # --- GET STARTING BALANCE FOR TODAY ---
        daily_stats_path = inv_root / "daily_stats.json"
        starting_balance = None
        
        today_date = datetime.now().date()
        today_str = today_date.strftime("%Y-%m-%d")
        
        print(f"      • Today's Date: {today_str}")
        
        if daily_stats_path.exists():
            try:
                with open(daily_stats_path, 'r', encoding='utf-8') as f:
                    daily_stats = json.load(f)
                
                if today_str in daily_stats:
                    starting_balance = daily_stats[today_str].get("starting_balance")
                    print(f"      • Found recorded starting balance: ${starting_balance:.2f}")
                else:
                    print(f"      • No recorded starting balance for today")
            except Exception as e:
                print(f"      ⚠️  Could not read daily stats: {e}")
        else:
            print(f"      • No daily stats file found")
        
        if starting_balance is None:
            print(f"      🔍 Calculating starting balance from trade history...")
            
            from_date = datetime(today_date.year, today_date.month, today_date.day, 0, 0, 0)
            to_date = datetime(today_date.year, today_date.month, today_date.day, 23, 59, 59)
            deals = mt5.history_deals_get(from_date, to_date)
            
            if deals is None:
                print(f"      ⚠️  Could not retrieve deal history")
                starting_balance = current_balance
                print(f"      • Using current balance as starting balance (no trades today)")
            else:
                today_profit = 0.0
                today_commission = 0.0
                today_swap = 0.0
                
                for deal in deals:
                    if deal.profit != 0 or deal.commission != 0 or deal.swap != 0:
                        today_profit += deal.profit
                        today_commission += deal.commission
                        today_swap += deal.swap
                
                total_pl = today_profit + today_commission + today_swap
                starting_balance = current_balance - total_pl
                
                print(f"      • Calculated starting balance: ${starting_balance:.2f}")
                print(f"      • Total profit/loss today: ${total_pl:.2f}")
            
            try:
                if daily_stats_path.exists():
                    with open(daily_stats_path, 'r', encoding='utf-8') as f:
                        daily_stats = json.load(f)
                else:
                    daily_stats = {}
                
                if today_str not in daily_stats:
                    daily_stats[today_str] = {}
                
                daily_stats[today_str]["starting_balance"] = starting_balance
                daily_stats[today_str]["last_updated"] = datetime.now().isoformat()
                
                with open(daily_stats_path, 'w', encoding='utf-8') as f:
                    json.dump(daily_stats, f, indent=2, ensure_ascii=False)
                
                print(f"      • Saved starting balance to daily_stats.json")
            except Exception as e:
                print(f"      ⚠️  Could not save daily stats: {e}")
        
        stats["starting_balance"] = starting_balance
        
        # --- CALCULATE DAILY LOSS ---
        daily_loss = starting_balance - current_balance
        
        if daily_loss > 0:
            loss_percentage = (daily_loss / starting_balance) * 100
            
            stats["has_loss"] = True
            stats["daily_loss"] = daily_loss
            stats["loss_percentage"] = loss_percentage
            
            print(f"\n  └─ 📉 DAILY LOSS DETECTED!")
            print(f"      • Starting Balance: ${starting_balance:.2f}")
            print(f"      • Current Balance: ${current_balance:.2f}")
            print(f"      • Total Loss Today: ${daily_loss:.2f}")
            print(f"      • Loss Percentage: {loss_percentage:.2f}%")
        else:
            print(f"\n  └─ ✅ NO DAILY LOSS")
            print(f"      • Starting Balance: ${starting_balance:.2f}")
            print(f"      • Current Balance: ${current_balance:.2f}")
            print(f"      • Change: ${current_balance - starting_balance:.2f} ({(current_balance - starting_balance) / starting_balance * 100:.2f}%)")
        
        # --- SYMBOL-SPECIFIC MARTINGALE LOOKBACK ANALYSIS WITH DAYS LOOKBACK ---
        print(f"\n  └─ 🔍 Performing Symbol-Specific Martingale Lookback Analysis...")
        print(f"      📅 Days Lookback: {days_lookback} day(s)")
        print(f"      🔢 Maximum Losses to Find: {maximum_lookback}")
        
        # Calculate date range based on days_lookback
        lookback_days = []
        for i in range(days_lookback):
            check_date = today_date - timedelta(days=i)
            lookback_days.append(check_date)
        
        print(f"      📆 Checking dates: {', '.join([d.strftime('%Y-%m-%d') for d in lookback_days])}")
        
        # Dictionary to store per-symbol analysis
        symbol_analysis = {}
        
        # Collect all deals within the lookback period
        all_deals_by_day = {}
        total_found_losses = {symbol: 0 for symbol in set()}
        
        try:
            # Fetch deals for each day in the lookback period
            for check_date in lookback_days:
                from_date = datetime(check_date.year, check_date.month, check_date.day, 0, 0, 0)
                to_date = datetime(check_date.year, check_date.month, check_date.day, 23, 59, 59)
                
                day_deals = mt5.history_deals_get(from_date, to_date)
                if day_deals and len(day_deals) > 0:
                    all_deals_by_day[check_date] = day_deals
                    print(f"      • {check_date.strftime('%Y-%m-%d')}: Found {len(day_deals)} deals")
                else:
                    print(f"      • {check_date.strftime('%Y-%m-%d')}: No deals found")
            
            # Process each symbol separately with cumulative loss collection across days
            # First, collect all deals and organize by symbol in chronological order across days
            symbol_deals_by_date = {}
            
            for check_date in sorted(all_deals_by_day.keys()):
                for deal in all_deals_by_day[check_date]:
                    symbol = deal.symbol
                    if symbol not in symbol_deals_by_date:
                        symbol_deals_by_date[symbol] = []
                    symbol_deals_by_date[symbol].append({
                        "deal": deal,
                        "date": check_date
                    })
            
            # Process each symbol
            for symbol, deals_with_dates in symbol_deals_by_date.items():
                print(f"\n      {'='*50}")
                print(f"      📊 Analyzing symbol: {symbol}")
                print(f"      {'='*50}")
                
                # Sort deals chronologically
                deals_with_dates.sort(key=lambda x: x["deal"].time)
                
                # Build P/L sequence for this symbol
                pl_sequence = []
                for item in deals_with_dates:
                    deal = item["deal"]
                    total_pl = deal.profit + deal.commission + deal.swap
                    if total_pl != 0:
                        pl_sequence.append({
                            "ticket": deal.ticket,
                            "symbol": deal.symbol,
                            "time": deal.time,
                            "date": item["date"],
                            "profit_loss": total_pl,
                            "type": "PROFIT" if total_pl > 0 else "LOSS",
                            "volume": deal.volume
                        })
                
                print(f"      • Total deals for {symbol}: {len(deals_with_dates)}")
                print(f"      • Non-zero P/L entries: {len(pl_sequence)}")
                
                if len(pl_sequence) == 0:
                    print(f"      ℹ️  No profit/loss entries found for {symbol}")
                    continue
                
                # Display sequence
                print(f"      📊 Profit/Loss Sequence (chronological):")
                display_count = min(10, len(pl_sequence))
                for idx, entry in enumerate(pl_sequence[:display_count], 1):
                    pl_sign = "📈" if entry["profit_loss"] > 0 else "📉"
                    print(f"        {idx}. {pl_sign} {entry['symbol']}: ${entry['profit_loss']:.2f} ({entry['type']}) - Volume: {entry['volume']} lots - Date: {entry['date'].strftime('%Y-%m-%d')}")
                
                latest_entry = pl_sequence[-1]
                latest_is_profit = latest_entry["profit_loss"] > 0
                
                print(f"\n      🎯 Latest Entry Analysis:")
                print(f"        • Latest is: {'PROFIT' if latest_is_profit else 'LOSS'}")
                print(f"        • Amount: ${abs(latest_entry['profit_loss']):.2f}")
                print(f"        • Volume: {latest_entry['volume']} lots")
                print(f"        • Date: {latest_entry['date'].strftime('%Y-%m-%d')}")
                
                # Collect losses within lookback period (scanning backwards from latest)
                # Stop when we have found maximum_lookback losses OR when we've processed all days
                print(f"\n      🔍 Analyzing losses within lookback period for {symbol}...")
                
                losses_found = []
                cumulative_profit = 0.0
                earliest_date_seen = None
                
                # Scan backwards from the latest entry to collect losses
                for i in range(len(pl_sequence) - 1, -1, -1):
                    entry = pl_sequence[i]
                    
                    # Track the earliest date we've processed
                    if earliest_date_seen is None or entry["date"] < earliest_date_seen:
                        earliest_date_seen = entry["date"]
                    
                    if entry["profit_loss"] < 0:
                        losses_found.append(entry)
                        print(f"        • Found loss: {entry['symbol']}: ${abs(entry['profit_loss']):.2f} (Date: {entry['date'].strftime('%Y-%m-%d')})")
                    else:
                        cumulative_profit += entry["profit_loss"]
                        print(f"        • Found profit: {entry['symbol']}: ${entry['profit_loss']:.2f} (Cumulative profit: ${cumulative_profit:.2f})")
                    
                    # Check if we've found enough losses
                    if len(losses_found) >= maximum_lookback:
                        print(f"        • Reached lookback limit of {maximum_lookback} losses")
                        break
                
                # Reverse to get chronological order
                lookback_losses = list(reversed(losses_found))
                
                # Initialize symbol analysis
                symbol_analysis[symbol] = {
                    "symbol": symbol,
                    "has_losses": len(lookback_losses) > 0,
                    "lookback_losses": [],
                    "total_loss_amount": 0.0,
                    "total_loss_volume": 0.0,
                    "required_profit": 0.0,
                    "required_profit_with_adder": 0.0,
                    "profits_after_losses_total": 0.0,
                    "latest_is_profit": latest_is_profit,
                    "latest_amount": abs(latest_entry["profit_loss"]),
                    "latest_volume": latest_entry["volume"],
                    "losses_in_lookback": len(lookback_losses),
                    "lookback_days_used": days_lookback,
                    "earliest_loss_date": earliest_date_seen.strftime('%Y-%m-%d') if earliest_date_seen else None
                }
                
                if lookback_losses:
                    # Calculate total loss amount for this symbol
                    total_loss_amount = sum(abs(loss["profit_loss"]) for loss in lookback_losses)
                    total_loss_volume = sum(loss["volume"] for loss in lookback_losses)
                    
                    print(f"\n      📊 Loss Analysis for {symbol}:")
                    print(f"        • Total losses found: {len(lookback_losses)}")
                    print(f"        • TOTAL LOSS AMOUNT: ${total_loss_amount:.2f}")
                    print(f"        • Total loss volume: {total_loss_volume} lots")
                    
                    # Find earliest loss index to identify profits after losses
                    earliest_loss_index = None
                    for i, entry in enumerate(pl_sequence):
                        if entry["ticket"] == lookback_losses[0]["ticket"]:
                            earliest_loss_index = i
                            break
                    
                    # Collect profits that occurred AFTER the earliest loss
                    profits_after_losses = []
                    if earliest_loss_index is not None:
                        for i in range(earliest_loss_index + 1, len(pl_sequence)):
                            if pl_sequence[i]["profit_loss"] > 0:
                                profits_after_losses.append(pl_sequence[i])
                    
                    total_profit_after = sum(p["profit_loss"] for p in profits_after_losses)
                    
                    print(f"\n      💰 Profit Analysis for {symbol}:")
                    print(f"        • Profits after the losses: {len(profits_after_losses)}")
                    print(f"        • Total profit after losses: ${total_profit_after:.2f}")
                    
                    # Calculate required profit (uncovered portion)
                    if total_profit_after >= total_loss_amount:
                        print(f"        ✅ Profits after losses cover all losses")
                        required_profit = 0
                    else:
                        required_profit = total_loss_amount - total_profit_after
                        print(f"         Profits after losses do NOT cover all losses")
                        print(f"        • Uncovered loss amount: ${required_profit:.2f}")
                    
                    # Apply percentage adder to required profit
                    adder_amount = required_profit * (recovery_adder_percentage / 100)
                    required_profit_with_adder = required_profit + adder_amount
                    
                    if recovery_adder_percentage > 0 and required_profit > 0:
                        print(f"\n      📈 Loss Recovery Adder ({recovery_adder_percentage}%):")
                        print(f"        • Base required profit: ${required_profit:.2f}")
                        print(f"        • Adder amount: ${adder_amount:.2f}")
                        print(f"        • Total to recover: ${required_profit_with_adder:.2f}")
                    else:
                        print(f"\n      📈 Loss Recovery:")
                        print(f"        • Required profit: ${required_profit:.2f}")
                    
                    # Store details
                    symbol_analysis[symbol]["lookback_losses"] = [
                        {
                            "symbol": loss["symbol"],
                            "amount": abs(loss["profit_loss"]),
                            "ticket": loss["ticket"],
                            "volume": loss["volume"],
                            "date": loss["date"].strftime('%Y-%m-%d')
                        }
                        for loss in lookback_losses
                    ]
                    symbol_analysis[symbol]["total_loss_amount"] = total_loss_amount
                    symbol_analysis[symbol]["total_loss_volume"] = total_loss_volume
                    symbol_analysis[symbol]["required_profit"] = required_profit
                    symbol_analysis[symbol]["required_profit_with_adder"] = required_profit_with_adder
                    symbol_analysis[symbol]["profits_after_losses_total"] = total_profit_after
                    
                    # Display losses details
                    print(f"\n      📉 Losses within lookback period for {symbol}:")
                    for idx, loss in enumerate(lookback_losses, 1):
                        print(f"        {idx}. {loss['symbol']}: ${abs(loss['profit_loss']):.2f} (Volume: {loss['volume']} lots) - Date: {loss['date'].strftime('%Y-%m-%d')}")
                    
                    # Display profits after losses
                    if profits_after_losses:
                        print(f"\n      📈 Profits that occurred after losses for {symbol}:")
                        for idx, profit in enumerate(profits_after_losses, 1):
                            print(f"        {idx}. {profit['symbol']}: ${profit['profit_loss']:.2f} - Date: {profit['date'].strftime('%Y-%m-%d')}")
                else:
                    print(f"        ℹ️  No losses found within lookback period for {symbol}")
                    symbol_analysis[symbol]["required_profit"] = 0
                    symbol_analysis[symbol]["required_profit_with_adder"] = 0
                    symbol_analysis[symbol]["total_loss_amount"] = 0
                
                print(f"\n      📊 Martingale Lookback Summary for {symbol}:")
                print(f"        • Days lookback: {days_lookback} day(s)")
                print(f"        • Lookback limit: {maximum_lookback} losses")
                print(f"        • Losses found in lookback: {len(lookback_losses)}")
                if len(lookback_losses) > 0 or symbol_analysis[symbol].get("required_profit", 0) > 0:
                    print(f"        • TOTAL LOSS AMOUNT: ${symbol_analysis[symbol]['total_loss_amount']:.2f}")
                    print(f"        • Total loss volume (ref only): {symbol_analysis[symbol]['total_loss_volume']} lots")
                    print(f"        • Profits after losses: ${symbol_analysis[symbol]['profits_after_losses_total']:.2f}")
                    print(f"        • Required profit (base): ${symbol_analysis[symbol]['required_profit']:.2f}")
                    print(f"        • Required profit (with {recovery_adder_percentage}% adder): ${symbol_analysis[symbol]['required_profit_with_adder']:.2f}")
                else:
                    print(f"        • No losses to recover")
                        
        except Exception as e:
            print(f"       Error in lookback analysis: {e}")
            import traceback
            traceback.print_exc()
        
        # --- PRE-SCALING: MODIFY SIGNALS.JSON DIRECTLY FOR POSITIONS WITH SL ---
        if martingale_pre_scaling:
            print(f"\n  └─ 🎯 PRE-SCALING: Checking positions with SL to pre-scale signals.json...")
            
            try:
                # Get all positions
                positions = mt5.positions_get()
                
                if positions is None:
                    positions = []
                
                print(f"      • Found {len(positions)} positions")
                
                if positions:
                    # Load signals.json
                    signals_path = inv_root / "prices" / "signals.json"
                    
                    if not signals_path.exists():
                        print(f"      ⚠️  signals.json not found at {signals_path}")
                        print(f"      ⏭️  Skipping pre-scaling")
                    else:
                        with open(signals_path, 'r', encoding='utf-8') as f:
                            signals_data = json.load(f)
                        
                        print(f"      📂 Loaded signals.json")
                        
                        # Process each position
                        for position in positions:
                            try:
                                symbol = position.symbol
                                position_sl = position.sl
                                position_type = position.type
                                position_volume = position.volume
                                position_entry = position.price_open
                                
                                print(f"\n      {'='*50}")
                                print(f"      📍 Checking position for PRE-SCALING: {symbol}")
                                print(f"      {'='*50}")
                                print(f"        • Position Type: {'BUY' if position_type == mt5.POSITION_TYPE_BUY else 'SELL'}")
                                print(f"        • Entry: {position_entry}")
                                print(f"        • SL: {position_sl}")
                                print(f"        • Volume: {position_volume}")
                                
                                if position_sl is None or position_sl == 0:
                                    print(f"        ℹ️  Position has no SL set - skipping pre-scaling")
                                    continue
                                
                                # Calculate expected loss if position hits SL
                                symbol_info = mt5.symbol_info(symbol)
                                if symbol_info:
                                    contract_size = symbol_info.trade_contract_size
                                    
                                    # Calculate loss amount
                                    if position_type == mt5.POSITION_TYPE_BUY:
                                        # BUY position loss = (entry - SL) * volume * contract_size
                                        price_diff = position_entry - position_sl
                                    else:
                                        # SELL position loss = (SL - entry) * volume * contract_size
                                        price_diff = position_sl - position_entry
                                    
                                    expected_loss = price_diff * position_volume * contract_size
                                    
                                    print(f"\n        💰 PRE-SCALING CALCULATION:")
                                    print(f"          • Price difference to SL: {price_diff:.5f}")
                                    print(f"          • Contract size: {contract_size}")
                                    print(f"          • Position volume: {position_volume}")
                                    print(f"          • Expected loss if SL hit: ${abs(expected_loss):.2f}")
                                    
                                    # Get the uncovered loss for this symbol from the analysis
                                    symbol_data = symbol_analysis.get(symbol, {})
                                    uncovered_loss = symbol_data.get("required_profit_with_adder", 0)
                                    
                                    # Add expected loss to uncovered loss
                                    total_to_recover = uncovered_loss + abs(expected_loss)
                                    
                                    print(f"\n        🎯 PRE-SCALING TARGET:")
                                    print(f"          • Current uncovered loss: ${uncovered_loss:.2f}")
                                    print(f"          • Expected loss from position: ${abs(expected_loss):.2f}")
                                    print(f"          • TOTAL TO RECOVER: ${total_to_recover:.2f}")
                                    
                                    # Find orders for this symbol in signals.json
                                    symbol_orders_found = False
                                    sample_entry = None
                                    sample_stop = None
                                    sample_order_type = None
                                    order_category = None
                                    order_type_bid_ask = None
                                    
                                    for category_name, category_data in signals_data.get('categories', {}).items():
                                        symbols_in_category = category_data.get('symbols', {})
                                        
                                        if symbol in symbols_in_category:
                                            symbol_signals = symbols_in_category[symbol]
                                            
                                            # Check for bid orders
                                            if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                                sample_order = symbol_signals['bid_orders'][0]
                                                sample_entry = sample_order.get('entry')
                                                sample_stop = sample_order.get('exit')
                                                sample_order_type = sample_order.get('order_type')
                                                order_category = category_name
                                                order_type_bid_ask = 'bid'
                                                symbol_orders_found = True
                                                break
                                            
                                            # Check for ask orders
                                            if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                                sample_order = symbol_signals['ask_orders'][0]
                                                sample_entry = sample_order.get('entry')
                                                sample_stop = sample_order.get('exit')
                                                sample_order_type = sample_order.get('order_type')
                                                order_category = category_name
                                                order_type_bid_ask = 'ask'
                                                symbol_orders_found = True
                                                break
                                    
                                    if not symbol_orders_found or not sample_entry or not sample_stop:
                                        print(f"        ⚠️  No orders found for {symbol} in signals.json")
                                        continue
                                    
                                    print(f"\n        📝 Found orders for {symbol}:")
                                    print(f"          • Category: {order_category}")
                                    print(f"          • Order Type: {sample_order_type}")
                                    print(f"          • Entry Price: {sample_entry}")
                                    print(f"          • Stop Loss: {sample_stop}")
                                    
                                    is_buy = 'buy' in sample_order_type.lower()
                                    
                                    if is_buy:
                                        calc_type = mt5.ORDER_TYPE_BUY
                                        order_direction = "BUY"
                                    else:
                                        calc_type = mt5.ORDER_TYPE_SELL
                                        order_direction = "SELL"
                                    
                                    price_diff_order = abs(sample_entry - sample_stop)
                                    
                                    if price_diff_order == 0:
                                        print(f"        ⚠️  Price difference is zero, cannot calculate volume")
                                        continue
                                    
                                    def calculate_profit_for_volume(volume):
                                        profit = mt5.order_calc_profit(
                                            calc_type,
                                            symbol,
                                            volume,
                                            sample_entry,
                                            sample_stop
                                        )
                                        return abs(profit) if profit is not None else None
                                    
                                    # Calculate required volume to recover total amount
                                    estimated_volume = total_to_recover / (price_diff_order * contract_size)
                                    required_volume = round(estimated_volume, 2)
                                    
                                    print(f"\n        📐 Volume Calculation:")
                                    print(f"          • Price difference (entry to stop): {price_diff_order:.5f}")
                                    print(f"          • Estimated volume: {estimated_volume:.4f} lots")
                                    print(f"          • Required volume: {required_volume} lots")
                                    
                                    min_volume = 0.01
                                    if required_volume < min_volume:
                                        print(f"        ⚠️  Required volume ({required_volume} lots) is below minimum lot size ({min_volume} lots)")
                                        print(f"        ⏭️  Skipping pre-scaling for {symbol} - volume too small")
                                        continue
                                    
                                    # Validate against maximum risk
                                    risk_for_required = calculate_profit_for_volume(required_volume)
                                    
                                    if risk_for_required is None:
                                        print(f"        ⚠️  Could not calculate risk")
                                        continue
                                    
                                    print(f"\n        🔍 Validating risk...")
                                    print(f"          • Martingale Maximum Risk: ${martingale_max_risk:.2f}")
                                    print(f"          • Calculated risk for {required_volume} lots: ${risk_for_required:.2f}")
                                    
                                    safe_volume = required_volume
                                    if risk_for_required > martingale_max_risk:
                                        print(f"           RISK CHECK FAILED - Adjusting volume")
                                        # Binary search for safe volume
                                        low = 0.01
                                        high = required_volume
                                        safe_volume = low
                                        iterations = 0
                                        
                                        while iterations < 20 and (high - low) > 0.001:
                                            mid = (low + high) / 2
                                            mid_risk = calculate_profit_for_volume(mid)
                                            
                                            if mid_risk is None:
                                                break
                                            
                                            if mid_risk <= martingale_max_risk:
                                                safe_volume = mid
                                                low = mid
                                            else:
                                                high = mid
                                            
                                            iterations += 1
                                        
                                        safe_volume = max(0.01, round(safe_volume, 2))
                                        safe_risk = calculate_profit_for_volume(safe_volume)
                                        print(f"          🔧 Adjusted to safe volume: {safe_volume} lots (risk: ${safe_risk:.2f})")
                                    else:
                                        print(f"          ✅ RISK CHECK PASSED")
                                    
                                    # Modify signals.json for this symbol
                                    orders_modified = 0
                                    original_volume = None
                                    
                                    for category_name, category_data in signals_data.get('categories', {}).items():
                                        symbols_in_category = category_data.get('symbols', {})
                                        
                                        if symbol in symbols_in_category:
                                            symbol_signals = symbols_in_category[symbol]
                                            
                                            if 'bid_orders' in symbol_signals:
                                                for order in symbol_signals['bid_orders']:
                                                    original_volume = order.get('volume', 0)
                                                    if abs(original_volume - safe_volume) > 0.001:
                                                        order['volume'] = safe_volume
                                                        orders_modified += 1
                                                        print(f"          🔄 Modified {symbol} bid order: {original_volume} → {safe_volume} lots")
                                            
                                            if 'ask_orders' in symbol_signals:
                                                for order in symbol_signals['ask_orders']:
                                                    original_volume = order.get('volume', 0)
                                                    if abs(original_volume - safe_volume) > 0.001:
                                                        order['volume'] = safe_volume
                                                        orders_modified += 1
                                                        print(f"          🔄 Modified {symbol} ask order: {original_volume} → {safe_volume} lots")
                                    
                                    if orders_modified > 0:
                                        pre_scaling_details[symbol] = {
                                            "symbol": symbol,
                                            "has_pre_scaling": True,
                                            "position_ticket": position.ticket,
                                            "expected_loss": abs(expected_loss),
                                            "uncovered_loss": uncovered_loss,
                                            "total_to_recover": total_to_recover,
                                            "old_volume": original_volume,
                                            "new_volume": safe_volume,
                                            "success": True
                                        }
                                        
                                        stats["pre_scaling_applied"] = True
                                        stats["signals_modified"] = True
                                        
                                        # Mark this symbol as handled by pre-scaling
                                        if symbol in symbol_analysis:
                                            symbol_analysis[symbol]["handled_by_pre_scaling"] = True
                                else:
                                    print(f"        ⚠️  Could not get symbol info for {symbol}")
                                    
                            except Exception as e:
                                print(f"         Error processing position: {e}")
                                import traceback
                                traceback.print_exc()
                                continue
                        
                        # Save signals.json if any modifications were made
                        if stats["pre_scaling_applied"]:
                            with open(signals_path, 'w', encoding='utf-8') as f:
                                json.dump(signals_data, f, indent=2, ensure_ascii=False)
                            
                            print(f"\n      ✅ Saved signals.json with pre-scaled volumes")
                        else:
                            print(f"\n      ℹ️  No pre-scaling modifications made")
                        
                else:
                    print(f"      ℹ️  No positions found to check for pre-scaling")
                        
            except Exception as e:
                print(f"       Error in pre-scaling: {e}")
                import traceback
                traceback.print_exc()
        
        # --- SYMBOL-SPECIFIC MARTINGALE VOLUME MODIFICATION (for signals.json) ---
        # Only modify signals.json for symbols that need recovery but weren't handled by pre-scaling
        symbols_with_recovery_needed = []
        for symbol, analysis in symbol_analysis.items():
            # Skip symbols that were handled by pre-scaling
            if analysis.get("handled_by_pre_scaling", False):
                continue
            if analysis.get("required_profit_with_adder", 0) > 0:
                symbols_with_recovery_needed.append(symbol)
        
        stats["symbols_with_loss"] = symbols_with_recovery_needed
        
        if symbols_with_recovery_needed and not martingale_pre_scaling:
            print(f"\n  └─ 🎰 Symbol-Specific Martingale Volume Adjustment...")
            
            signals_path = inv_root / "prices" / "signals.json"
            
            if not signals_path.exists():
                print(f"      ⚠️  signals.json not found at {signals_path}")
                print(f"      ⏭️  Skipping volume modification")
            else:
                try:
                    with open(signals_path, 'r', encoding='utf-8') as f:
                        signals_data = json.load(f)
                    
                    print(f"      📂 Loaded signals.json")
                    print(f"      🔄 Symbols requiring recovery: {', '.join(symbols_with_recovery_needed)}")
                    
                    # Track modifications per symbol
                    modifications_made = []
                    
                    # Process each symbol that needs recovery
                    for recovery_symbol in symbols_with_recovery_needed:
                        print(f"\n      {'='*50}")
                        print(f"      🎯 Processing symbol: {recovery_symbol}")
                        print(f"      {'='*50}")
                        
                        symbol_analysis_data = symbol_analysis.get(recovery_symbol, {})
                        required_profit_with_adder = symbol_analysis_data.get("required_profit_with_adder", 0)
                        
                        if required_profit_with_adder <= 0:
                            print(f"      ℹ️  No recovery needed for {recovery_symbol}")
                            continue
                        
                        print(f"      💰 Required profit to recover (with {stats['martingale_loss_recovery_adder_percentage']}% adder): ${required_profit_with_adder:.2f}")
                        print(f"      📊 Base loss amount: ${symbol_analysis_data.get('total_loss_amount', 0):.2f}")
                        
                        # Find orders for this specific symbol in signals.json
                        symbol_orders_found = False
                        sample_order = None
                        sample_entry = None
                        sample_stop = None
                        sample_order_type = None
                        order_category = None
                        order_type_bid_ask = None
                        
                        for category_name, category_data in signals_data.get('categories', {}).items():
                            symbols_in_category = category_data.get('symbols', {})
                            
                            if recovery_symbol in symbols_in_category:
                                symbol_signals = symbols_in_category[recovery_symbol]
                                
                                # Check for bid orders
                                if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                    sample_order = symbol_signals['bid_orders'][0]
                                    sample_entry = sample_order.get('entry')
                                    sample_stop = sample_order.get('exit')
                                    sample_order_type = sample_order.get('order_type')
                                    order_category = category_name
                                    order_type_bid_ask = 'bid'
                                    symbol_orders_found = True
                                    break
                                
                                # Check for ask orders
                                if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                    sample_order = symbol_signals['ask_orders'][0]
                                    sample_entry = sample_order.get('entry')
                                    sample_stop = sample_order.get('exit')
                                    sample_order_type = sample_order.get('order_type')
                                    order_category = category_name
                                    order_type_bid_ask = 'ask'
                                    symbol_orders_found = True
                                    break
                        
                        if not symbol_orders_found or not sample_entry or not sample_stop:
                            print(f"      ⚠️  No orders found for {recovery_symbol} in signals.json")
                            continue
                        
                        print(f"      📝 Found orders for {recovery_symbol}:")
                        print(f"        • Category: {order_category}")
                        print(f"        • Order Type: {sample_order_type}")
                        print(f"        • Entry Price: {sample_entry}")
                        print(f"        • Stop Loss: {sample_stop}")
                        
                        is_buy = 'buy' in sample_order_type.lower()
                        
                        if is_buy:
                            calc_type = mt5.ORDER_TYPE_BUY
                            order_direction = "BUY"
                        else:
                            calc_type = mt5.ORDER_TYPE_SELL
                            order_direction = "SELL"
                        
                        symbol_info = mt5.symbol_info(recovery_symbol)
                        if not symbol_info:
                            print(f"      ⚠️  Cannot get symbol info for {recovery_symbol}")
                            continue
                        
                        if not symbol_info.visible:
                            mt5.symbol_select(recovery_symbol, True)
                            print(f"      • Selected {recovery_symbol} in Market Watch")
                        
                        price_diff = abs(sample_entry - sample_stop)
                        contract_size = symbol_info.trade_contract_size
                        
                        print(f"      📊 Symbol Information:")
                        print(f"        • Contract size: {contract_size}")
                        print(f"        • Price difference (entry to stop): {price_diff:.2f}")
                        
                        def calculate_profit_for_volume(volume):
                            profit = mt5.order_calc_profit(
                                calc_type,
                                recovery_symbol,
                                volume,
                                sample_entry,
                                sample_stop
                            )
                            return abs(profit) if profit is not None else None
                        
                        # Calculate volume needed to recover the EXACT loss amount (with adder)
                        if price_diff * contract_size > 0:
                            estimated_volume = required_profit_with_adder / (price_diff * contract_size)
                            print(f"      📐 Estimated volume to recover ${required_profit_with_adder:.2f}: {estimated_volume:.4f} lots")
                            
                            # Round to 2 decimal places
                            required_volume = round(estimated_volume, 2)
                        else:
                            print(f"      ⚠️  Invalid price difference or contract size")
                            continue
                        
                        min_volume = 0.01
                        if required_volume < min_volume:
                            print(f"      ⚠️  Required volume ({required_volume} lots) is below minimum lot size ({min_volume} lots)")
                            print(f"      ⏭️  Skipping volume modification for {recovery_symbol} - volume too small to recover loss")
                            stats["order_risk_validation"][recovery_symbol] = {
                                "symbol": recovery_symbol,
                                "required_profit": required_profit_with_adder,
                                "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                "estimated_volume": estimated_volume,
                                "min_volume_needed": min_volume,
                                "status": "volume_too_small",
                                "message": f"Need at least {min_volume} lots to recover ${required_profit_with_adder:.2f}, but calculated volume is {required_volume:.4f} lots"
                            }
                            continue
                        
                        actual_profit = calculate_profit_for_volume(required_volume)
                        
                        if actual_profit is not None:
                            print(f"      💰 Calculated volume: {required_volume} lots")
                            print(f"        • Expected profit: ${actual_profit:.2f}")
                            print(f"        • Required profit (with adder): ${required_profit_with_adder:.2f}")
                            print(f"        • Difference: ${actual_profit - required_profit_with_adder:.2f}")
                            
                            # Validate against maximum risk
                            risk_for_required = calculate_profit_for_volume(required_volume)
                            
                            print(f"\n      🔍 Validating risk for calculated volume...")
                            print(f"      💰 Martingale Maximum Risk: ${stats['martingale_maximum_risk']:.2f}")
                            
                            if risk_for_required is not None:
                                print(f"      💰 RISK CALCULATION:")
                                print(f"        • Calculated risk for {required_volume} lots: ${risk_for_required:.2f}")
                                
                                if risk_for_required <= stats['martingale_maximum_risk']:
                                    print(f"        ✅ RISK CHECK PASSED")
                                    safe_volume = required_volume
                                    risk_check_passed = True
                                    risk_exceeded = False
                                    
                                    stats["order_risk_validation"][recovery_symbol] = {
                                        "symbol": recovery_symbol,
                                        "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                        "required_profit": required_profit_with_adder,
                                        "calculated_volume": required_volume,
                                        "calculated_risk": risk_for_required,
                                        "max_allowed_risk": stats['martingale_maximum_risk'],
                                        "price_difference": price_diff,
                                        "contract_size": contract_size,
                                        "risk_check_passed": True,
                                        "safe_volume": safe_volume
                                    }
                                else:
                                    print(f"         RISK CHECK FAILED")
                                    # Binary search for safe volume
                                    low = 0.01
                                    high = required_volume
                                    safe_volume = low
                                    iterations = 0
                                    
                                    while iterations < 20 and (high - low) > 0.001:
                                        mid = (low + high) / 2
                                        mid_risk = calculate_profit_for_volume(mid)
                                        
                                        if mid_risk is None:
                                            break
                                        
                                        if mid_risk <= stats['martingale_maximum_risk']:
                                            safe_volume = mid
                                            low = mid
                                        else:
                                            high = mid
                                        
                                        iterations += 1
                                    
                                    safe_volume = max(0.01, round(safe_volume, 2))
                                    safe_risk = calculate_profit_for_volume(safe_volume)
                                    
                                    print(f"        🔧 Adjusted to safe volume: {safe_volume} lots")
                                    print(f"        • Risk at safe volume: ${safe_risk:.2f}")
                                    
                                    risk_check_passed = True
                                    risk_exceeded = True
                                    
                                    stats["order_risk_validation"][recovery_symbol] = {
                                        "symbol": recovery_symbol,
                                        "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                        "required_profit": required_profit_with_adder,
                                        "calculated_volume": required_volume,
                                        "calculated_risk": risk_for_required,
                                        "safe_volume": safe_volume,
                                        "safe_risk": safe_risk,
                                        "max_allowed_risk": stats['martingale_maximum_risk'],
                                        "risk_check_passed": False,
                                        "risk_exceeded": True
                                    }
                            else:
                                print(f"      ⚠️  Could not calculate risk")
                                risk_check_passed = False
                        else:
                            print(f"      ⚠️  Could not calculate profit")
                            risk_check_passed = False
                        
                        # Modify signals.json for this specific symbol if needed
                        if risk_check_passed and safe_volume > 0 and safe_volume >= 0.01:
                            # Get original volume for comparison
                            original_volume = sample_order.get('volume', 0)
                            
                            if abs(safe_volume - original_volume) < 0.001:
                                print(f"      ℹ️  Calculated volume ({safe_volume} lots) is same as original for {recovery_symbol}")
                                modifications_made.append({
                                    "symbol": recovery_symbol,
                                    "modified": False,
                                    "old_volume": original_volume,
                                    "new_volume": safe_volume
                                })
                            else:
                                # Update orders for this specific symbol only in signals.json
                                orders_modified = 0
                                
                                for category_name, category_data in signals_data.get('categories', {}).items():
                                    symbols_in_category = category_data.get('symbols', {})
                                    
                                    if recovery_symbol in symbols_in_category:
                                        symbol_signals = symbols_in_category[recovery_symbol]
                                        
                                        if 'bid_orders' in symbol_signals:
                                            for order in symbol_signals['bid_orders']:
                                                old_volume = order.get('volume', 0)
                                                if abs(old_volume - safe_volume) > 0.001:
                                                    order['volume'] = safe_volume
                                                    orders_modified += 1
                                                    print(f"        🔄 Modified {recovery_symbol} bid order in signals.json: {old_volume} → {safe_volume} lots")
                                        
                                        if 'ask_orders' in symbol_signals:
                                            for order in symbol_signals['ask_orders']:
                                                old_volume = order.get('volume', 0)
                                                if abs(old_volume - safe_volume) > 0.001:
                                                    order['volume'] = safe_volume
                                                    orders_modified += 1
                                                    print(f"        🔄 Modified {recovery_symbol} ask order in signals.json: {old_volume} → {safe_volume} lots")
                                
                                if orders_modified > 0:
                                    modifications_made.append({
                                        "symbol": recovery_symbol,
                                        "modified": True,
                                        "old_volume": original_volume,
                                        "new_volume": safe_volume,
                                        "orders_modified": orders_modified
                                    })
                                    print(f"\n      ✅ Modified {orders_modified} orders in signals.json for {recovery_symbol} to volume: {safe_volume} lots")
                    
                    # Save signals.json if any modifications were made
                    if modifications_made and any(mod.get('modified', False) for mod in modifications_made):
                        with open(signals_path, 'w', encoding='utf-8') as f:
                            json.dump(signals_data, f, indent=2, ensure_ascii=False)
                        
                        stats["signals_modified"] = True
                        print(f"\n      ✅ Saved signals.json with {len([m for m in modifications_made if m.get('modified')])} symbols modified")
                    else:
                        stats["signals_modified"] = False
                        if modifications_made:
                            print(f"\n      ℹ️  No volume changes needed for any symbols")
                        else:
                            print(f"\n      ℹ️  No modifications made")
                    
                except Exception as e:
                    print(f"       Error: {e}")
                    import traceback
                    traceback.print_exc()
                    stats["errors"] += 1
                    stats["signals_modified"] = False
        else:
            if stats.get('pre_scaling_applied'):
                print(f"\n  └─ ℹ️  All symbols with losses were handled by PRE-SCALING. Skipping additional signals.json modification.")
            elif not symbols_with_recovery_needed:
                print(f"\n  └─ ℹ️  No symbols need loss recovery")
            elif martingale_pre_scaling:
                print(f"\n  └─ ℹ️  Pre-scaling is enabled - recovery handled there")
        
        # --- SAFETY: CANCEL MT5 ORDERS THAT DON'T MATCH SIGNALS.JSON VOLUMES ---
        print(f"\n  └─ 🛡️ SAFETY CHECK: Verifying MT5 orders match signals.json volumes...")
        
        try:
            # Get all pending orders from MT5
            pending_orders = mt5.orders_get()
            
            if pending_orders is None:
                pending_orders = []
            
            print(f"      • Found {len(pending_orders)} pending orders in MT5")
            
            if pending_orders:
                # Load signals.json to get expected volumes
                signals_path = inv_root / "prices" / "signals.json"
                
                if not signals_path.exists():
                    print(f"      ⚠️  signals.json not found - cannot verify volumes")
                else:
                    with open(signals_path, 'r', encoding='utf-8') as f:
                        signals_data = json.load(f)
                    
                    # Build expected volumes dictionary from signals.json
                    expected_volumes = {}
                    
                    for category_name, category_data in signals_data.get('categories', {}).items():
                        symbols_in_category = category_data.get('symbols', {})
                        
                        for symbol, symbol_signals in symbols_in_category.items():
                            # Check bid orders
                            if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                for order in symbol_signals['bid_orders']:
                                    order_type = order.get('order_type', '').lower()
                                    expected_volume = order.get('volume', 0)
                                    
                                    if expected_volume > 0:
                                        # Store expected volume for this symbol and order type
                                        if symbol not in expected_volumes:
                                            expected_volumes[symbol] = {}
                                        expected_volumes[symbol]['bid'] = expected_volume
                            
                            # Check ask orders
                            if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                for order in symbol_signals['ask_orders']:
                                    order_type = order.get('order_type', '').lower()
                                    expected_volume = order.get('volume', 0)
                                    
                                    if expected_volume > 0:
                                        if symbol not in expected_volumes:
                                            expected_volumes[symbol] = {}
                                        expected_volumes[symbol]['ask'] = expected_volume
                    
                    print(f"      📋 Expected volumes from signals.json:")
                    for symbol, volumes in expected_volumes.items():
                        print(f"        • {symbol}: Bid={volumes.get('bid', 0)} lots, Ask={volumes.get('ask', 0)} lots")
                    
                    # Check each pending order
                    orders_to_cancel = []
                    
                    for order in pending_orders:
                        symbol = order.symbol
                        order_type = order.type
                        order_volume = order.volume_initial
                        order_ticket = order.ticket
                        
                        # Determine if this is a buy or sell order
                        is_buy = order_type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP]
                        is_sell = order_type in [mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP]
                        
                        order_direction = 'bid' if is_buy else 'ask' if is_sell else None
                        
                        if not order_direction:
                            print(f"        ⚠️  Unknown order type {order_type} for ticket {order_ticket}")
                            continue
                        
                        # Get expected volume for this symbol and direction
                        expected_volume = expected_volumes.get(symbol, {}).get(order_direction, 0)
                        
                        if expected_volume == 0:
                            print(f"        ⚠️  No expected volume found for {symbol} {order_direction} order in signals.json")
                            print(f"          • Order ticket: {order_ticket}")
                            print(f"          • Current volume: {order_volume} lots")
                            print(f"          • Action: Will CANCEL this order (no matching entry in signals.json)")
                            orders_to_cancel.append(order)
                        elif abs(order_volume - expected_volume) > 0.001:
                            print(f"         Volume mismatch for {symbol} {order_direction.upper()} order:")
                            print(f"          • Order ticket: {order_ticket}")
                            print(f"          • Current volume: {order_volume} lots")
                            print(f"          • Expected volume: {expected_volume} lots")
                            print(f"          • Action: Will CANCEL this order")
                            orders_to_cancel.append(order)
                        else:
                            print(f"        ✅ {symbol} {order_direction.upper()} order volume matches: {order_volume} lots (Ticket: {order_ticket})")
                    
                    # Cancel mismatched orders
                    if orders_to_cancel:
                        print(f"\n      🔄 Cancelling {len(orders_to_cancel)} mismatched orders...")
                        
                        for order in orders_to_cancel:
                            try:
                                cancel_request = {
                                    "action": mt5.TRADE_ACTION_REMOVE,
                                    "order": order.ticket,
                                }
                                
                                cancel_result = mt5.order_send(cancel_request)
                                
                                if cancel_result and cancel_result.retcode == mt5.TRADE_RETCODE_DONE:
                                    print(f"        ✅ Successfully cancelled order ticket: {order.ticket} ({order.symbol})")
                                    safety_cancellations[order.ticket] = {
                                        "symbol": order.symbol,
                                        "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                        "old_volume": order.volume_initial,
                                        "success": True
                                    }
                                    safety_cancellations_count += 1
                                else:
                                    error_msg = "Unknown error"
                                    if cancel_result:
                                        error_msg = f"Retcode: {cancel_result.retcode}, Comment: {cancel_result.comment}"
                                    print(f"         Failed to cancel order ticket {order.ticket}: {error_msg}")
                                    safety_cancellations[order.ticket] = {
                                        "symbol": order.symbol,
                                        "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                        "old_volume": order.volume_initial,
                                        "success": False,
                                        "error": error_msg
                                    }
                                    stats["errors"] += 1
                                    
                            except Exception as e:
                                print(f"         Error cancelling order {order.ticket}: {e}")
                                safety_cancellations[order.ticket] = {
                                    "symbol": order.symbol,
                                    "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                    "old_volume": order.volume_initial,
                                    "success": False,
                                    "error": str(e)
                                }
                                stats["errors"] += 1
                        
                        if safety_cancellations_count > 0:
                            stats["pending_orders_modified"] = True
                            print(f"\n      ✅ Cancelled {safety_cancellations_count} mismatched orders")
                    else:
                        print(f"\n      ✅ All pending orders match signals.json volumes")
            
            else:
                print(f"      ℹ️  No pending orders found in MT5")
                
        except Exception as e:
            print(f"       Error in safety check: {e}")
            import traceback
            traceback.print_exc()
            stats["errors"] += 1
        
        stats["symbol_analysis"] = symbol_analysis
        stats["pre_scaling_details"] = pre_scaling_details
        stats["safety_cancellations"] = safety_cancellations
        stats["safety_cancellations_count"] = safety_cancellations_count
        
        stats["investors_processed"] += 1
        stats["processing_success"] = True

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 MARTINGALE STATUS SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Investors processed: {stats['investors_processed']}")
    print(f"   Martingale Enabled: {'✅ YES' if stats['martingale_enabled'] else ' NO'}")
    
    if stats['martingale_enabled']:
        print(f"   Maximum Lookback: {stats['maximum_martingale_lookback']} losses")
        print(f"   Days Lookback: {stats['martingale_days_lookback']} day(s)")
        print(f"   Martingale Maximum Risk: ${stats['martingale_maximum_risk']:.2f}")
        print(f"   Loss Recovery Adder: {stats['martingale_loss_recovery_adder_percentage']}%")
        print(f"   Sync to Pending Orders: {'✅ ENABLED' if stats['martingale_for_position_order_scale'] else ' DISABLED'}")
        print(f"   PRE-SCALING: {'✅ ENABLED' if stats['martingale_pre_scaling'] else ' DISABLED'}")
        print(f"   Has Daily Loss: {'✅ YES' if stats['has_loss'] else ' NO'}")
        
        if stats.get('pre_scaling_applied'):
            print(f"\n   🎯 PRE-SCALING APPLIED (Modified signals.json):")
            for symbol, details in stats['pre_scaling_details'].items():
                if details.get('has_pre_scaling'):
                    print(f"      🔹 {symbol}:")
                    print(f"         • Position expected loss: ${details['expected_loss']:.2f}")
                    if details.get('uncovered_loss'):
                        print(f"         • Uncovered loss: ${details['uncovered_loss']:.2f}")
                    print(f"         • Total to recover: ${details['total_to_recover']:.2f}")
                    print(f"         • Volume: {details['old_volume']} → {details['new_volume']} lots")
                    print(f"         • Status: {'✅ SUCCESS' if details.get('success') else ' FAILED'}")
        
        if stats.get('symbol_analysis'):
            print(f"\n   📊 Symbol-Specific Martingale Analysis:")
            for symbol, analysis in stats['symbol_analysis'].items():
                if analysis.get('required_profit_with_adder', 0) > 0 and not analysis.get('handled_by_pre_scaling', False):
                    print(f"\n      🔹 {symbol}:")
                    print(f"         • Losses in lookback: {analysis['losses_in_lookback']}")
                    print(f"         • Total loss amount: ${analysis['total_loss_amount']:.2f}")
                    print(f"         • Required profit (base): ${analysis['required_profit']:.2f}")
                    print(f"         • Required profit (with adder): ${analysis['required_profit_with_adder']:.2f}")
                    if analysis.get('lookback_days_used'):
                        print(f"         • Lookback days used: {analysis['lookback_days_used']}")
                    if analysis.get('earliest_loss_date'):
                        print(f"         • Earliest loss date: {analysis['earliest_loss_date']}")
        
        print(f"\n   🎰 Martingale Volume Adjustment:")
        if stats.get('pre_scaling_applied'):
            print(f"      • Handled by PRE-SCALING: ✅")
        elif stats.get('symbols_with_loss'):
            print(f"      • Symbols requiring recovery: {', '.join(stats['symbols_with_loss'])}")
        
        if stats.get('signals_modified'):
            print(f"      • Signals.json modified: ✅ YES")
        elif stats.get('pre_scaling_applied'):
            print(f"      • Signals.json modified: ✅ YES (via pre-scaling)")
        else:
            print(f"      • Signals.json modified:  NO")
        
        if stats.get('pending_orders_modified'):
            print(f"      • Pending orders modified: ✅ YES")
            if stats.get('safety_cancellations_count', 0) > 0:
                print(f"        • Safety cancellations: {stats['safety_cancellations_count']} orders cancelled")
        
        if stats.get('order_risk_validation'):
            print(f"\n   💰 Volume Calculations:")
            for symbol, validation in stats['order_risk_validation'].items():
                if isinstance(validation, dict):
                    print(f"\n      🔹 {symbol}:")
                    if validation.get('total_loss_amount'):
                        print(f"         • Total loss amount: ${validation['total_loss_amount']:.2f}")
                    if validation.get('required_profit'):
                        print(f"         • Required profit (with adder): ${validation['required_profit']:.2f}")
                    if validation.get('calculated_volume'):
                        print(f"         • Calculated volume: {validation['calculated_volume']} lots")
                    if validation.get('safe_volume'):
                        print(f"         • Safe volume: {validation['safe_volume']} lots")
                    if validation.get('status') == 'volume_too_small':
                        print(f"         • Status: ⚠️  Volume too small to recover")
                    elif validation.get('risk_check_passed'):
                        print(f"         • Status: ✅ Within limits")
                    else:
                        print(f"         • Status: ⚠️  Adjusted to safe volume")
    
    print(f"   Errors: {stats['errors']}")
    print(f"   Processing Status: {'✅ SUCCESS' if stats['processing_success'] else ' FAILED'}")
    
    print(f"\n{'='*10} 🏁 MARTINGALE STATUS CHECK COMPLETE {'='*10}\n")
    
    return stats

def martingale(inv_id=None):

    """
    Function: Checks daily loss and martingale status for the day.
    
    Calculates:
    - Starting balance for the day (balance at market open or start of day)
    - Current balance
    - Total loss incurred today
    - Martingale lookback analysis based on maximum_martingale_lookback setting
    - Modifies signals.json volumes based on safe volume that respects martingale_maximum_risk
    - Validates risk for one order using EXACT same method as check_pending_orders_risk()
    - Symbol-specific martingale tracking (each symbol tracked separately)
    - Loss recovery with percentage adder based on martingale_loss_recovery_adder_percentage
    - Optionally syncs volume changes to existing pending orders if martingale_for_position_order_scale is enabled
    - PRE-SCALING: Modifies signals.json volumes to account for expected losses from positions with SL AND highest-risk order in signals.json
    - SAFETY: Cancels any pending orders in MT5 that don't match the volumes in signals.json
    - DAYS LOOKBACK: Limits search to specified number of days (1 = today only, 2 = today and yesterday, etc.)
    
    Args:
        inv_id: Optional specific investor ID to process. If None, processes all investors.
        
    Returns:
        dict: Statistics about the daily loss and martingale status
    """
    print(f"\n{'='*10} 🎰 MARTINGALE STATUS CHECK {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")

    # Track statistics
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_processed": 0,
        "martingale_enabled": False,
        "maximum_martingale_lookback": 0,
        "martingale_days_lookback": 0,
        "martingale_maximum_risk": 0,
        "martingale_loss_recovery_adder_percentage": 0,
        "martingale_for_position_order_scale": False,
        "martingale_pre_scaling": False,
        "has_loss": False,
        "daily_loss": 0.0,
        "starting_balance": 0.0,
        "current_balance": 0.0,
        "loss_percentage": 0.0,
        "errors": 0,
        "processing_success": False,
        "symbols_with_loss": [],
        "symbol_analysis": {},
        "signals_modified": False,
        "pending_orders_modified": False,
        "risk_check_passed": False,
        "risk_exceeded": False,
        "order_risk_validation": {},
        "pending_order_sync_results": {},
        "pre_scaling_applied": False,
        "pre_scaling_details": {},
        "safety_cancellations": {},
        "safety_cancellations_count": 0
    }

    # Determine which investors to process
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    total_investors = len(investors_to_process) if not inv_id else 1
    processed = 0

    for user_brokerid in investors_to_process:
        processed += 1
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking martingale status...")
        
        # Reset per-investor variables
        pre_scaling_details = {}
        safety_cancellations = {}
        safety_cancellations_count = 0
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─  No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"

        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  Account config missing. Skipping.")
            continue

        # --- LOAD CONFIG AND CHECK MARTINGALE SETTINGS ---
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            # Check both old and new config structure
            settings = config.get("settings", {})
            martingale_config = settings.get("martingale_config", {})
            
            # If martingale_config exists, use it, otherwise fall back to old structure
            if martingale_config:
                martingale_enabled = martingale_config.get("enable_martingale", False)
                maximum_lookback = martingale_config.get("maximum_martingale_lookback", 3)
                days_lookback = martingale_config.get("martingale_days_lookback", 1)
                recovery_adder_str = martingale_config.get("martingale_loss_recovery_adder_percentage", "0%")
                martingale_for_position_order_scale = martingale_config.get("martingale_for_position_order_scale", False)
                martingale_pre_scaling = martingale_config.get("martingale_pre_scaling", False)
            else:
                # Fall back to old structure
                martingale_enabled = settings.get("enable_martingale", False)
                maximum_lookback = settings.get("maximum_martingale_lookback", 3)
                days_lookback = settings.get("martingale_days_lookback", 1)
                recovery_adder_str = settings.get("martingale_loss_recovery_adder_percentage", "0%")
                martingale_for_position_order_scale = settings.get("martingale_for_position_order_scale", False)
                martingale_pre_scaling = settings.get("martingale_pre_scaling", False)
            
            # Ensure days_lookback is at least 1
            try:
                days_lookback = int(days_lookback)
                if days_lookback < 1:
                    days_lookback = 1
            except (ValueError, TypeError):
                days_lookback = 1
            
            # Parse percentage (remove % sign and convert to float)
            recovery_adder_percentage = 0
            if recovery_adder_str:
                try:
                    recovery_adder_percentage = float(recovery_adder_str.replace('%', ''))
                except:
                    recovery_adder_percentage = 0
            
            stats["martingale_enabled"] = martingale_enabled
            stats["maximum_martingale_lookback"] = maximum_lookback
            stats["martingale_days_lookback"] = days_lookback
            stats["martingale_loss_recovery_adder_percentage"] = recovery_adder_percentage
            stats["martingale_for_position_order_scale"] = martingale_for_position_order_scale
            stats["martingale_pre_scaling"] = martingale_pre_scaling
            
            if not martingale_enabled:
                print(f"  └─ ⏭️  Martingale DISABLED in settings. Skipping.")
                stats["processing_success"] = True
                continue
            
            print(f"  └─ ✅ Martingale ENABLED")
            print(f"  └─ 🔢 Maximum Martingale Lookback: {maximum_lookback} losses")
            print(f"  └─ 📅 Days Lookback: {days_lookback} day(s)")
            print(f"  └─ 📈 Loss Recovery Adder: {recovery_adder_percentage}%")
            print(f"  └─ 🔄 Sync to Pending Orders: {'✅ ENABLED' if martingale_for_position_order_scale else ' DISABLED'}")
            print(f"  └─ 🎯 PRE-SCALING: {'✅ ENABLED' if martingale_pre_scaling else ' DISABLED'}")
            print(f"  └─ 🛡️ SAFETY: Will cancel MT5 orders that don't match signals.json")
            
        except Exception as e:
            print(f"  └─  Failed to read config: {e}")
            stats["errors"] += 1
            continue

        # --- ACCOUNT INITIALIZATION ---
        print(f"  └─ 🔌 Initializing account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")

        # Initialize MT5 connection if needed
        if not mt5.initialize(path=mt5_path):
            print(f"  └─  MT5 initialization failed")
            stats["errors"] += 1
            continue

        # Check login status
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─  Login failed: {error}")
                stats["errors"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        # --- GET CURRENT BALANCE ---
        account_info = mt5.account_info()
        if not account_info:
            print(f"  └─  Failed to get account information")
            stats["errors"] += 1
            continue
        
        current_balance = account_info.balance
        stats["current_balance"] = current_balance
        
        print(f"      • Current Balance: ${current_balance:.2f}")

        # --- GET MARTINGALE RISK BASED ON CURRENT BALANCE ---
        martingale_risk_map = config.get("martingale_risk_management", {})
        martingale_max_risk = None
        
        if martingale_risk_map:
            print(f"  └─ 📊 Determining Martingale Risk based on balance ${current_balance:.2f}...")
            
            for range_str, risk_value in martingale_risk_map.items():
                try:
                    raw_range = range_str.split("_")[0]
                    low_str, high_str = raw_range.split("-")
                    low = float(low_str)
                    high = float(high_str)
                    
                    if low <= current_balance <= high:
                        martingale_max_risk = float(risk_value)
                        print(f"      • Found matching range: {range_str}")
                        print(f"      • Risk limit: ${martingale_max_risk:.2f}")
                        break
                except Exception as e:
                    print(f"      ⚠️  Error parsing range '{range_str}': {e}")
                    continue
            
            if martingale_max_risk is None:
                print(f"      ⚠️  No risk mapping found for balance ${current_balance:.2f}")
                print(f"      Using default risk: $500")
                martingale_max_risk = 500
        else:
            print(f"      ⚠️  No martingale_risk_management section found in config")
            print(f"      Using default risk: $500")
            martingale_max_risk = 500
        
        stats["martingale_maximum_risk"] = martingale_max_risk
        print(f"  └─ 💰 Martingale Maximum Risk: ${martingale_max_risk:.2f}")

        # --- GET STARTING BALANCE FOR TODAY ---
        daily_stats_path = inv_root / "daily_stats.json"
        starting_balance = None
        
        today_date = datetime.now().date()
        today_str = today_date.strftime("%Y-%m-%d")
        
        print(f"      • Today's Date: {today_str}")
        
        if daily_stats_path.exists():
            try:
                with open(daily_stats_path, 'r', encoding='utf-8') as f:
                    daily_stats = json.load(f)
                
                if today_str in daily_stats:
                    starting_balance = daily_stats[today_str].get("starting_balance")
                    print(f"      • Found recorded starting balance: ${starting_balance:.2f}")
                else:
                    print(f"      • No recorded starting balance for today")
            except Exception as e:
                print(f"      ⚠️  Could not read daily stats: {e}")
        else:
            print(f"      • No daily stats file found")
        
        if starting_balance is None:
            print(f"      🔍 Calculating starting balance from trade history...")
            
            from_date = datetime(today_date.year, today_date.month, today_date.day, 0, 0, 0)
            to_date = datetime(today_date.year, today_date.month, today_date.day, 23, 59, 59)
            deals = mt5.history_deals_get(from_date, to_date)
            
            if deals is None:
                print(f"      ⚠️  Could not retrieve deal history")
                starting_balance = current_balance
                print(f"      • Using current balance as starting balance (no trades today)")
            else:
                today_profit = 0.0
                today_commission = 0.0
                today_swap = 0.0
                
                for deal in deals:
                    if deal.profit != 0 or deal.commission != 0 or deal.swap != 0:
                        today_profit += deal.profit
                        today_commission += deal.commission
                        today_swap += deal.swap
                
                total_pl = today_profit + today_commission + today_swap
                starting_balance = current_balance - total_pl
                
                print(f"      • Calculated starting balance: ${starting_balance:.2f}")
                print(f"      • Total profit/loss today: ${total_pl:.2f}")
            
            try:
                if daily_stats_path.exists():
                    with open(daily_stats_path, 'r', encoding='utf-8') as f:
                        daily_stats = json.load(f)
                else:
                    daily_stats = {}
                
                if today_str not in daily_stats:
                    daily_stats[today_str] = {}
                
                daily_stats[today_str]["starting_balance"] = starting_balance
                daily_stats[today_str]["last_updated"] = datetime.now().isoformat()
                
                with open(daily_stats_path, 'w', encoding='utf-8') as f:
                    json.dump(daily_stats, f, indent=2, ensure_ascii=False)
                
                print(f"      • Saved starting balance to daily_stats.json")
            except Exception as e:
                print(f"      ⚠️  Could not save daily stats: {e}")
        
        stats["starting_balance"] = starting_balance
        
        # --- CALCULATE DAILY LOSS ---
        daily_loss = starting_balance - current_balance
        
        if daily_loss > 0:
            loss_percentage = (daily_loss / starting_balance) * 100
            
            stats["has_loss"] = True
            stats["daily_loss"] = daily_loss
            stats["loss_percentage"] = loss_percentage
            
            print(f"\n  └─ 📉 DAILY LOSS DETECTED!")
            print(f"      • Starting Balance: ${starting_balance:.2f}")
            print(f"      • Current Balance: ${current_balance:.2f}")
            print(f"      • Total Loss Today: ${daily_loss:.2f}")
            print(f"      • Loss Percentage: {loss_percentage:.2f}%")
        else:
            print(f"\n  └─ ✅ NO DAILY LOSS")
            print(f"      • Starting Balance: ${starting_balance:.2f}")
            print(f"      • Current Balance: ${current_balance:.2f}")
            print(f"      • Change: ${current_balance - starting_balance:.2f} ({(current_balance - starting_balance) / starting_balance * 100:.2f}%)")
        
        # --- SYMBOL-SPECIFIC MARTINGALE LOOKBACK ANALYSIS WITH DAYS LOOKBACK ---
        print(f"\n  └─ 🔍 Performing Symbol-Specific Martingale Lookback Analysis...")
        print(f"      📅 Days Lookback: {days_lookback} day(s)")
        print(f"      🔢 Maximum Losses to Find: {maximum_lookback}")
        
        # Calculate date range based on days_lookback
        lookback_days = []
        for i in range(days_lookback):
            check_date = today_date - timedelta(days=i)
            lookback_days.append(check_date)
        
        print(f"      📆 Checking dates: {', '.join([d.strftime('%Y-%m-%d') for d in lookback_days])}")
        
        # Dictionary to store per-symbol analysis
        symbol_analysis = {}
        
        # Collect all deals within the lookback period
        all_deals_by_day = {}
        total_found_losses = {symbol: 0 for symbol in set()}
        
        try:
            # Fetch deals for each day in the lookback period
            for check_date in lookback_days:
                from_date = datetime(check_date.year, check_date.month, check_date.day, 0, 0, 0)
                to_date = datetime(check_date.year, check_date.month, check_date.day, 23, 59, 59)
                
                day_deals = mt5.history_deals_get(from_date, to_date)
                if day_deals and len(day_deals) > 0:
                    all_deals_by_day[check_date] = day_deals
                    print(f"      • {check_date.strftime('%Y-%m-%d')}: Found {len(day_deals)} deals")
                else:
                    print(f"      • {check_date.strftime('%Y-%m-%d')}: No deals found")
            
            # Process each symbol separately with cumulative loss collection across days
            # First, collect all deals and organize by symbol in chronological order across days
            symbol_deals_by_date = {}
            
            for check_date in sorted(all_deals_by_day.keys()):
                for deal in all_deals_by_day[check_date]:
                    symbol = deal.symbol
                    if symbol not in symbol_deals_by_date:
                        symbol_deals_by_date[symbol] = []
                    symbol_deals_by_date[symbol].append({
                        "deal": deal,
                        "date": check_date
                    })
            
            # Process each symbol
            for symbol, deals_with_dates in symbol_deals_by_date.items():
                print(f"\n      {'='*50}")
                print(f"      📊 Analyzing symbol: {symbol}")
                print(f"      {'='*50}")
                
                # Sort deals chronologically
                deals_with_dates.sort(key=lambda x: x["deal"].time)
                
                # Build P/L sequence for this symbol
                pl_sequence = []
                for item in deals_with_dates:
                    deal = item["deal"]
                    total_pl = deal.profit + deal.commission + deal.swap
                    if total_pl != 0:
                        pl_sequence.append({
                            "ticket": deal.ticket,
                            "symbol": deal.symbol,
                            "time": deal.time,
                            "date": item["date"],
                            "profit_loss": total_pl,
                            "type": "PROFIT" if total_pl > 0 else "LOSS",
                            "volume": deal.volume
                        })
                
                print(f"      • Total deals for {symbol}: {len(deals_with_dates)}")
                print(f"      • Non-zero P/L entries: {len(pl_sequence)}")
                
                if len(pl_sequence) == 0:
                    print(f"      ℹ️  No profit/loss entries found for {symbol}")
                    continue
                
                # Display sequence
                print(f"      📊 Profit/Loss Sequence (chronological):")
                display_count = min(10, len(pl_sequence))
                for idx, entry in enumerate(pl_sequence[:display_count], 1):
                    pl_sign = "📈" if entry["profit_loss"] > 0 else "📉"
                    print(f"        {idx}. {pl_sign} {entry['symbol']}: ${entry['profit_loss']:.2f} ({entry['type']}) - Volume: {entry['volume']} lots - Date: {entry['date'].strftime('%Y-%m-%d')}")
                
                latest_entry = pl_sequence[-1]
                latest_is_profit = latest_entry["profit_loss"] > 0
                
                print(f"\n      🎯 Latest Entry Analysis:")
                print(f"        • Latest is: {'PROFIT' if latest_is_profit else 'LOSS'}")
                print(f"        • Amount: ${abs(latest_entry['profit_loss']):.2f}")
                print(f"        • Volume: {latest_entry['volume']} lots")
                print(f"        • Date: {latest_entry['date'].strftime('%Y-%m-%d')}")
                
                # Collect losses within lookback period (scanning backwards from latest)
                # Stop when we have found maximum_lookback losses OR when we've processed all days
                print(f"\n      🔍 Analyzing losses within lookback period for {symbol}...")
                
                losses_found = []
                cumulative_profit = 0.0
                earliest_date_seen = None
                
                # Scan backwards from the latest entry to collect losses
                for i in range(len(pl_sequence) - 1, -1, -1):
                    entry = pl_sequence[i]
                    
                    # Track the earliest date we've processed
                    if earliest_date_seen is None or entry["date"] < earliest_date_seen:
                        earliest_date_seen = entry["date"]
                    
                    if entry["profit_loss"] < 0:
                        losses_found.append(entry)
                        print(f"        • Found loss: {entry['symbol']}: ${abs(entry['profit_loss']):.2f} (Date: {entry['date'].strftime('%Y-%m-%d')})")
                    else:
                        cumulative_profit += entry["profit_loss"]
                        print(f"        • Found profit: {entry['symbol']}: ${entry['profit_loss']:.2f} (Cumulative profit: ${cumulative_profit:.2f})")
                    
                    # Check if we've found enough losses
                    if len(losses_found) >= maximum_lookback:
                        print(f"        • Reached lookback limit of {maximum_lookback} losses")
                        break
                
                # Reverse to get chronological order
                lookback_losses = list(reversed(losses_found))
                
                # Initialize symbol analysis
                symbol_analysis[symbol] = {
                    "symbol": symbol,
                    "has_losses": len(lookback_losses) > 0,
                    "lookback_losses": [],
                    "total_loss_amount": 0.0,
                    "total_loss_volume": 0.0,
                    "required_profit": 0.0,
                    "required_profit_with_adder": 0.0,
                    "profits_after_losses_total": 0.0,
                    "latest_is_profit": latest_is_profit,
                    "latest_amount": abs(latest_entry["profit_loss"]),
                    "latest_volume": latest_entry["volume"],
                    "losses_in_lookback": len(lookback_losses),
                    "lookback_days_used": days_lookback,
                    "earliest_loss_date": earliest_date_seen.strftime('%Y-%m-%d') if earliest_date_seen else None
                }
                
                if lookback_losses:
                    # Calculate total loss amount for this symbol
                    total_loss_amount = sum(abs(loss["profit_loss"]) for loss in lookback_losses)
                    total_loss_volume = sum(loss["volume"] for loss in lookback_losses)
                    
                    print(f"\n      📊 Loss Analysis for {symbol}:")
                    print(f"        • Total losses found: {len(lookback_losses)}")
                    print(f"        • TOTAL LOSS AMOUNT: ${total_loss_amount:.2f}")
                    print(f"        • Total loss volume: {total_loss_volume} lots")
                    
                    # Find earliest loss index to identify profits after losses
                    earliest_loss_index = None
                    for i, entry in enumerate(pl_sequence):
                        if entry["ticket"] == lookback_losses[0]["ticket"]:
                            earliest_loss_index = i
                            break
                    
                    # Collect profits that occurred AFTER the earliest loss
                    profits_after_losses = []
                    if earliest_loss_index is not None:
                        for i in range(earliest_loss_index + 1, len(pl_sequence)):
                            if pl_sequence[i]["profit_loss"] > 0:
                                profits_after_losses.append(pl_sequence[i])
                    
                    total_profit_after = sum(p["profit_loss"] for p in profits_after_losses)
                    
                    print(f"\n      💰 Profit Analysis for {symbol}:")
                    print(f"        • Profits after the losses: {len(profits_after_losses)}")
                    print(f"        • Total profit after losses: ${total_profit_after:.2f}")
                    
                    # Calculate required profit (uncovered portion)
                    if total_profit_after >= total_loss_amount:
                        print(f"        ✅ Profits after losses cover all losses")
                        required_profit = 0
                    else:
                        required_profit = total_loss_amount - total_profit_after
                        print(f"         Profits after losses do NOT cover all losses")
                        print(f"        • Uncovered loss amount: ${required_profit:.2f}")
                    
                    # Apply percentage adder to required profit
                    adder_amount = required_profit * (recovery_adder_percentage / 100)
                    required_profit_with_adder = required_profit + adder_amount
                    
                    if recovery_adder_percentage > 0 and required_profit > 0:
                        print(f"\n      📈 Loss Recovery Adder ({recovery_adder_percentage}%):")
                        print(f"        • Base required profit: ${required_profit:.2f}")
                        print(f"        • Adder amount: ${adder_amount:.2f}")
                        print(f"        • Total to recover: ${required_profit_with_adder:.2f}")
                    else:
                        print(f"\n      📈 Loss Recovery:")
                        print(f"        • Required profit: ${required_profit:.2f}")
                    
                    # Store details
                    symbol_analysis[symbol]["lookback_losses"] = [
                        {
                            "symbol": loss["symbol"],
                            "amount": abs(loss["profit_loss"]),
                            "ticket": loss["ticket"],
                            "volume": loss["volume"],
                            "date": loss["date"].strftime('%Y-%m-%d')
                        }
                        for loss in lookback_losses
                    ]
                    symbol_analysis[symbol]["total_loss_amount"] = total_loss_amount
                    symbol_analysis[symbol]["total_loss_volume"] = total_loss_volume
                    symbol_analysis[symbol]["required_profit"] = required_profit
                    symbol_analysis[symbol]["required_profit_with_adder"] = required_profit_with_adder
                    symbol_analysis[symbol]["profits_after_losses_total"] = total_profit_after
                    
                    # Display losses details
                    print(f"\n      📉 Losses within lookback period for {symbol}:")
                    for idx, loss in enumerate(lookback_losses, 1):
                        print(f"        {idx}. {loss['symbol']}: ${abs(loss['profit_loss']):.2f} (Volume: {loss['volume']} lots) - Date: {loss['date'].strftime('%Y-%m-%d')}")
                    
                    # Display profits after losses
                    if profits_after_losses:
                        print(f"\n      📈 Profits that occurred after losses for {symbol}:")
                        for idx, profit in enumerate(profits_after_losses, 1):
                            print(f"        {idx}. {profit['symbol']}: ${profit['profit_loss']:.2f} - Date: {profit['date'].strftime('%Y-%m-%d')}")
                else:
                    print(f"        ℹ️  No losses found within lookback period for {symbol}")
                    symbol_analysis[symbol]["required_profit"] = 0
                    symbol_analysis[symbol]["required_profit_with_adder"] = 0
                    symbol_analysis[symbol]["total_loss_amount"] = 0
                
                print(f"\n      📊 Martingale Lookback Summary for {symbol}:")
                print(f"        • Days lookback: {days_lookback} day(s)")
                print(f"        • Lookback limit: {maximum_lookback} losses")
                print(f"        • Losses found in lookback: {len(lookback_losses)}")
                if len(lookback_losses) > 0 or symbol_analysis[symbol].get("required_profit", 0) > 0:
                    print(f"        • TOTAL LOSS AMOUNT: ${symbol_analysis[symbol]['total_loss_amount']:.2f}")
                    print(f"        • Total loss volume (ref only): {symbol_analysis[symbol]['total_loss_volume']} lots")
                    print(f"        • Profits after losses: ${symbol_analysis[symbol]['profits_after_losses_total']:.2f}")
                    print(f"        • Required profit (base): ${symbol_analysis[symbol]['required_profit']:.2f}")
                    print(f"        • Required profit (with {recovery_adder_percentage}% adder): ${symbol_analysis[symbol]['required_profit_with_adder']:.2f}")
                else:
                    print(f"        • No losses to recover")
                        
        except Exception as e:
            print(f"       Error in lookback analysis: {e}")
            import traceback
            traceback.print_exc()
        
        # --- PRE-SCALING: MODIFY SIGNALS.JSON DIRECTLY FOR POSITIONS WITH SL AND HIGHEST-RISK ORDERS ---
        if martingale_pre_scaling:
            print(f"\n  └─ 🎯 PRE-SCALING: Checking positions with SL and highest-risk orders in signals.json...")
            
            try:
                # Get all positions
                positions = mt5.positions_get()
                
                if positions is None:
                    positions = []
                
                print(f"      • Found {len(positions)} positions")
                
                if positions:
                    # Load signals.json
                    signals_path = inv_root / "prices" / "signals.json"
                    
                    if not signals_path.exists():
                        print(f"      ⚠️  signals.json not found at {signals_path}")
                        print(f"      ⏭️  Skipping pre-scaling")
                    else:
                        with open(signals_path, 'r', encoding='utf-8') as f:
                            signals_data = json.load(f)
                        
                        print(f"      📂 Loaded signals.json")
                        
                        # First, find the highest-risk order in signals.json for each symbol
                        print(f"\n      🔍 Finding highest-risk orders in signals.json...")
                        highest_risk_orders = {}
                        
                        for category_name, category_data in signals_data.get('categories', {}).items():
                            symbols_in_category = category_data.get('symbols', {})
                            
                            for symbol, symbol_signals in symbols_in_category.items():
                                highest_risk = 0
                                highest_risk_order_info = None
                                
                                # Check bid orders
                                if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                    for order in symbol_signals['bid_orders']:
                                        entry = order.get('entry')
                                        stop = order.get('exit')
                                        volume = order.get('volume', 0)
                                        
                                        if entry and stop and volume > 0:
                                            # Calculate risk for this order
                                            symbol_info = mt5.symbol_info(symbol)
                                            if symbol_info:
                                                contract_size = symbol_info.trade_contract_size
                                                price_diff = abs(entry - stop)
                                                risk = price_diff * volume * contract_size
                                                
                                                if risk > highest_risk:
                                                    highest_risk = risk
                                                    highest_risk_order_info = {
                                                        'order_type': order.get('order_type'),
                                                        'entry': entry,
                                                        'stop': stop,
                                                        'volume': volume,
                                                        'risk': risk,
                                                        'bid_ask': 'bid'
                                                    }
                                
                                # Check ask orders
                                if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                    for order in symbol_signals['ask_orders']:
                                        entry = order.get('entry')
                                        stop = order.get('exit')
                                        volume = order.get('volume', 0)
                                        
                                        if entry and stop and volume > 0:
                                            symbol_info = mt5.symbol_info(symbol)
                                            if symbol_info:
                                                contract_size = symbol_info.trade_contract_size
                                                price_diff = abs(entry - stop)
                                                risk = price_diff * volume * contract_size
                                                
                                                if risk > highest_risk:
                                                    highest_risk = risk
                                                    highest_risk_order_info = {
                                                        'order_type': order.get('order_type'),
                                                        'entry': entry,
                                                        'stop': stop,
                                                        'volume': volume,
                                                        'risk': risk,
                                                        'bid_ask': 'ask'
                                                    }
                                
                                if highest_risk_order_info:
                                    highest_risk_orders[symbol] = highest_risk_order_info
                                    print(f"      • {symbol}: Highest risk order = ${highest_risk:.2f} ({highest_risk_order_info['order_type']})")
                        
                        # Process each position
                        for position in positions:
                            try:
                                symbol = position.symbol
                                position_sl = position.sl
                                position_type = position.type
                                position_volume = position.volume
                                position_entry = position.price_open
                                
                                print(f"\n      {'='*50}")
                                print(f"      📍 Checking position for PRE-SCALING: {symbol}")
                                print(f"      {'='*50}")
                                print(f"        • Position Type: {'BUY' if position_type == mt5.POSITION_TYPE_BUY else 'SELL'}")
                                print(f"        • Entry: {position_entry}")
                                print(f"        • SL: {position_sl}")
                                print(f"        • Volume: {position_volume}")
                                
                                if position_sl is None or position_sl == 0:
                                    print(f"        ℹ️  Position has no SL set - skipping pre-scaling")
                                    continue
                                
                                # Calculate expected loss if position hits SL
                                symbol_info = mt5.symbol_info(symbol)
                                if symbol_info:
                                    contract_size = symbol_info.trade_contract_size
                                    
                                    # Calculate loss amount
                                    if position_type == mt5.POSITION_TYPE_BUY:
                                        # BUY position loss = (entry - SL) * volume * contract_size
                                        price_diff = position_entry - position_sl
                                    else:
                                        # SELL position loss = (SL - entry) * volume * contract_size
                                        price_diff = position_sl - position_entry
                                    
                                    expected_loss = price_diff * position_volume * contract_size
                                    
                                    print(f"\n        💰 PRE-SCALING CALCULATION:")
                                    print(f"          • Price difference to SL: {price_diff:.5f}")
                                    print(f"          • Contract size: {contract_size}")
                                    print(f"          • Position volume: {position_volume}")
                                    print(f"          • Expected loss if SL hit: ${abs(expected_loss):.2f}")
                                    
                                    # Get the uncovered loss for this symbol from the analysis
                                    symbol_data = symbol_analysis.get(symbol, {})
                                    uncovered_loss = symbol_data.get("required_profit_with_adder", 0)
                                    
                                    # Get the highest risk order for this symbol
                                    highest_risk_order = highest_risk_orders.get(symbol, {})
                                    highest_risk_value = highest_risk_order.get('risk', 0)
                                    
                                    print(f"\n        🎯 PRE-SCALING TARGET:")
                                    print(f"          • Current uncovered loss: ${uncovered_loss:.2f}")
                                    if highest_risk_value > 0:
                                        print(f"          • Highest risk order in signals.json: ${highest_risk_value:.2f} ({highest_risk_order.get('order_type', 'N/A')})")
                                    print(f"          • Expected loss from position: ${abs(expected_loss):.2f}")
                                    
                                    # TOTAL TO RECOVER = uncovered loss + highest risk order + expected loss from position
                                    total_to_recover = uncovered_loss + highest_risk_value + abs(expected_loss)
                                    
                                    print(f"          • TOTAL TO RECOVER: ${total_to_recover:.2f}")
                                    
                                    # Find orders for this symbol in signals.json
                                    symbol_orders_found = False
                                    sample_entry = None
                                    sample_stop = None
                                    sample_order_type = None
                                    order_category = None
                                    order_type_bid_ask = None
                                    
                                    for category_name, category_data in signals_data.get('categories', {}).items():
                                        symbols_in_category = category_data.get('symbols', {})
                                        
                                        if symbol in symbols_in_category:
                                            symbol_signals = symbols_in_category[symbol]
                                            
                                            # Check for bid orders
                                            if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                                sample_order = symbol_signals['bid_orders'][0]
                                                sample_entry = sample_order.get('entry')
                                                sample_stop = sample_order.get('exit')
                                                sample_order_type = sample_order.get('order_type')
                                                order_category = category_name
                                                order_type_bid_ask = 'bid'
                                                symbol_orders_found = True
                                                break
                                            
                                            # Check for ask orders
                                            if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                                sample_order = symbol_signals['ask_orders'][0]
                                                sample_entry = sample_order.get('entry')
                                                sample_stop = sample_order.get('exit')
                                                sample_order_type = sample_order.get('order_type')
                                                order_category = category_name
                                                order_type_bid_ask = 'ask'
                                                symbol_orders_found = True
                                                break
                                    
                                    if not symbol_orders_found or not sample_entry or not sample_stop:
                                        print(f"        ⚠️  No orders found for {symbol} in signals.json")
                                        continue
                                    
                                    print(f"\n        📝 Found orders for {symbol}:")
                                    print(f"          • Category: {order_category}")
                                    print(f"          • Order Type: {sample_order_type}")
                                    print(f"          • Entry Price: {sample_entry}")
                                    print(f"          • Stop Loss: {sample_stop}")
                                    
                                    is_buy = 'buy' in sample_order_type.lower()
                                    
                                    if is_buy:
                                        calc_type = mt5.ORDER_TYPE_BUY
                                        order_direction = "BUY"
                                    else:
                                        calc_type = mt5.ORDER_TYPE_SELL
                                        order_direction = "SELL"
                                    
                                    price_diff_order = abs(sample_entry - sample_stop)
                                    
                                    if price_diff_order == 0:
                                        print(f"        ⚠️  Price difference is zero, cannot calculate volume")
                                        continue
                                    
                                    def calculate_profit_for_volume(volume):
                                        profit = mt5.order_calc_profit(
                                            calc_type,
                                            symbol,
                                            volume,
                                            sample_entry,
                                            sample_stop
                                        )
                                        return abs(profit) if profit is not None else None
                                    
                                    # Calculate required volume to recover total amount
                                    estimated_volume = total_to_recover / (price_diff_order * contract_size)
                                    required_volume = round(estimated_volume, 2)
                                    
                                    print(f"\n        📐 Volume Calculation:")
                                    print(f"          • Price difference (entry to stop): {price_diff_order:.5f}")
                                    print(f"          • Estimated volume: {estimated_volume:.4f} lots")
                                    print(f"          • Required volume: {required_volume} lots")
                                    
                                    min_volume = 0.01
                                    if required_volume < min_volume:
                                        print(f"        ⚠️  Required volume ({required_volume} lots) is below minimum lot size ({min_volume} lots)")
                                        print(f"        ⏭️  Skipping pre-scaling for {symbol} - volume too small")
                                        continue
                                    
                                    # Validate against maximum risk
                                    risk_for_required = calculate_profit_for_volume(required_volume)
                                    
                                    if risk_for_required is None:
                                        print(f"        ⚠️  Could not calculate risk")
                                        continue
                                    
                                    print(f"\n        🔍 Validating risk...")
                                    print(f"          • Martingale Maximum Risk: ${martingale_max_risk:.2f}")
                                    print(f"          • Calculated risk for {required_volume} lots: ${risk_for_required:.2f}")
                                    
                                    safe_volume = required_volume
                                    if risk_for_required > martingale_max_risk:
                                        print(f"           RISK CHECK FAILED - Adjusting volume")
                                        # Binary search for safe volume
                                        low = 0.01
                                        high = required_volume
                                        safe_volume = low
                                        iterations = 0
                                        
                                        while iterations < 20 and (high - low) > 0.001:
                                            mid = (low + high) / 2
                                            mid_risk = calculate_profit_for_volume(mid)
                                            
                                            if mid_risk is None:
                                                break
                                            
                                            if mid_risk <= martingale_max_risk:
                                                safe_volume = mid
                                                low = mid
                                            else:
                                                high = mid
                                            
                                            iterations += 1
                                        
                                        safe_volume = max(0.01, round(safe_volume, 2))
                                        safe_risk = calculate_profit_for_volume(safe_volume)
                                        print(f"          🔧 Adjusted to safe volume: {safe_volume} lots (risk: ${safe_risk:.2f})")
                                    else:
                                        print(f"          ✅ RISK CHECK PASSED")
                                    
                                    # Modify signals.json for this symbol
                                    orders_modified = 0
                                    original_volume = None
                                    
                                    for category_name, category_data in signals_data.get('categories', {}).items():
                                        symbols_in_category = category_data.get('symbols', {})
                                        
                                        if symbol in symbols_in_category:
                                            symbol_signals = symbols_in_category[symbol]
                                            
                                            if 'bid_orders' in symbol_signals:
                                                for order in symbol_signals['bid_orders']:
                                                    original_volume = order.get('volume', 0)
                                                    if abs(original_volume - safe_volume) > 0.001:
                                                        order['volume'] = safe_volume
                                                        orders_modified += 1
                                                        print(f"          🔄 Modified {symbol} bid order: {original_volume} → {safe_volume} lots")
                                            
                                            if 'ask_orders' in symbol_signals:
                                                for order in symbol_signals['ask_orders']:
                                                    original_volume = order.get('volume', 0)
                                                    if abs(original_volume - safe_volume) > 0.001:
                                                        order['volume'] = safe_volume
                                                        orders_modified += 1
                                                        print(f"          🔄 Modified {symbol} ask order: {original_volume} → {safe_volume} lots")
                                    
                                    if orders_modified > 0:
                                        pre_scaling_details[symbol] = {
                                            "symbol": symbol,
                                            "has_pre_scaling": True,
                                            "position_ticket": position.ticket,
                                            "expected_loss": abs(expected_loss),
                                            "uncovered_loss": uncovered_loss,
                                            "highest_order_risk": highest_risk_value,
                                            "total_to_recover": total_to_recover,
                                            "old_volume": original_volume,
                                            "new_volume": safe_volume,
                                            "success": True
                                        }
                                        
                                        stats["pre_scaling_applied"] = True
                                        stats["signals_modified"] = True
                                        
                                        # Mark this symbol as handled by pre-scaling
                                        if symbol in symbol_analysis:
                                            symbol_analysis[symbol]["handled_by_pre_scaling"] = True
                                else:
                                    print(f"        ⚠️  Could not get symbol info for {symbol}")
                                    
                            except Exception as e:
                                print(f"         Error processing position: {e}")
                                import traceback
                                traceback.print_exc()
                                continue
                        
                        # Save signals.json if any modifications were made
                        if stats["pre_scaling_applied"]:
                            with open(signals_path, 'w', encoding='utf-8') as f:
                                json.dump(signals_data, f, indent=2, ensure_ascii=False)
                            
                            print(f"\n      ✅ Saved signals.json with pre-scaled volumes")
                        else:
                            print(f"\n      ℹ️  No pre-scaling modifications made")
                        
                else:
                    print(f"      ℹ️  No positions found to check for pre-scaling")
                        
            except Exception as e:
                print(f"       Error in pre-scaling: {e}")
                import traceback
                traceback.print_exc()
        
        # --- SYMBOL-SPECIFIC MARTINGALE VOLUME MODIFICATION (for signals.json) ---
        # Only modify signals.json for symbols that need recovery but weren't handled by pre-scaling
        symbols_with_recovery_needed = []
        for symbol, analysis in symbol_analysis.items():
            # Skip symbols that were handled by pre-scaling
            if analysis.get("handled_by_pre_scaling", False):
                continue
            if analysis.get("required_profit_with_adder", 0) > 0:
                symbols_with_recovery_needed.append(symbol)
        
        stats["symbols_with_loss"] = symbols_with_recovery_needed
        
        if symbols_with_recovery_needed and not martingale_pre_scaling:
            print(f"\n  └─ 🎰 Symbol-Specific Martingale Volume Adjustment...")
            
            signals_path = inv_root / "prices" / "signals.json"
            
            if not signals_path.exists():
                print(f"      ⚠️  signals.json not found at {signals_path}")
                print(f"      ⏭️  Skipping volume modification")
            else:
                try:
                    with open(signals_path, 'r', encoding='utf-8') as f:
                        signals_data = json.load(f)
                    
                    print(f"      📂 Loaded signals.json")
                    print(f"      🔄 Symbols requiring recovery: {', '.join(symbols_with_recovery_needed)}")
                    
                    # Track modifications per symbol
                    modifications_made = []
                    
                    # Process each symbol that needs recovery
                    for recovery_symbol in symbols_with_recovery_needed:
                        print(f"\n      {'='*50}")
                        print(f"      🎯 Processing symbol: {recovery_symbol}")
                        print(f"      {'='*50}")
                        
                        symbol_analysis_data = symbol_analysis.get(recovery_symbol, {})
                        required_profit_with_adder = symbol_analysis_data.get("required_profit_with_adder", 0)
                        
                        if required_profit_with_adder <= 0:
                            print(f"      ℹ️  No recovery needed for {recovery_symbol}")
                            continue
                        
                        print(f"      💰 Required profit to recover (with {stats['martingale_loss_recovery_adder_percentage']}% adder): ${required_profit_with_adder:.2f}")
                        print(f"      📊 Base loss amount: ${symbol_analysis_data.get('total_loss_amount', 0):.2f}")
                        
                        # Find orders for this specific symbol in signals.json
                        symbol_orders_found = False
                        sample_order = None
                        sample_entry = None
                        sample_stop = None
                        sample_order_type = None
                        order_category = None
                        order_type_bid_ask = None
                        
                        for category_name, category_data in signals_data.get('categories', {}).items():
                            symbols_in_category = category_data.get('symbols', {})
                            
                            if recovery_symbol in symbols_in_category:
                                symbol_signals = symbols_in_category[recovery_symbol]
                                
                                # Check for bid orders
                                if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                    sample_order = symbol_signals['bid_orders'][0]
                                    sample_entry = sample_order.get('entry')
                                    sample_stop = sample_order.get('exit')
                                    sample_order_type = sample_order.get('order_type')
                                    order_category = category_name
                                    order_type_bid_ask = 'bid'
                                    symbol_orders_found = True
                                    break
                                
                                # Check for ask orders
                                if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                    sample_order = symbol_signals['ask_orders'][0]
                                    sample_entry = sample_order.get('entry')
                                    sample_stop = sample_order.get('exit')
                                    sample_order_type = sample_order.get('order_type')
                                    order_category = category_name
                                    order_type_bid_ask = 'ask'
                                    symbol_orders_found = True
                                    break
                        
                        if not symbol_orders_found or not sample_entry or not sample_stop:
                            print(f"      ⚠️  No orders found for {recovery_symbol} in signals.json")
                            continue
                        
                        print(f"      📝 Found orders for {recovery_symbol}:")
                        print(f"        • Category: {order_category}")
                        print(f"        • Order Type: {sample_order_type}")
                        print(f"        • Entry Price: {sample_entry}")
                        print(f"        • Stop Loss: {sample_stop}")
                        
                        is_buy = 'buy' in sample_order_type.lower()
                        
                        if is_buy:
                            calc_type = mt5.ORDER_TYPE_BUY
                            order_direction = "BUY"
                        else:
                            calc_type = mt5.ORDER_TYPE_SELL
                            order_direction = "SELL"
                        
                        symbol_info = mt5.symbol_info(recovery_symbol)
                        if not symbol_info:
                            print(f"      ⚠️  Cannot get symbol info for {recovery_symbol}")
                            continue
                        
                        if not symbol_info.visible:
                            mt5.symbol_select(recovery_symbol, True)
                            print(f"      • Selected {recovery_symbol} in Market Watch")
                        
                        price_diff = abs(sample_entry - sample_stop)
                        contract_size = symbol_info.trade_contract_size
                        
                        print(f"      📊 Symbol Information:")
                        print(f"        • Contract size: {contract_size}")
                        print(f"        • Price difference (entry to stop): {price_diff:.2f}")
                        
                        def calculate_profit_for_volume(volume):
                            profit = mt5.order_calc_profit(
                                calc_type,
                                recovery_symbol,
                                volume,
                                sample_entry,
                                sample_stop
                            )
                            return abs(profit) if profit is not None else None
                        
                        # Calculate volume needed to recover the EXACT loss amount (with adder)
                        if price_diff * contract_size > 0:
                            estimated_volume = required_profit_with_adder / (price_diff * contract_size)
                            print(f"      📐 Estimated volume to recover ${required_profit_with_adder:.2f}: {estimated_volume:.4f} lots")
                            
                            # Round to 2 decimal places
                            required_volume = round(estimated_volume, 2)
                        else:
                            print(f"      ⚠️  Invalid price difference or contract size")
                            continue
                        
                        min_volume = 0.01
                        if required_volume < min_volume:
                            print(f"      ⚠️  Required volume ({required_volume} lots) is below minimum lot size ({min_volume} lots)")
                            print(f"      ⏭️  Skipping volume modification for {recovery_symbol} - volume too small to recover loss")
                            stats["order_risk_validation"][recovery_symbol] = {
                                "symbol": recovery_symbol,
                                "required_profit": required_profit_with_adder,
                                "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                "estimated_volume": estimated_volume,
                                "min_volume_needed": min_volume,
                                "status": "volume_too_small",
                                "message": f"Need at least {min_volume} lots to recover ${required_profit_with_adder:.2f}, but calculated volume is {required_volume:.4f} lots"
                            }
                            continue
                        
                        actual_profit = calculate_profit_for_volume(required_volume)
                        
                        if actual_profit is not None:
                            print(f"      💰 Calculated volume: {required_volume} lots")
                            print(f"        • Expected profit: ${actual_profit:.2f}")
                            print(f"        • Required profit (with adder): ${required_profit_with_adder:.2f}")
                            print(f"        • Difference: ${actual_profit - required_profit_with_adder:.2f}")
                            
                            # Validate against maximum risk
                            risk_for_required = calculate_profit_for_volume(required_volume)
                            
                            print(f"\n      🔍 Validating risk for calculated volume...")
                            print(f"      💰 Martingale Maximum Risk: ${stats['martingale_maximum_risk']:.2f}")
                            
                            if risk_for_required is not None:
                                print(f"      💰 RISK CALCULATION:")
                                print(f"        • Calculated risk for {required_volume} lots: ${risk_for_required:.2f}")
                                
                                if risk_for_required <= stats['martingale_maximum_risk']:
                                    print(f"        ✅ RISK CHECK PASSED")
                                    safe_volume = required_volume
                                    risk_check_passed = True
                                    risk_exceeded = False
                                    
                                    stats["order_risk_validation"][recovery_symbol] = {
                                        "symbol": recovery_symbol,
                                        "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                        "required_profit": required_profit_with_adder,
                                        "calculated_volume": required_volume,
                                        "calculated_risk": risk_for_required,
                                        "max_allowed_risk": stats['martingale_maximum_risk'],
                                        "price_difference": price_diff,
                                        "contract_size": contract_size,
                                        "risk_check_passed": True,
                                        "safe_volume": safe_volume
                                    }
                                else:
                                    print(f"         RISK CHECK FAILED")
                                    # Binary search for safe volume
                                    low = 0.01
                                    high = required_volume
                                    safe_volume = low
                                    iterations = 0
                                    
                                    while iterations < 20 and (high - low) > 0.001:
                                        mid = (low + high) / 2
                                        mid_risk = calculate_profit_for_volume(mid)
                                        
                                        if mid_risk is None:
                                            break
                                        
                                        if mid_risk <= stats['martingale_maximum_risk']:
                                            safe_volume = mid
                                            low = mid
                                        else:
                                            high = mid
                                        
                                        iterations += 1
                                    
                                    safe_volume = max(0.01, round(safe_volume, 2))
                                    safe_risk = calculate_profit_for_volume(safe_volume)
                                    
                                    print(f"        🔧 Adjusted to safe volume: {safe_volume} lots")
                                    print(f"        • Risk at safe volume: ${safe_risk:.2f}")
                                    
                                    risk_check_passed = True
                                    risk_exceeded = True
                                    
                                    stats["order_risk_validation"][recovery_symbol] = {
                                        "symbol": recovery_symbol,
                                        "total_loss_amount": symbol_analysis_data.get('total_loss_amount', 0),
                                        "required_profit": required_profit_with_adder,
                                        "calculated_volume": required_volume,
                                        "calculated_risk": risk_for_required,
                                        "safe_volume": safe_volume,
                                        "safe_risk": safe_risk,
                                        "max_allowed_risk": stats['martingale_maximum_risk'],
                                        "risk_check_passed": False,
                                        "risk_exceeded": True
                                    }
                            else:
                                print(f"      ⚠️  Could not calculate risk")
                                risk_check_passed = False
                        else:
                            print(f"      ⚠️  Could not calculate profit")
                            risk_check_passed = False
                        
                        # Modify signals.json for this specific symbol if needed
                        if risk_check_passed and safe_volume > 0 and safe_volume >= 0.01:
                            # Get original volume for comparison
                            original_volume = sample_order.get('volume', 0)
                            
                            if abs(safe_volume - original_volume) < 0.001:
                                print(f"      ℹ️  Calculated volume ({safe_volume} lots) is same as original for {recovery_symbol}")
                                modifications_made.append({
                                    "symbol": recovery_symbol,
                                    "modified": False,
                                    "old_volume": original_volume,
                                    "new_volume": safe_volume
                                })
                            else:
                                # Update orders for this specific symbol only in signals.json
                                orders_modified = 0
                                
                                for category_name, category_data in signals_data.get('categories', {}).items():
                                    symbols_in_category = category_data.get('symbols', {})
                                    
                                    if recovery_symbol in symbols_in_category:
                                        symbol_signals = symbols_in_category[recovery_symbol]
                                        
                                        if 'bid_orders' in symbol_signals:
                                            for order in symbol_signals['bid_orders']:
                                                old_volume = order.get('volume', 0)
                                                if abs(old_volume - safe_volume) > 0.001:
                                                    order['volume'] = safe_volume
                                                    orders_modified += 1
                                                    print(f"        🔄 Modified {recovery_symbol} bid order in signals.json: {old_volume} → {safe_volume} lots")
                                        
                                        if 'ask_orders' in symbol_signals:
                                            for order in symbol_signals['ask_orders']:
                                                old_volume = order.get('volume', 0)
                                                if abs(old_volume - safe_volume) > 0.001:
                                                    order['volume'] = safe_volume
                                                    orders_modified += 1
                                                    print(f"        🔄 Modified {recovery_symbol} ask order in signals.json: {old_volume} → {safe_volume} lots")
                                
                                if orders_modified > 0:
                                    modifications_made.append({
                                        "symbol": recovery_symbol,
                                        "modified": True,
                                        "old_volume": original_volume,
                                        "new_volume": safe_volume,
                                        "orders_modified": orders_modified
                                    })
                                    print(f"\n      ✅ Modified {orders_modified} orders in signals.json for {recovery_symbol} to volume: {safe_volume} lots")
                    
                    # Save signals.json if any modifications were made
                    if modifications_made and any(mod.get('modified', False) for mod in modifications_made):
                        with open(signals_path, 'w', encoding='utf-8') as f:
                            json.dump(signals_data, f, indent=2, ensure_ascii=False)
                        
                        stats["signals_modified"] = True
                        print(f"\n      ✅ Saved signals.json with {len([m for m in modifications_made if m.get('modified')])} symbols modified")
                    else:
                        stats["signals_modified"] = False
                        if modifications_made:
                            print(f"\n      ℹ️  No volume changes needed for any symbols")
                        else:
                            print(f"\n      ℹ️  No modifications made")
                    
                except Exception as e:
                    print(f"       Error: {e}")
                    import traceback
                    traceback.print_exc()
                    stats["errors"] += 1
                    stats["signals_modified"] = False
        else:
            if stats.get('pre_scaling_applied'):
                print(f"\n  └─ ℹ️  All symbols with losses were handled by PRE-SCALING. Skipping additional signals.json modification.")
            elif not symbols_with_recovery_needed:
                print(f"\n  └─ ℹ️  No symbols need loss recovery")
            elif martingale_pre_scaling:
                print(f"\n  └─ ℹ️  Pre-scaling is enabled - recovery handled there")
        
        # --- SAFETY: CANCEL MT5 ORDERS THAT DON'T MATCH SIGNALS.JSON VOLUMES ---
        print(f"\n  └─ 🛡️ SAFETY CHECK: Verifying MT5 orders match signals.json volumes...")
        
        try:
            # Get all pending orders from MT5
            pending_orders = mt5.orders_get()
            
            if pending_orders is None:
                pending_orders = []
            
            print(f"      • Found {len(pending_orders)} pending orders in MT5")
            
            if pending_orders:
                # Load signals.json to get expected volumes
                signals_path = inv_root / "prices" / "signals.json"
                
                if not signals_path.exists():
                    print(f"      ⚠️  signals.json not found - cannot verify volumes")
                else:
                    with open(signals_path, 'r', encoding='utf-8') as f:
                        signals_data = json.load(f)
                    
                    # Build expected volumes dictionary from signals.json
                    expected_volumes = {}
                    
                    for category_name, category_data in signals_data.get('categories', {}).items():
                        symbols_in_category = category_data.get('symbols', {})
                        
                        for symbol, symbol_signals in symbols_in_category.items():
                            # Check bid orders
                            if 'bid_orders' in symbol_signals and symbol_signals['bid_orders']:
                                for order in symbol_signals['bid_orders']:
                                    order_type = order.get('order_type', '').lower()
                                    expected_volume = order.get('volume', 0)
                                    
                                    if expected_volume > 0:
                                        # Store expected volume for this symbol and order type
                                        if symbol not in expected_volumes:
                                            expected_volumes[symbol] = {}
                                        expected_volumes[symbol]['bid'] = expected_volume
                            
                            # Check ask orders
                            if 'ask_orders' in symbol_signals and symbol_signals['ask_orders']:
                                for order in symbol_signals['ask_orders']:
                                    order_type = order.get('order_type', '').lower()
                                    expected_volume = order.get('volume', 0)
                                    
                                    if expected_volume > 0:
                                        if symbol not in expected_volumes:
                                            expected_volumes[symbol] = {}
                                        expected_volumes[symbol]['ask'] = expected_volume
                    
                    print(f"      📋 Expected volumes from signals.json:")
                    for symbol, volumes in expected_volumes.items():
                        print(f"        • {symbol}: Bid={volumes.get('bid', 0)} lots, Ask={volumes.get('ask', 0)} lots")
                    
                    # Check each pending order
                    orders_to_cancel = []
                    
                    for order in pending_orders:
                        symbol = order.symbol
                        order_type = order.type
                        order_volume = order.volume_initial
                        order_ticket = order.ticket
                        
                        # Determine if this is a buy or sell order
                        is_buy = order_type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP]
                        is_sell = order_type in [mt5.ORDER_TYPE_SELL_LIMIT, mt5.ORDER_TYPE_SELL_STOP]
                        
                        order_direction = 'bid' if is_buy else 'ask' if is_sell else None
                        
                        if not order_direction:
                            print(f"        ⚠️  Unknown order type {order_type} for ticket {order_ticket}")
                            continue
                        
                        # Get expected volume for this symbol and direction
                        expected_volume = expected_volumes.get(symbol, {}).get(order_direction, 0)
                        
                        if expected_volume == 0:
                            print(f"        ⚠️  No expected volume found for {symbol} {order_direction} order in signals.json")
                            print(f"          • Order ticket: {order_ticket}")
                            print(f"          • Current volume: {order_volume} lots")
                            print(f"          • Action: Will CANCEL this order (no matching entry in signals.json)")
                            orders_to_cancel.append(order)
                        elif abs(order_volume - expected_volume) > 0.001:
                            print(f"         Volume mismatch for {symbol} {order_direction.upper()} order:")
                            print(f"          • Order ticket: {order_ticket}")
                            print(f"          • Current volume: {order_volume} lots")
                            print(f"          • Expected volume: {expected_volume} lots")
                            print(f"          • Action: Will CANCEL this order")
                            orders_to_cancel.append(order)
                        else:
                            print(f"        ✅ {symbol} {order_direction.upper()} order volume matches: {order_volume} lots (Ticket: {order_ticket})")
                    
                    # Cancel mismatched orders
                    if orders_to_cancel:
                        print(f"\n      🔄 Cancelling {len(orders_to_cancel)} mismatched orders...")
                        
                        for order in orders_to_cancel:
                            try:
                                cancel_request = {
                                    "action": mt5.TRADE_ACTION_REMOVE,
                                    "order": order.ticket,
                                }
                                
                                cancel_result = mt5.order_send(cancel_request)
                                
                                if cancel_result and cancel_result.retcode == mt5.TRADE_RETCODE_DONE:
                                    print(f"        ✅ Successfully cancelled order ticket: {order.ticket} ({order.symbol})")
                                    safety_cancellations[order.ticket] = {
                                        "symbol": order.symbol,
                                        "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                        "old_volume": order.volume_initial,
                                        "success": True
                                    }
                                    safety_cancellations_count += 1
                                else:
                                    error_msg = "Unknown error"
                                    if cancel_result:
                                        error_msg = f"Retcode: {cancel_result.retcode}, Comment: {cancel_result.comment}"
                                    print(f"         Failed to cancel order ticket {order.ticket}: {error_msg}")
                                    safety_cancellations[order.ticket] = {
                                        "symbol": order.symbol,
                                        "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                        "old_volume": order.volume_initial,
                                        "success": False,
                                        "error": error_msg
                                    }
                                    stats["errors"] += 1
                                    
                            except Exception as e:
                                print(f"         Error cancelling order {order.ticket}: {e}")
                                safety_cancellations[order.ticket] = {
                                    "symbol": order.symbol,
                                    "type": "BUY" if order.type in [mt5.ORDER_TYPE_BUY_LIMIT, mt5.ORDER_TYPE_BUY_STOP] else "SELL",
                                    "old_volume": order.volume_initial,
                                    "success": False,
                                    "error": str(e)
                                }
                                stats["errors"] += 1
                        
                        if safety_cancellations_count > 0:
                            stats["pending_orders_modified"] = True
                            print(f"\n      ✅ Cancelled {safety_cancellations_count} mismatched orders")
                    else:
                        print(f"\n      ✅ All pending orders match signals.json volumes")
            
            else:
                print(f"      ℹ️  No pending orders found in MT5")
                
        except Exception as e:
            print(f"       Error in safety check: {e}")
            import traceback
            traceback.print_exc()
            stats["errors"] += 1
        
        stats["symbol_analysis"] = symbol_analysis
        stats["pre_scaling_details"] = pre_scaling_details
        stats["safety_cancellations"] = safety_cancellations
        stats["safety_cancellations_count"] = safety_cancellations_count
        
        stats["investors_processed"] += 1
        stats["processing_success"] = True

    # --- FINAL SUMMARY ---
    print(f"\n{'='*10} 📊 MARTINGALE STATUS SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Investors processed: {stats['investors_processed']}")
    print(f"   Martingale Enabled: {'✅ YES' if stats['martingale_enabled'] else ' NO'}")
    
    if stats['martingale_enabled']:
        print(f"   Maximum Lookback: {stats['maximum_martingale_lookback']} losses")
        print(f"   Days Lookback: {stats['martingale_days_lookback']} day(s)")
        print(f"   Martingale Maximum Risk: ${stats['martingale_maximum_risk']:.2f}")
        print(f"   Loss Recovery Adder: {stats['martingale_loss_recovery_adder_percentage']}%")
        print(f"   Sync to Pending Orders: {'✅ ENABLED' if stats['martingale_for_position_order_scale'] else ' DISABLED'}")
        print(f"   PRE-SCALING: {'✅ ENABLED' if stats['martingale_pre_scaling'] else ' DISABLED'}")
        print(f"   Has Daily Loss: {'✅ YES' if stats['has_loss'] else ' NO'}")
        
        if stats.get('pre_scaling_applied'):
            print(f"\n   🎯 PRE-SCALING APPLIED (Modified signals.json):")
            for symbol, details in stats['pre_scaling_details'].items():
                if details.get('has_pre_scaling'):
                    print(f"      🔹 {symbol}:")
                    print(f"         • Position expected loss: ${details['expected_loss']:.2f}")
                    if details.get('uncovered_loss'):
                        print(f"         • Uncovered loss: ${details['uncovered_loss']:.2f}")
                    if details.get('highest_order_risk'):
                        print(f"         • Highest order risk: ${details['highest_order_risk']:.2f}")
                    print(f"         • Total to recover: ${details['total_to_recover']:.2f}")
                    print(f"         • Volume: {details['old_volume']} → {details['new_volume']} lots")
                    print(f"         • Status: {'✅ SUCCESS' if details.get('success') else ' FAILED'}")
        
        if stats.get('symbol_analysis'):
            print(f"\n   📊 Symbol-Specific Martingale Analysis:")
            for symbol, analysis in stats['symbol_analysis'].items():
                if analysis.get('required_profit_with_adder', 0) > 0 and not analysis.get('handled_by_pre_scaling', False):
                    print(f"\n      🔹 {symbol}:")
                    print(f"         • Losses in lookback: {analysis['losses_in_lookback']}")
                    print(f"         • Total loss amount: ${analysis['total_loss_amount']:.2f}")
                    print(f"         • Required profit (base): ${analysis['required_profit']:.2f}")
                    print(f"         • Required profit (with adder): ${analysis['required_profit_with_adder']:.2f}")
                    if analysis.get('lookback_days_used'):
                        print(f"         • Lookback days used: {analysis['lookback_days_used']}")
                    if analysis.get('earliest_loss_date'):
                        print(f"         • Earliest loss date: {analysis['earliest_loss_date']}")
        
        print(f"\n   🎰 Martingale Volume Adjustment:")
        if stats.get('pre_scaling_applied'):
            print(f"      • Handled by PRE-SCALING: ✅")
        elif stats.get('symbols_with_loss'):
            print(f"      • Symbols requiring recovery: {', '.join(stats['symbols_with_loss'])}")
        
        if stats.get('signals_modified'):
            print(f"      • Signals.json modified: ✅ YES")
        elif stats.get('pre_scaling_applied'):
            print(f"      • Signals.json modified: ✅ YES (via pre-scaling)")
        else:
            print(f"      • Signals.json modified:  NO")
        
        if stats.get('pending_orders_modified'):
            print(f"      • Pending orders modified: ✅ YES")
            if stats.get('safety_cancellations_count', 0) > 0:
                print(f"        • Safety cancellations: {stats['safety_cancellations_count']} orders cancelled")
        
        if stats.get('order_risk_validation'):
            print(f"\n   💰 Volume Calculations:")
            for symbol, validation in stats['order_risk_validation'].items():
                if isinstance(validation, dict):
                    print(f"\n      🔹 {symbol}:")
                    if validation.get('total_loss_amount'):
                        print(f"         • Total loss amount: ${validation['total_loss_amount']:.2f}")
                    if validation.get('required_profit'):
                        print(f"         • Required profit (with adder): ${validation['required_profit']:.2f}")
                    if validation.get('calculated_volume'):
                        print(f"         • Calculated volume: {validation['calculated_volume']} lots")
                    if validation.get('safe_volume'):
                        print(f"         • Safe volume: {validation['safe_volume']} lots")
                    if validation.get('status') == 'volume_too_small':
                        print(f"         • Status: ⚠️  Volume too small to recover")
                    elif validation.get('risk_check_passed'):
                        print(f"         • Status: ✅ Within limits")
                    else:
                        print(f"         • Status: ⚠️  Adjusted to safe volume")
    
    print(f"   Errors: {stats['errors']}")
    print(f"   Processing Status: {'✅ SUCCESS' if stats['processing_success'] else ' FAILED'}")
    
    print(f"\n{'='*10} 🏁 MARTINGALE STATUS CHECK COMPLETE {'='*10}\n")
    
    return stats

def check_pending_orders_risk(inv_id=None):
    """
    Function 3: Validates live pending orders against the account's current risk bucket.
     VERSION: Uses the EXACT account initialization logic from place_usd_orders_for_accounts()
    Only removes orders with risk HIGHER than allowed (lower risk orders are kept).
    
    NOW CHECKS: ALL pending orders (LIMIT, STOP, STOP-LIMIT)
    
    RISK CONFIGURATION LOGIC:
    - If enable_martingale = true: USE martingale_risk_management (balance-based risk from dedicated section)
    - Else if enable_maximum_account_balance_management = true -> use account_balance_maximum_risk_management
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
        "martingale_active": False,
        "martingale_max_risk": 0.0,
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
            print(f"  └─  No broker config found")
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
            enable_martingale = settings.get("enable_martingale", False)
            enable_default = settings.get("enable_default_account_balance_management", False)
            enable_maximum = settings.get("enable_maximum_account_balance_management", False)
            
            print(f"  └─ ⚙️  Risk Configuration Settings:")
            print(f"      • enable_martingale: {enable_martingale}")
            print(f"      • enable_default_account_balance_management: {enable_default}")
            print(f"      • enable_maximum_account_balance_management: {enable_maximum}")
            
            # Determine which risk config to use
            risk_map = None
            risk_config_used = None
            primary_risk = None
            
            # Get account info first to get current balance
            # --- ACCOUNT CONNECTION CHECK ---
            print(f"  └─ 🔌 Checking account connection...")
            
            login_id = int(broker_cfg['LOGIN_ID'])
            mt5_path = broker_cfg["TERMINAL_PATH"]
            
            print(f"      • Terminal Path: {mt5_path}")
            print(f"      • Login ID: {login_id}")

            # Check if already logged into correct account
            acc = mt5.account_info()
            if acc is None or acc.login != login_id:
                print(f"  └─  Not logged into the correct account. Expected: {login_id}, Found: {acc.login if acc else 'None'}")
                continue
            else:
                print(f"      ✅ Connected to account: {acc.login}")

            acc_info = mt5.account_info()
            if not acc_info:
                print(f"  └─  Failed to get account info")
                continue
                
            balance = acc_info.balance
            print(f"      • Current Balance: ${balance:.2f}")
            
            # NEW LOGIC: If martingale is enabled, USE martingale_risk_management section
            if enable_martingale:
                martingale_risk_map = config.get("martingale_risk_management", {})
                
                if martingale_risk_map:
                    print(f"      📋 USING: Martingale risk management (balance-based)")
                    print(f"      🔍 Finding risk for balance ${balance:.2f}...")
                    
                    # Parse the risk map to find the appropriate risk for current balance
                    for range_str, risk_value in martingale_risk_map.items():
                        try:
                            # Extract the balance range from the key (e.g., "1000-90000.99_risk" -> "1000-90000.99")
                            raw_range = range_str.split("_")[0]
                            low_str, high_str = raw_range.split("-")
                            low = float(low_str)
                            high = float(high_str)
                            
                            if low <= balance <= high:
                                primary_risk = float(risk_value)
                                risk_config_used = f"martingale_risk_management ({range_str})"
                                stats["martingale_active"] = True
                                stats["martingale_max_risk"] = primary_risk
                                print(f"      • Found matching range: {range_str}")
                                print(f"      • Risk limit: ${primary_risk:.2f}")
                                break
                        except Exception as e:
                            print(f"      ⚠️  Error parsing range '{range_str}': {e}")
                            continue
                    
                    if primary_risk is None:
                        print(f"      ⚠️  No risk mapping found for balance ${balance:.2f} in martingale_risk_management")
                        print(f"      Using default risk: $500")
                        primary_risk = 500
                        risk_config_used = "martingale_risk_management (default fallback)"
                        stats["martingale_max_risk"] = primary_risk
                else:
                    print(f"      ⚠️  No martingale_risk_management section found in config")
                    print(f"      Using default risk: $500")
                    primary_risk = 500
                    risk_config_used = "martingale_risk_management (missing section - fallback)"
                    stats["martingale_max_risk"] = primary_risk
                
                stats["martingale_active"] = True
                print(f"      ℹ️  Martingale is enabled - using balance-based martingale risk management")
            
            # If martingale is NOT enabled, use account balance risk management
            else:
                stats["martingale_active"] = False
                
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
                
                # Determine Primary Risk Value based on selected risk map
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

        except Exception as e:
            print(f"  └─  Failed to read config: {e}")
            continue

        print(f"\n  └─ 💰 Target Risk: ${primary_risk:.2f}")
        print(f"  └─ 📋 Risk Source: {risk_config_used}")
        
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
                            print(f"        Cancel failed: {error_msg}")
                    
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
            print(f"       • Risk source: {risk_config_used}")
            if stats["martingale_active"]:
                print(f"       • Martingale Active: ✅ YES (risk limit: ${primary_risk:.2f})")
            else:
                print(f"       • Martingale Active:  NO")
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
    if stats['martingale_active']:
        print(f"   Martingale Active: ✅ YES (using martingale_risk_management)")
        print(f"   Martingale Max Risk: ${stats['martingale_max_risk']:.2f}")
    else:
        print(f"   Martingale Active:  NO (using account balance management)")
    print(f"   Orders checked: {stats['orders_checked']}")
    print(f"   Orders removed: {stats['orders_removed']}")
    print(f"   Orders kept (lower risk): {stats['orders_kept_lower']}")
    print(f"   Orders kept (in tolerance): {stats['orders_kept_in_range']}")
    
    if stats['orders_checked'] > 0:
        removal_rate = (stats['orders_removed'] / stats['orders_checked']) * 100
        print(f"   Removal rate: {removal_rate:.1f}%")
    
    print(f"\n{'='*10} 🏁 RISK AUDIT COMPLETE {'='*10}\n")
    return stats

def orders_risk_correction(inv_id=None):
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
            print(f"  └─  No broker config found")
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
            print(f"  └─  Failed to read config: {e}")
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
                print(f"  └─   login failed: {error}")
                stats["orders_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")
        # --- END EXACT INITIALIZATION COPY ---

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─  Failed to get account info")
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
        print(f"      • AutoTrading: {'✅ ENABLED' if term_info.trade_allowed else ' DISABLED'}")

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
                            print(f"        Modification failed: {error_msg}")
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
                                    print(f"        Fallback modification failed")
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
                        print(f"        Fallback calculation error: {e}")
        
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
                            print(f"        Modification failed: {error_msg}")
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
                                    print(f"        Fallback modification failed")
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
                        print(f"        Fallback calculation error: {e}")

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

def adjust_pending_orders_to_max_risk(inv_id=None):
    """
    Adjust pending orders to match maximum risk configuration.
    
    ONLY ACTIVE WHEN: enable_maximum_account_balance_management = true
    
    Uses the SAME risk calculation method as orders_risk_correction() for consistency.
    
    Args:
        inv_id: Optional specific investor ID to process
        
    Returns:
        dict: Statistics about the adjustments
    """
    print(f"\n{'='*10} 🎯 ADJUST PENDING ORDERS TO MAX RISK {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    # Statistics for this run
    stats = {
        "investor_id": inv_id if inv_id else "all",
        "investors_skipped_due_to_settings": 0,
        "investors_with_max_config": 0,
        "total_symbols_analyzed": 0,
        "symbols_with_liquidator_orders": 0,
        "liquidator_orders_found": 0,
        "pending_orders_found": 0,
        "pending_orders_matched": 0,
        "pending_orders_adjusted": 0,
        "pending_orders_skipped_error": 0,
        "adjustment_failures": 0,
        "orders_increased_risk": 0,
        "orders_decreased_risk": 0,
        "max_risk_values": {},
        "adjustments_made": [],
        "symbol_details": {}
    }
    
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
        print(f"\n[{processed}/{total_investors}] {user_brokerid} 🔍 Checking maximum risk configuration...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─  No broker config found")
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        prices_dir = inv_root / "prices"
        acc_mgmt_path = inv_root / "accountmanagement.json"
        signals_path = prices_dir / "signals.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️  accountmanagement.json not found - skipping")
            stats["investors_skipped_due_to_settings"] += 1
            continue
        
        # =====================================================
        # STEP 1: Check account management settings for maximum config
        # =====================================================
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                acc_config = json.load(f)
            
            # Check if maximum account balance management is enabled
            settings = acc_config.get("settings", {})
            enable_maximum = settings.get("enable_maximum_account_balance_management", False)
            
            if not enable_maximum:
                print(f"  └─ ⏭️ Maximum account balance management is DISABLED")
                stats["investors_skipped_due_to_settings"] += 1
                continue
            
            print(f"  └─ ✅ Maximum account balance management is ENABLED")
            
            # Get maximum risk configuration
            max_risk_config = acc_config.get("account_balance_maximum_risk_management", {})
            if not max_risk_config:
                print(f"  └─ ⚠️ account_balance_maximum_risk_management is empty or missing")
                continue
            
            stats["investors_with_max_config"] += 1
            
        except Exception as e:
            print(f"  └─  Error reading accountmanagement.json: {e}")
            stats["investors_skipped_due_to_settings"] += 1
            continue
        
        # =====================================================
        # STEP 2: Load signals.json to find orders with liquidator flags
        # =====================================================
        if not signals_path.exists():
            print(f"  └─  signals.json not found at {signals_path}")
            continue
        
        try:
            with open(signals_path, 'r', encoding='utf-8') as f:
                signals_data = json.load(f)
            print(f"  └─ 📂 Loaded signals.json")
        except Exception as e:
            print(f"  └─  Failed to load signals.json: {e}")
            continue
        
        # =====================================================
        # STEP 3: Account connection check
        # =====================================================
        print(f"  └─ 🔌 Checking account connection...")
        
        login_id = int(broker_cfg['LOGIN_ID'])
        mt5_path = broker_cfg["TERMINAL_PATH"]
        
        print(f"      • Terminal Path: {mt5_path}")
        print(f"      • Login ID: {login_id}")
        
        # Check if already logged into correct account
        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            print(f"      🔑 Logging into account...")
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                error = mt5.last_error()
                print(f"  └─  Login failed: {error}")
                stats["adjustment_failures"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")
        
        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─  Failed to get account info")
            stats["adjustment_failures"] += 1
            continue
        
        balance = acc_info.balance
        print(f"      • Balance: ${balance:,.2f}")
        
        # Determine maximum risk value based on balance
        max_risk = None
        for range_str, risk_val in max_risk_config.items():
            try:
                raw_range = range_str.split("_")[0]
                low, high = map(float, raw_range.split("-"))
                if low <= balance <= high:
                    max_risk = float(risk_val)
                    break
            except Exception as e:
                print(f"      ⚠️ Error parsing range '{range_str}': {e}")
                continue
        
        if max_risk is None:
            print(f"  └─ ⚠️ No maximum risk mapping for balance ${balance:,.2f}")
            continue
        
        print(f"  └─ 💰 Maximum Risk Target: ${max_risk:.2f}")
        stats["max_risk_values"][user_brokerid] = max_risk
        
        # =====================================================
        # STEP 4: Get all live pending orders
        # =====================================================
        pending_orders = mt5.orders_get()
        if not pending_orders:
            print(f"  └─ 🔘 No pending orders found")
            continue
        
        # Create lookup dictionary for pending orders by ticket number
        pending_by_ticket = {}
        for order in pending_orders:
            if order.type not in ORDER_TYPES.keys():
                continue
            
            pending_by_ticket[order.ticket] = {
                'ticket': order.ticket,
                'type': order.type,
                'type_name': ORDER_TYPES.get(order.type, f"Unknown"),
                'symbol': order.symbol,
                'volume': order.volume_initial,
                'price_open': order.price_open,
                'sl': order.sl,
                'tp': order.tp,
                'comment': order.comment
            }
        
        print(f"  └─ 📊 Found {len(pending_orders)} total pending orders")
        stats["pending_orders_found"] += len(pending_orders)
        
        # =====================================================
        # STEP 5: Process each symbol for liquidator orders
        # =====================================================
        changes_made_to_signals = False
        investor_orders_adjusted = 0
        investor_orders_increased = 0
        investor_orders_decreased = 0
        investor_orders_skipped_error = 0
        investor_liquidator_orders_found = 0
        
        for category_name, category_data in signals_data.get('categories', {}).items():
            symbols_in_category = category_data.get('symbols', {})
            
            for symbol in symbols_in_category:
                symbol_signals = symbols_in_category[symbol]
                stats["total_symbols_analyzed"] += 1
                
                print(f"\n    🔍 Analyzing {symbol} for liquidator orders...")
                
                # Collect all orders with liquidator flags
                liquidator_orders = []
                
                # Check bid_orders
                for idx, order in enumerate(symbol_signals.get('bid_orders', [])):
                    # Check main order
                    if (order.get('first_both_levels_green_liquidator') or 
                        order.get('first_both_levels_red_liquidator')):
                        order_copy = order.copy()
                        order_copy['_location'] = 'bid'
                        order_copy['_is_main'] = True
                        order_copy['_signal_index'] = idx
                        liquidator_orders.append(order_copy)
                    
                    # Check counter order
                    if 'order_counter' in order:
                        counter = order['order_counter']
                        if (counter.get('first_both_levels_green_liquidator') or 
                            counter.get('first_both_levels_red_liquidator')):
                            counter_copy = counter.copy()
                            counter_copy['_location'] = 'bid'
                            counter_copy['_is_main'] = False
                            counter_copy['_parent_entry'] = order.get('entry')
                            counter_copy['_signal_index'] = idx
                            liquidator_orders.append(counter_copy)
                
                # Check ask_orders
                for idx, order in enumerate(symbol_signals.get('ask_orders', [])):
                    # Check main order
                    if (order.get('first_both_levels_green_liquidator') or 
                        order.get('first_both_levels_red_liquidator')):
                        order_copy = order.copy()
                        order_copy['_location'] = 'ask'
                        order_copy['_is_main'] = True
                        order_copy['_signal_index'] = idx
                        liquidator_orders.append(order_copy)
                    
                    # Check counter order
                    if 'order_counter' in order:
                        counter = order['order_counter']
                        if (counter.get('first_both_levels_green_liquidator') or 
                            counter.get('first_both_levels_red_liquidator')):
                            counter_copy = counter.copy()
                            counter_copy['_location'] = 'ask'
                            counter_copy['_is_main'] = False
                            counter_copy['_parent_entry'] = order.get('entry')
                            counter_copy['_signal_index'] = idx
                            liquidator_orders.append(counter_copy)
                
                if not liquidator_orders:
                    print(f"      ⏳ No liquidator orders found for {symbol}")
                    continue
                
                stats["symbols_with_liquidator_orders"] += 1
                investor_liquidator_orders_found += len(liquidator_orders)
                stats["liquidator_orders_found"] += len(liquidator_orders)
                print(f"      🎯 Found {len(liquidator_orders)} liquidator order(s)")
                
                # For each liquidator order, try to find matching pending order
                for sig_order in liquidator_orders:
                    sig_entry = sig_order.get('entry')
                    sig_exit = sig_order.get('exit')
                    sig_order_type = sig_order.get('order_type')
                    sig_volume = sig_order.get('volume')
                    
                    # Skip if missing critical data
                    if not all([sig_entry, sig_exit, sig_order_type, sig_volume]):
                        print(f"        ⚠️ Liquidator order missing critical data - skipping")
                        investor_orders_skipped_error += 1
                        stats["pending_orders_skipped_error"] += 1
                        continue
                    
                    # Determine order direction
                    is_buy = sig_order_type.startswith('buy_')
                    calc_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
                    
                    # =====================================================
                    # STEP 6: Calculate current risk using MT5's order_calc_profit (same as orders_risk_correction)
                    # =====================================================
                    sl_profit = mt5.order_calc_profit(calc_type, symbol, sig_volume, sig_entry, sig_exit)
                    
                    if sl_profit is None:
                        print(f"        ⚠️ Could not calculate risk for order at {sig_entry}")
                        investor_orders_skipped_error += 1
                        stats["pending_orders_skipped_error"] += 1
                        continue
                    
                    current_risk = abs(sl_profit)
                    
                    print(f"\n        📋 Liquidator Order Details:")
                    print(f"          • Type: {sig_order_type}")
                    print(f"          • Entry: {sig_entry}")
                    print(f"          • Exit: {sig_exit}")
                    print(f"          • Volume: {sig_volume}")
                    print(f"          • Current Risk: ${current_risk:.2f}")
                    print(f"          • Target Max Risk: ${max_risk:.2f}")
                    
                    # Calculate risk difference
                    risk_difference = current_risk - max_risk
                    
                    # Determine adjustment direction
                    if abs(risk_difference) < 0.01:  # Within 1 cent, no adjustment needed
                        print(f"          ✅ Risk already exactly at maximum (${current_risk:.2f} = ${max_risk:.2f})")
                        continue
                    
                    if risk_difference < 0:
                        direction = "increase"
                        print(f"          📊 Need to INCREASE risk by: ${abs(risk_difference):.2f}")
                    else:
                        direction = "decrease"
                        print(f"          📊 Need to DECREASE risk by: ${risk_difference:.2f}")
                    
                    # =====================================================
                    # STEP 7: Find matching pending order by properties
                    # =====================================================
                    matching_pending = None
                    
                    # Search through all pending orders for this symbol
                    for ticket, pending in pending_by_ticket.items():
                        if pending['symbol'] != symbol:
                            continue
                        
                        pending_type = pending['type']
                        pending_price = pending['price_open']
                        pending_volume = pending['volume']
                        
                        # Check if order types match
                        type_matches = False
                        if sig_order_type == "buy_stop" and pending_type == mt5.ORDER_TYPE_BUY_STOP:
                            type_matches = True
                        elif sig_order_type == "sell_stop" and pending_type == mt5.ORDER_TYPE_SELL_STOP:
                            type_matches = True
                        elif sig_order_type == "buy_limit" and pending_type == mt5.ORDER_TYPE_BUY_LIMIT:
                            type_matches = True
                        elif sig_order_type == "sell_limit" and pending_type == mt5.ORDER_TYPE_SELL_LIMIT:
                            type_matches = True
                        elif sig_order_type == "buy_stop_limit" and pending_type == mt5.ORDER_TYPE_BUY_STOP_LIMIT:
                            type_matches = True
                        elif sig_order_type == "sell_stop_limit" and pending_type == mt5.ORDER_TYPE_SELL_STOP_LIMIT:
                            type_matches = True
                        
                        if not type_matches:
                            continue
                        
                        # Check if prices match (allow small tolerance based on symbol)
                        symbol_info = mt5.symbol_info(symbol)
                        if symbol_info:
                            tolerance = symbol_info.point * 10  # 10 points tolerance
                            if abs(pending_price - sig_entry) > tolerance:
                                continue
                        else:
                            # Fallback tolerance if can't get symbol info
                            if abs(pending_price - sig_entry) > 0.001:
                                continue
                        
                        # Check if volume matches (allow small tolerance)
                        if abs(pending_volume - sig_volume) > 0.001:
                            continue
                        
                        # Found a match
                        matching_pending = pending
                        break
                    
                    if not matching_pending:
                        print(f"           No matching pending order found for this liquidator order")
                        investor_orders_skipped_error += 1
                        stats["pending_orders_skipped_error"] += 1
                        continue
                    
                    stats["pending_orders_matched"] += 1
                    print(f"          ✅ Found matching pending order #{matching_pending['ticket']}")
                    
                    # =====================================================
                    # STEP 8: Calculate new exit price to achieve EXACT max risk
                    # =====================================================
                    # Get symbol info for price rounding
                    symbol_info = mt5.symbol_info(symbol)
                    if not symbol_info:
                        print(f"          ⚠️ Could not get symbol info for {symbol}")
                        investor_orders_skipped_error += 1
                        stats["pending_orders_skipped_error"] += 1
                        continue
                    
                    digits = symbol_info.digits
                    point = symbol_info.point
                    
                    # Calculate the price movement needed for target risk
                    # We need to find a new exit price where the SL profit = max_risk
                    # This is an iterative process since order_calc_profit is not linear in all cases
                    
                    # Start with a binary search to find the correct price
                    if is_buy:
                        # For BUY: exit price is lower than entry
                        min_exit = point * 10  # Minimum positive price
                        max_exit = sig_entry - point  # Just below entry
                        
                        # Ensure we don't go negative
                        if min_exit > max_exit:
                            min_exit = point * 10
                        
                        # Binary search for the exit price that gives exactly max_risk
                        for _ in range(50):  # Limit iterations
                            test_exit = (min_exit + max_exit) / 2
                            test_risk = mt5.order_calc_profit(calc_type, symbol, sig_volume, sig_entry, test_exit)
                            
                            if test_risk is None:
                                break
                            
                            test_risk_abs = abs(test_risk)
                            
                            if abs(test_risk_abs - max_risk) < 0.01:  # Within 1 cent
                                new_exit = test_exit
                                break
                            elif test_risk_abs > max_risk:
                                # Risk too high, move exit closer to entry (increase exit price)
                                min_exit = test_exit
                            else:
                                # Risk too low, move exit away from entry (decrease exit price)
                                max_exit = test_exit
                        else:
                            # If binary search didn't converge, use linear approximation
                            # Calculate approximate price movement needed
                            if current_risk > 0:
                                # Linear approximation: price_diff_needed = (max_risk / current_risk) * (entry - sig_exit)
                                current_price_diff = abs(sig_entry - sig_exit)
                                target_price_diff = (max_risk / current_risk) * current_price_diff
                                
                                if is_buy:
                                    new_exit = sig_entry - target_price_diff
                                else:
                                    new_exit = sig_entry + target_price_diff
                            else:
                                new_exit = sig_exit
                    
                    else:  # SELL order
                        # For SELL: exit price is higher than entry
                        min_exit = sig_entry + point  # Just above entry
                        max_exit = sig_entry + (10000 * point)  # Far above entry
                        
                        # Binary search for the exit price that gives exactly max_risk
                        for _ in range(50):  # Limit iterations
                            test_exit = (min_exit + max_exit) / 2
                            test_risk = mt5.order_calc_profit(calc_type, symbol, sig_volume, sig_entry, test_exit)
                            
                            if test_risk is None:
                                break
                            
                            test_risk_abs = abs(test_risk)
                            
                            if abs(test_risk_abs - max_risk) < 0.01:  # Within 1 cent
                                new_exit = test_exit
                                break
                            elif test_risk_abs > max_risk:
                                # Risk too high, move exit closer to entry (decrease exit price)
                                max_exit = test_exit
                            else:
                                # Risk too low, move exit away from entry (increase exit price)
                                min_exit = test_exit
                        else:
                            # If binary search didn't converge, use linear approximation
                            if current_risk > 0:
                                current_price_diff = abs(sig_exit - sig_entry)
                                target_price_diff = (max_risk / current_risk) * current_price_diff
                                
                                if is_buy:
                                    new_exit = sig_entry - target_price_diff
                                else:
                                    new_exit = sig_entry + target_price_diff
                            else:
                                new_exit = sig_exit
                    
                    # Round to symbol digits
                    new_exit = round(new_exit, digits)
                    
                    # Verify the new risk calculation
                    new_risk = mt5.order_calc_profit(calc_type, symbol, sig_volume, sig_entry, new_exit)
                    if new_risk is not None:
                        new_risk_abs = abs(new_risk)
                        print(f"          📊 Calculated adjustment:")
                        print(f"            • Current Exit: {sig_exit:.{digits}f}")
                        print(f"            • New Exit: {new_exit:.{digits}f}")
                        print(f"            • Verified New Risk: ${new_risk_abs:.2f}")
                        
                        # Check if we're close enough to target
                        risk_diff = abs(new_risk_abs - max_risk)
                        if risk_diff > 0.10:  # More than 10 cents off
                            print(f"          ⚠️ Calculated risk off by ${risk_diff:.2f} - may need manual adjustment")
                    else:
                        print(f"          ⚠️ Could not verify new risk calculation")
                    
                    # =====================================================
                    # STEP 9: Modify the pending order in MT5
                    # =====================================================
                    modify_request = {
                        "action": mt5.TRADE_ACTION_MODIFY,
                        "order": matching_pending['ticket'],
                        "price": matching_pending['price_open'],  # Keep original entry price
                        "sl": new_exit,
                        "tp": matching_pending['tp']  # Keep original TP
                    }
                    
                    result = mt5.order_send(modify_request)
                    
                    if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                        print(f"          ✅ Order #{matching_pending['ticket']} adjusted successfully")
                        
                        # Track if we increased or decreased risk
                        if risk_difference < 0:
                            investor_orders_increased += 1
                            stats["orders_increased_risk"] += 1
                        else:
                            investor_orders_decreased += 1
                            stats["orders_decreased_risk"] += 1
                        
                        # Update the order in signals.json
                        location = sig_order.get('_location')
                        is_main = sig_order.get('_is_main', True)
                        signal_index = sig_order.get('_signal_index')
                        
                        # Prepare update data
                        update_data = {
                            'exit': new_exit,
                            'risk_in_usd': round(new_risk_abs if new_risk is not None else max_risk, 2),
                            'adjusted_to_max_risk': True,
                            'previous_exit': sig_exit,
                            'previous_risk': round(current_risk, 2),
                            'max_risk_target': max_risk,
                            'adjustment_direction': 'increased' if risk_difference < 0 else 'decreased'
                        }
                        
                        # Apply update based on location
                        if location == 'bid':
                            if is_main:
                                symbol_signals['bid_orders'][signal_index].update(update_data)
                            else:
                                symbol_signals['bid_orders'][signal_index]['order_counter'].update(update_data)
                        elif location == 'ask':
                            if is_main:
                                symbol_signals['ask_orders'][signal_index].update(update_data)
                            else:
                                symbol_signals['ask_orders'][signal_index]['order_counter'].update(update_data)
                        
                        investor_orders_adjusted += 1
                        stats["pending_orders_adjusted"] += 1
                        stats["adjustments_made"].append({
                            "investor": user_brokerid,
                            "symbol": symbol,
                            "ticket": matching_pending['ticket'],
                            "order_type": sig_order_type,
                            "entry": sig_entry,
                            "old_exit": sig_exit,
                            "new_exit": new_exit,
                            "old_risk": round(current_risk, 2),
                            "new_risk": round(new_risk_abs if new_risk is not None else max_risk, 2),
                            "adjustment": "increased" if risk_difference < 0 else "decreased"
                        })
                        changes_made_to_signals = True
                    else:
                        error_msg = result.comment if result else "No response"
                        retcode = result.retcode if result else "No retcode"
                        print(f"           Failed to adjust order: {error_msg} (Code: {retcode})")
                        
                        # Provide more helpful error messages
                        if retcode == 10013:
                            print(f"            ℹ️ This error (Invalid request) often means:")
                            print(f"               • The SL price is too close to current market price")
                            print(f"               • The SL price violates broker minimum distance rules")
                            print(f"               • Try adjusting by smaller increments")
                        stats["adjustment_failures"] += 1
        
        # =====================================================
        # STEP 10: Save updated signals.json if changes were made
        # =====================================================
        if changes_made_to_signals:
            try:
                with open(signals_path, 'w', encoding='utf-8') as f:
                    json.dump(signals_data, f, indent=4)
                print(f"\n  └─ ✅ Updated signals.json with adjusted orders")
            except Exception as e:
                print(f"\n  └─  Failed to save signals.json: {e}")
        
        # Investor summary
        print(f"\n  └─ 📊 Results for {user_brokerid}:")
        print(f"     • Max risk target: ${max_risk:.2f}")
        print(f"     • Liquidator orders found: {investor_liquidator_orders_found}")
        print(f"     • Matched with pending: {stats['pending_orders_matched']}")
        print(f"     • Adjusted to max: {investor_orders_adjusted}")
        if investor_orders_increased > 0:
            print(f"       └─ 🔼 Increased risk (was below max): {investor_orders_increased}")
        if investor_orders_decreased > 0:
            print(f"       └─ 🔽 Decreased risk (was above max): {investor_orders_decreased}")
        print(f"     • Adjustment failures: {stats['adjustment_failures']}")
        print(f"     • Skipped (errors): {investor_orders_skipped_error}")
    
    # =====================================================
    # FINAL SUMMARY
    # =====================================================
    print(f"\n{'='*10} 📊 MAX RISK ADJUSTMENT SUMMARY {'='*10}")
    print(f"   Investor ID: {stats['investor_id']}")
    print(f"   Investors with max config enabled: {stats['investors_with_max_config']}")
    print(f"   Investors skipped (config disabled): {stats['investors_skipped_due_to_settings']}")
    print(f"   Symbols analyzed: {stats['total_symbols_analyzed']}")
    print(f"   Symbols with liquidator orders: {stats['symbols_with_liquidator_orders']}")
    print(f"   Liquidator orders found: {stats['liquidator_orders_found']}")
    print(f"   Pending orders found: {stats['pending_orders_found']}")
    print(f"   Pending orders matched: {stats['pending_orders_matched']}")
    print(f"   Pending orders adjusted: {stats['pending_orders_adjusted']}")
    if stats['orders_increased_risk'] > 0:
        print(f"     └─ 🔼 Increased risk (was below max): {stats['orders_increased_risk']}")
    if stats['orders_decreased_risk'] > 0:
        print(f"     └─ 🔽 Decreased risk (was above max): {stats['orders_decreased_risk']}")
    print(f"   Adjustment failures: {stats['adjustment_failures']}")
    print(f"   Pending orders skipped (errors): {stats['pending_orders_skipped_error']}")
    
    if stats['adjustments_made']:
        print(f"\n   📋 Adjustments made (last 5):")
        for adj in stats['adjustments_made'][-5:]:
            arrow = "🔼" if adj['adjustment'] == "increased" else "🔽"
            print(f"     • {adj['investor']} - {adj['symbol']} #{adj['ticket']} {arrow}")
            print(f"       {adj['order_type']} @ {adj['entry']}")
            print(f"       Exit: {adj['old_exit']} → {adj['new_exit']}")
            print(f"       Risk: ${adj['old_risk']:.2f} → ${adj['new_risk']:.2f}")
        if len(stats['adjustments_made']) > 5:
            print(f"       ... and {len(stats['adjustments_made']) - 5} more")
    
    print(f"\n{'='*10} 🏁 MAX RISK ADJUSTMENT COMPLETE {'='*10}\n")
    return stats  

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
            print(f"  └─  No broker config found")
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
            print(f"  └─  Failed to read config: {e}")
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
                print(f"  └─  login failed: {error}")
                stats["positions_error"] += 1
                continue
            print(f"      ✅ Successfully logged into account")
        else:
            print(f"      ✅ Already logged into account")

        acc_info = mt5.account_info()
        if not acc_info:
            print(f"  └─  Failed to get account info")
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
                # risk_distance should always be positive
                if is_buy:
                    risk_distance = position.price_open - position.sl
                else:  # SELL
                    risk_distance = abs(position.sl - position.price_open)
                
                # Ensure risk_distance is positive
                if risk_distance <= 0:
                    print(f"       ⚠️  Invalid risk distance (SL at or beyond entry). Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue
                
                risk_points = risk_distance / symbol_info.point
                
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

                # Validate risk_usd is positive
                if risk_usd <= 0:
                    print(f"       ⚠️  Invalid risk value: ${risk_usd:.2f}. Skipping.")
                    investor_positions_skipped += 1
                    stats["positions_skipped"] += 1
                    continue

                # Calculate current profit in R multiples
                current_profit_usd = position.profit
                current_r_multiple = current_profit_usd / risk_usd

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
                # target_reward can be positive (move SL into profit) or zero (breakeven)
                if target_reward >= 0:
                    # Calculate target SL price
                    if is_buy:
                        # For BUY: entry + (risk_distance * target_reward)
                        # When target_reward = 0: SL at entry (breakeven)
                        # When target_reward = 0.5: SL at entry + 0.5R (partial profit protection)
                        target_sl_price = position.price_open + (risk_distance * target_reward)
                    else:  # SELL
                        # For SELL: entry - (risk_distance * target_reward)
                        # When target_reward = 0: SL at entry (breakeven)
                        # When target_reward = 0.5: SL at entry - 0.5R (partial profit protection)
                        target_sl_price = position.price_open - (risk_distance * target_reward)
                    
                    # Round to symbol digits
                    digits = symbol_info.digits
                    target_sl_price = round(target_sl_price, digits)
                    
                    print(f"       Current SL: {position.sl:.{digits}f}")
                    print(f"       Target SL:  {target_sl_price:.{digits}f} ({target_reward}R)")
                    
                    # Check if SL needs adjustment
                    # Calculate the distance from entry to current SL and target SL
                    if is_buy:
                        current_sl_distance = position.price_open - position.sl if position.sl != 0 else 0
                        target_sl_distance = position.price_open - target_sl_price
                        
                        # For BUY: A better SL is higher (closer to entry or above entry for profit)
                        # Check if target SL would actually improve the position
                        if target_sl_price <= position.sl:
                            print(f"       ℹ️  Target SL would not improve protection (not moving higher). Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                        
                    else:  # SELL
                        current_sl_distance = position.sl - position.price_open if position.sl != 0 else 0
                        target_sl_distance = target_sl_price - position.price_open
                        
                        # For SELL: A better SL is lower (closer to entry or below entry for profit)
                        # Check if target SL would actually improve the position
                        if target_sl_price >= position.sl:
                            print(f"       ℹ️  Target SL would not improve protection (not moving lower). Skipping.")
                            investor_positions_skipped += 1
                            stats["positions_skipped"] += 1
                            continue
                    
                    # Calculate threshold for adjustment (10% of target distance or 2 pips)
                    pip_threshold = max(abs(target_sl_distance) * 0.1, symbol_info.point * 20)
                    
                    # Check if SL needs adjustment (if current SL is significantly different from target)
                    if abs(current_sl_distance - target_sl_distance) > pip_threshold:
                        print(f"       📐 SL needs adjustment (distance difference: {abs(current_sl_distance - target_sl_distance):.{digits}f})")
                        
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
                            print(f"        Modification failed: {error_msg}")
                    else:
                        print(f"       ✅ SL already at optimal level (within threshold)")
                        investor_positions_skipped += 1
                        stats["positions_skipped"] += 1
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

def update_investor_info(inv_id=None):
    """
    Updates investor information in UPDATED_INVESTORS.json including:
    - Balance at execution start date
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
        print(f"\n📋 INVESTOR: {user_brokerid} Current Info")
        print("-" * 60)
        
        if user_brokerid not in usersdictionary:
            print(f"   Investor {user_brokerid} not found in usersdictionary")
            continue
            
        base_info = usersdictionary[user_brokerid].copy()
        inv_root = Path(INV_PATH) / user_brokerid
        
        if not inv_root.exists():
            print(f"   Path not found: {inv_root}")
            continue
        
        # Initialize aggregated data
        total_authorized_pnl = 0.0
        authorized_closed_trades_list = []
        won_trades = 0
        lost_trades = 0
        symbols_lost = {}
        symbols_won = {}
        execution_start_date = None
        starting_balance = None
        unauthorized_detected = False
        bypass_active = False
        autotrading_active = False
        unauthorized_type = set()
        unauthorized_trades_list = []
        unauthorized_withdrawals_list = []
        authorized_tickets = set()
        
        # Look for activities.json directly in investor root folder
        activities_path = inv_root / "activities.json"
        if activities_path.exists():
            try:
                with open(activities_path, 'r', encoding='utf-8') as f:
                    activities = json.load(f)
                
                # Check authorization status using same logic as place_usd_orders
                unauthorized_detected = activities.get('unauthorized_action_detected', False)
                bypass_active = activities.get('bypass_restriction', False)
                autotrading_active = activities.get('activate_autotrading', False)
                
                if unauthorized_detected:
                    unauthorized_trades = activities.get('unauthorized_trades', {})
                    if unauthorized_trades:
                        unauthorized_type.add('trades')
                        for ticket_key, trade in unauthorized_trades.items():
                            unauthorized_trades_list.append({
                                'ticket': trade.get('ticket'),
                                'symbol': trade.get('symbol'),
                                'type': trade.get('type'),
                                'volume': trade.get('volume'),
                                'profit': round(float(trade.get('profit', 0)), 2),
                                'time': trade.get('time'),
                                'reason': trade.get('reason')
                            })
                    unauthorized_withdrawals = activities.get('unauthorized_withdrawals', {})
                    if unauthorized_withdrawals:
                        unauthorized_type.add('withdrawal')
                        for wd_key, withdrawal in unauthorized_withdrawals.items():
                            unauthorized_withdrawals_list.append({
                                'ticket': withdrawal.get('ticket'),
                                'amount': withdrawal.get('amount'),
                                'time': withdrawal.get('time'),
                                'comment': withdrawal.get('comment')
                            })
                
                execution_start_date = activities.get('execution_start_date')
                print(f"    📋 Found activities.json with execution_start_date: {execution_start_date}")
            except Exception as e:
                print(f"    ⚠️  Error reading activities.json: {e}")
        else:
            print(f"    ⚠️  activities.json not found in {inv_root}")
        
        # Look for tradeshistory.json directly in investor root folder
        history_path = inv_root / "tradeshistory.json"
        if history_path.exists():
            try:
                with open(history_path, 'r', encoding='utf-8') as f:
                    authorized_trades = json.load(f)
                for trade in authorized_trades:
                    if 'ticket' in trade and trade['ticket']:
                        authorized_tickets.add(int(trade['ticket']))
                print(f"    📋 Found {len(authorized_tickets)} authorized tickets in tradeshistory.json")
            except Exception as e:
                print(f"    ⚠️  Error reading tradeshistory.json: {e}")
        else:
            print(f"    ⚠️  tradeshistory.json not found in {inv_root}")

        # Fallback to accountmanagement.json if execution_start_date not found
        if not execution_start_date:
            acc_mgmt_path = inv_root / "accountmanagement.json"
            if acc_mgmt_path.exists():
                try:
                    with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                        acc_mgmt = json.load(f)
                    execution_start_date = acc_mgmt.get('execution_start_date')
                    print(f"    📋 Found execution_start_date in accountmanagement.json: {execution_start_date}")
                except: pass
        
        if execution_start_date:
            try:
                start_datetime = None
                for date_format in ["%B %d, %Y", "%Y-%m-%d"]:
                    try:
                        start_datetime = datetime.strptime(execution_start_date, date_format)
                        start_datetime = start_datetime.replace(hour=0, minute=0, second=0)
                        break
                    except: continue
                
                if start_datetime:
                    print(f"    🔍 Looking for trades from: {start_datetime.strftime('%Y-%m-%d')}")
                    all_deals = mt5.history_deals_get(start_datetime, datetime.now())
                    
                    if all_deals and len(all_deals) > 0:
                        all_deals = sorted(list(all_deals), key=lambda x: x.time)
                        total_profit_all_trades = 0
                        
                        for deal in all_deals:
                            if deal.type in [0, 1]:  # 0=BUY, 1=SELL
                                total_profit_all_trades += deal.profit
                                symbol = deal.symbol if hasattr(deal, 'symbol') else 'Unknown'
                                
                                # Process ONLY authorized trades for the summary and stats
                                if deal.ticket in authorized_tickets:
                                    total_authorized_pnl += deal.profit
                                    
                                    authorized_closed_trades_list.append({
                                        'ticket': deal.ticket,
                                        'symbol': symbol,
                                        'type': 'BUY' if deal.type == 0 else 'SELL',
                                        'volume': deal.volume,
                                        'profit': round(deal.profit, 2),
                                        'time': datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d %H:%M:%S')
                                    })
                                    
                                    if deal.profit > 0:
                                        won_trades += 1
                                        symbols_won[symbol] = symbols_won.get(symbol, 0.0) + deal.profit
                                    elif deal.profit < 0:
                                        lost_trades += 1
                                        # Keep the negative sign for losses in summary
                                        symbols_lost[symbol] = symbols_lost.get(symbol, 0.0) + deal.profit
                                else:
                                    # Process as unauthorized
                                    ticket_exists = any(t.get('ticket') == deal.ticket for t in unauthorized_trades_list)
                                    if not ticket_exists:
                                        unauthorized_trades_list.append({
                                            'ticket': deal.ticket,
                                            'symbol': symbol,
                                            'type': 'BUY' if deal.type == 0 else 'SELL',
                                            'volume': deal.volume,
                                            'profit': round(deal.profit, 2),
                                            'time': datetime.fromtimestamp(deal.time).strftime('%Y-%m-%d %H:%M:%S'),
                                            'reason': f"Trade NOT in tradeshistory.json (Ticket: {deal.ticket})"
                                        })
                                        if 'trades' not in unauthorized_type: unauthorized_type.add('trades')
                                        unauthorized_detected = True
                        
                        account_info = mt5.account_info()
                        if account_info:
                            starting_balance = account_info.balance - total_profit_all_trades
                            print(f"    ✅ Calculated starting balance: ${starting_balance:.2f}")
                            print(f"       Current balance: ${account_info.balance:.2f}")
                            print(f"       Total profits all trades: ${total_profit_all_trades:.2f}")
                            print(f"       Authorized trades P&L: ${total_authorized_pnl:.2f}")
                    else:
                        account_info = mt5.account_info()
                        if account_info:
                            starting_balance = account_info.balance
                            print(f"    ✅ No trades since start, using current balance: ${starting_balance:.2f}")
            except Exception as e:
                print(f"    ⚠️  Error getting starting balance: {e}")

        # Build Structured Trades Dict with Negative signs preserved
        trades_info = {
            "summary": {
                "total_trades": len(authorized_closed_trades_list),
                "won": won_trades,
                "lost": lost_trades,
                "symbols_that_lost": {k: round(v, 2) for k, v in symbols_lost.items()},
                "symbols_that_won": {k: round(v, 2) for k, v in symbols_won.items()}
            },
            "authorized_closed_trades": authorized_closed_trades_list
        }

        # Contract days logic
        contract_days_left = "30"
        if execution_start_date:
            try:
                start = None
                for fmt in ["%Y-%m-%d", "%B %d, %Y"]:
                    try: 
                        start = datetime.strptime(execution_start_date, fmt)
                        break
                    except: continue
                if start:
                    days_passed = (datetime.now() - start).days
                    contract_days_left = str(max(0, 30 - days_passed))
            except: pass

        investor_info = {
            "id": user_brokerid,
            "server": base_info.get("SERVER", base_info.get("server", "")),
            "login": base_info.get("LOGIN_ID", base_info.get("login", "")),
            "password": base_info.get("PASSWORD", base_info.get("password", "")),
            "application_status": base_info.get("application_status", "pending"),
            "broker_balance": round(starting_balance, 2) if starting_balance is not None else None,
            "profitandloss": round(total_authorized_pnl, 2),
            "contract_days_left": contract_days_left,
            "execution_start_date": execution_start_date if execution_start_date else "",
            "trades": trades_info,
            "unauthorized_actions": {
                "detected": unauthorized_detected,
                "bypass_active": bypass_active,
                "autotrading_active": autotrading_active,
                "type": list(unauthorized_type) if unauthorized_type else [],
                "unauthorized_trades": unauthorized_trades_list,
                "unauthorized_withdrawals": unauthorized_withdrawals_list
            }
        }
        
        # --- CRITICAL: Check if investor should be moved to issues ---
        # Using the EXACT same logic as place_usd_orders:
        # From place_usd_orders:
        # if unauthorized_detected:
        #     if bypass_active:
        #         # proceed with order placement
        #     else:
        #         # block orders
        #
        # Therefore:
        # - If unauthorized_detected AND bypass_active → keep in updated_investors
        # - If unauthorized_detected AND NOT bypass_active → move to issues_investors
        
        should_move_to_issues = False
        issue_message = ""
        
        if unauthorized_detected:
            if bypass_active:
                # Bypass active - keep in updated investors (same as place_usd_orders allowing orders)
                print(f"  ⚠️  Unauthorized actions detected but BYPASS ACTIVE - keeping in updated_investors.json")
                should_move_to_issues = False
            else:
                # No bypass - move to issues (same as place_usd_orders blocking orders)
                should_move_to_issues = True
                issue_message = "Unauthorized action detected - restricted (bypass inactive)"
        
        if should_move_to_issues:
            print(f"  ⛔ Investor has unauthorized actions without bypass - MOVING TO ISSUES INVESTORS")
            print(f"      Message: {issue_message}")
            
            # Add message to investor info
            investor_info['MESSAGE'] = issue_message
            
            # Remove from updated_investors if exists
            if user_brokerid in updated_investors:
                del updated_investors[user_brokerid]
            
            # Add to issues_investors
            issues_investors[user_brokerid] = investor_info
            
        else:
            # Investor is clean or has bypass - add to updated investors
            # Set application_status to "approved" for investors in updated_investors
            investor_info['application_status'] = "approved"
            
            print(f"\n  📊 INVESTOR SUMMARY (added to updated_investors.json with status: APPROVED):")
            print(f"    • Starting Balance: ${investor_info['broker_balance'] if investor_info['broker_balance'] else 0.0:.2f}")
            print(f"    • Authorized P&L: ${investor_info['profitandloss']:.2f}")
            print(f"    • Authorized Trade Stats: {won_trades} Won / {lost_trades} Lost")
            print(f"    • Unauthorized: {'YES (BYPASS ACTIVE)' if unauthorized_detected else 'NO'}")
            print(f"    • Application Status: {investor_info['application_status']}")
            
            updated_investors[user_brokerid] = investor_info

    # Save updated_investors.json
    try:
        with open(updated_investors_path, 'w', encoding='utf-8') as f:
            json.dump(updated_investors, f, indent=4)
    except Exception as e:
        print(f"\n Failed to save updated_investors.json: {e}")
    
    # Save issues_investors.json
    try:
        with open(issues_investors_path, 'w', encoding='utf-8') as f:
            json.dump(issues_investors, f, indent=4)
    except Exception as e:
        print(f"\n Failed to save issues_investors.json: {e}")
    
    print("\n" + "="*80)
    print("✅ INVESTOR INFORMATION UPDATE COMPLETE")
    print("="*80)
    
    return updated_investors

def place_instant_stop_orders(inv_id=None):
    """
    Places buy/sell stop orders at the minimum allowed distance from market price.
    """
    print(f"\n{'='*10} ⚡ INSTANT STOP ORDERS {'='*10}")
    if inv_id:
        print(f" Processing single investor: {inv_id}")
    
    stats = {
        "investors_processed": 0,
        "investors_skipped": 0,
        "buy_stops_placed": 0,
        "sell_stops_placed": 0,
        "orders_failed": 0
    }
    
    # Process investors
    investors_to_process = [inv_id] if inv_id else usersdictionary.keys()
    
    for user_brokerid in investors_to_process:
        print(f"\n[{user_brokerid}] Processing...")
        
        # Get broker config
        broker_cfg = usersdictionary.get(user_brokerid)
        if not broker_cfg:
            print(f"  └─  No broker config")
            stats["investors_skipped"] += 1
            continue
        
        inv_root = Path(INV_PATH) / user_brokerid
        acc_mgmt_path = inv_root / "accountmanagement.json"
        
        if not acc_mgmt_path.exists():
            print(f"  └─ ⚠️ No accountmanagement.json")
            stats["investors_skipped"] += 1
            continue
        
        # Load config
        try:
            with open(acc_mgmt_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            symbols_dict = config.get("symbols_dictionary", {})
            all_symbols = []
            for category, symbols in symbols_dict.items():
                all_symbols.extend(symbols)
            
            selected_rr = config.get("selected_risk_reward", [2])
            risk_reward = float(selected_rr[0]) if selected_rr else 2
            
        except Exception as e:
            print(f"  └─  Error: {e}")
            stats["investors_skipped"] += 1
            continue
        
        # Check account
        login_id = int(broker_cfg['LOGIN_ID'])
        acc = mt5.account_info()
        
        if acc is None or acc.login != login_id:
            print(f"  └─  Wrong account")
            stats["investors_skipped"] += 1
            continue
        
        magic_number = int(user_brokerid) if user_brokerid.isdigit() else 0
        
        # Process each symbol
        for raw_symbol in all_symbols:
            print(f"\n    {raw_symbol}:")
            
            # Normalize symbol
            normalized_symbol = get_normalized_symbol(raw_symbol)
            
            # Get symbol info
            symbol_info = mt5.symbol_info(normalized_symbol)
            if not symbol_info:
                print(f"       Cannot get symbol info")
                stats["orders_failed"] += 1
                continue
            
            # Select symbol
            if not mt5.symbol_select(normalized_symbol, True):
                print(f"       Cannot select symbol")
                stats["orders_failed"] += 1
                continue
            
            # Get tick
            tick = mt5.symbol_info_tick(normalized_symbol)
            if not tick:
                print(f"       No tick data")
                stats["orders_failed"] += 1
                continue
            
            # Get minimum allowed distance
            stops_level = getattr(symbol_info, 'trade_stops_level', 0)
            if stops_level <= 0:
                stops_level = getattr(symbol_info, 'freeze_level', 100)
            
            # Use the minimum distance (but cap at reasonable values if needed)
            min_distance_points = stops_level
            point = symbol_info.point
            digits = symbol_info.digits
            
            # Calculate entry prices at minimum distance
            buy_stop_price = round(tick.ask + (min_distance_points * point), digits)
            sell_stop_price = round(tick.bid - (min_distance_points * point), digits)
            
            print(f"      Distance: {min_distance_points} points")
            print(f"      BUY STOP: {buy_stop_price}")
            print(f"      SELL STOP: {sell_stop_price}")
            
            # Delete existing orders for this symbol
            orders = mt5.orders_get(symbol=normalized_symbol)
            if orders:
                for order in orders:
                    if order.magic == magic_number:
                        mt5.order_send({
                            "action": mt5.TRADE_ACTION_REMOVE,
                            "order": order.ticket
                        })
            
            # Use minimum volume
            volume = symbol_info.volume_min
            
            # Calculate SL (15 points or minimum distance, whichever is larger)
            sl_distance = max(15, min_distance_points)
            
            # Place BUY STOP
            buy_sl = round(buy_stop_price - (sl_distance * point), digits)
            buy_tp = round(buy_stop_price + ((buy_stop_price - buy_sl) * risk_reward), digits)
            
            buy_request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": normalized_symbol,
                "volume": float(volume),
                "type": mt5.ORDER_TYPE_BUY_STOP,
                "price": float(buy_stop_price),
                "sl": float(buy_sl),
                "tp": float(buy_tp),
                "deviation": 20,
                "magic": magic_number,
                "comment": "INSTANT_BUY",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_RETURN,
            }
            
            result = mt5.order_send(buy_request)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"      ✅ BUY STOP placed")
                stats["buy_stops_placed"] += 1
            else:
                print(f"       BUY STOP failed: {result.comment if result else 'Unknown'}")
                stats["orders_failed"] += 1
            
            # Place SELL STOP
            sell_sl = round(sell_stop_price + (sl_distance * point), digits)
            sell_tp = round(sell_stop_price - ((sell_sl - sell_stop_price) * risk_reward), digits)
            
            sell_request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": normalized_symbol,
                "volume": float(volume),
                "type": mt5.ORDER_TYPE_SELL_STOP,
                "price": float(sell_stop_price),
                "sl": float(sell_sl),
                "tp": float(sell_tp),
                "deviation": 20,
                "magic": magic_number,
                "comment": "INSTANT_SELL",
                "type_time": mt5.ORDER_TIME_GTC,
                "type_filling": mt5.ORDER_FILLING_RETURN,
            }
            
            result = mt5.order_send(sell_request)
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                print(f"      ✅ SELL STOP placed")
                stats["sell_stops_placed"] += 1
            else:
                print(f"       SELL STOP failed: {result.comment if result else 'Unknown'}")
                stats["orders_failed"] += 1
        
        stats["investors_processed"] += 1
    
    # Simple summary
    print(f"\n{'='*10} SUMMARY {'='*10}")
    print(f"BUY STOPs: {stats['buy_stops_placed']}")
    print(f"SELL STOPs: {stats['sell_stops_placed']}")
    print(f"Failed: {stats['orders_failed']}")
    
    return stats

def process_single_invest(inv_folder):
    """
    WORKER FUNCTION: Handles the entire pipeline for ONE investor.
    Sequential execution without console output.
    """
    inv_id = inv_folder.name
    
    account_stats = {
        "inv_id": inv_id, 
        "success": False, 
        "price_collection_stats": {},
        "candle_fetch_stats": {},
        "crosser_analysis_stats": {},
        "trapped_analysis_stats": {},
        "liquidator_analysis_stats": {},
        "ranging_analysis_stats": {},
        "order_placement_stats": {},
        "risk_correction_stats": {},
        "risk_audit_stats": {},
        "symbols_filtered": 0,
        "orders_filtered": 0,
        "symbols_processed": 0,
        "symbols_successful": 0,
        "orders_placed": 0,
        "counter_orders_placed": 0,
        "total_active_orders": 0,
        "orders_adjusted": 0,
        "orders_removed": 0,
        "current_candle_forming": False,
        "bid_wins": 0,
        "ask_wins": 0,
        "trapped_candles_found": 0,
        "symbols_with_trapped": 0,
        "symbols_with_liquidator": 0,
        "liquidator_candles_found": 0,
        "bullish_liquidators": 0,
        "bearish_liquidators": 0,
        "symbols_ranging": 0,
        "avg_ranging_cycles": 0
    }
    
    broker_cfg = usersdictionary.get(inv_id)
    if not broker_cfg:
        return account_stats

    import random
    import time
    time.sleep(random.uniform(0.1, 2.0)) 
    
    login_id = int(broker_cfg['LOGIN_ID'])
    mt5_path = broker_cfg["TERMINAL_PATH"]

    try:
        if not mt5.initialize(path=mt5_path, timeout=180000):
            return account_stats

        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                mt5.shutdown()
                return account_stats
            
        #timeframe_countdown(inv_id=inv_id)

        # STEP 0: SYMBOL AUTHORIZATION FILTER
        martingale(inv_id=inv_id)
        
        
        mt5.shutdown()
        account_stats["success"] = True
        
    except Exception as e:
        try:
            mt5.shutdown()
        except:
            pass
    
    return account_stats

def process_single_investor(inv_folder):
    """
    WORKER FUNCTION: Handles the entire pipeline for ONE investor.
    Sequential execution without console output.
    """
    inv_id = inv_folder.name
    
    account_stats = {
        "inv_id": inv_id, 
        "success": False, 
        "price_collection_stats": {},
        "candle_fetch_stats": {},
        "crosser_analysis_stats": {},
        "trapped_analysis_stats": {},
        "liquidator_analysis_stats": {},
        "ranging_analysis_stats": {},
        "order_placement_stats": {},
        "risk_correction_stats": {},
        "risk_audit_stats": {},
        "symbols_filtered": 0,
        "orders_filtered": 0,
        "symbols_processed": 0,
        "symbols_successful": 0,
        "orders_placed": 0,
        "counter_orders_placed": 0,
        "total_active_orders": 0,
        "orders_adjusted": 0,
        "orders_removed": 0,
        "current_candle_forming": False,
        "bid_wins": 0,
        "ask_wins": 0,
        "trapped_candles_found": 0,
        "symbols_with_trapped": 0,
        "symbols_with_liquidator": 0,
        "liquidator_candles_found": 0,
        "bullish_liquidators": 0,
        "bearish_liquidators": 0,
        "symbols_ranging": 0,
        "avg_ranging_cycles": 0
    }
    
    broker_cfg = usersdictionary.get(inv_id)
    if not broker_cfg:
        return account_stats

    import random
    import time
    time.sleep(random.uniform(0.1, 2.0)) 
    
    login_id = int(broker_cfg['LOGIN_ID'])
    mt5_path = broker_cfg["TERMINAL_PATH"]

    try:
        if not mt5.initialize(path=mt5_path, timeout=180000):
            return account_stats

        acc = mt5.account_info()
        if acc is None or acc.login != login_id:
            if not mt5.login(login_id, password=broker_cfg["PASSWORD"], server=broker_cfg["SERVER"]):
                mt5.shutdown()
                return account_stats
            
        #timeframe_countdown(inv_id=inv_id)

        # STEP 0: SYMBOL AUTHORIZATION FILTER
        filter_stats = filter_unauthorized_symbols(inv_id=inv_id)
        account_stats["symbols_filtered"] = filter_stats.get("symbols_filtered", 0)
        account_stats["orders_filtered"] = filter_stats.get("orders_filtered", 0)

        # STEP 1: PRICE COLLECTION
        price_stats = symbols_grid_prices(inv_id=inv_id)
        account_stats["price_collection_stats"] = price_stats
        account_stats["symbols_processed"] = price_stats.get("total_symbols", 0)
        account_stats["symbols_successful"] = price_stats.get("successful_symbols", 0)
        
        # STEP 1.5: FETCH 15-MINUTE CANDLES
        candle_stats = fetch_15m_candles(inv_id=inv_id)
        account_stats["candle_fetch_stats"] = candle_stats
        account_stats["current_candle_forming"] = candle_stats.get("current_candle_forming", False)
        
        # STEP 1.6: IDENTIFY FIRST CROSSER CANDLE
        crosser_stats = identify_first_crosser_candle(inv_id=inv_id)
        account_stats["crosser_analysis_stats"] = crosser_stats
        account_stats["bid_wins"] = crosser_stats.get("bid_wins", 0)
        account_stats["ask_wins"] = crosser_stats.get("ask_wins", 0)
        
        # STEP 1.7: IDENTIFY TRAPPED CANDLES
        trapped_stats = identify_trapped_candles(inv_id=inv_id)
        account_stats["trapped_analysis_stats"] = trapped_stats
        account_stats["trapped_candles_found"] = trapped_stats.get("total_trapped_candles_found", 0)
        account_stats["symbols_with_trapped"] = trapped_stats.get("symbols_with_trapped_candles", 0)
        
        # STEP 1.8: IDENTIFY LEVELS LIQUIDATOR CANDLE
        liquidator_stats = identify_levels_liquidator_candle(inv_id=inv_id)
        account_stats["liquidator_analysis_stats"] = liquidator_stats
        account_stats["symbols_with_liquidator"] = liquidator_stats.get("symbols_with_liquidator", 0)
        account_stats["liquidator_candles_found"] = liquidator_stats.get("symbols_with_liquidator", 0)
        account_stats["bullish_liquidators"] = liquidator_stats.get("liquidator_candle_stats", {}).get("green_candles", 0)
        account_stats["bearish_liquidators"] = liquidator_stats.get("liquidator_candle_stats", {}).get("red_candles", 0)
        
        # STEP 1.9: IDENTIFY RANGING ORDERS CANDLES
        ranging_stats = identify_ranging_orders_candles(inv_id=inv_id)
        account_stats["ranging_analysis_stats"] = ranging_stats
        account_stats["symbols_ranging"] = ranging_stats.get("symbols_ranging", 0)
        account_stats["avg_ranging_cycles"] = ranging_stats.get("ranging_stats", {}).get("avg_cycle_count", 0)


        # STEP 1.6.5: FLAG ORDERS WITHOUT CROSSER CANDLE
        account_stats["orders_config_stats"] = orders_configuration(inv_id=inv_id)

        # STEP 1.6.5: LIQUIDATOR LEVELS
        account_stats["liquidator_config_stats"] = liquidator_configuration(inv_id=inv_id)

        

        # STEP 2: ORDER PLACEMENT
        order_stats = manage_single_position_and_pending(inv_id=inv_id)
        martingale(inv_id=inv_id)
        order_stats = place_signals_orders_accounts(inv_id=inv_id)
        order_stats = manage_single_position_and_pending(inv_id=inv_id)
        apply_dynamic_breakeven(inv_id=inv_id)
        account_stats["order_placement_stats"] = order_stats
        account_stats["orders_placed"] = order_stats.get("orders_placed", 0)
        account_stats["counter_orders_placed"] = order_stats.get("counter_orders_placed", 0)
        account_stats["total_active_orders"] = order_stats.get("total_active_orders", 0)

        adjust_pending_orders_to_max_risk(inv_id=inv_id)

        # STEP 4: RISK AUDIT
        audit_stats = check_pending_orders_risk(inv_id=inv_id)
        account_stats["risk_audit_stats"] = audit_stats
        account_stats["orders_removed"] = audit_stats.get("orders_removed", 0)

        correction_stats = orders_risk_correction(inv_id=inv_id)
        account_stats["risk_correction_stats"] = correction_stats
        account_stats["orders_adjusted"] = correction_stats.get("orders_adjusted", 0)
        
        mt5.shutdown()
        account_stats["success"] = True
        
    except Exception as e:
        try:
            mt5.shutdown()
        except:
            pass
    
    return account_stats

def place_grid_orders_parallel():
    """
    ORCHESTRATOR: Spawns multiple processes to handle  investors in parallel.
    Uses the  account initialization logic.
    """
    inv_base_path = Path(INV_PATH)
    investor_folders = [f for f in inv_base_path.iterdir() if f.is_dir()]
    
    if not investor_folders:
        print(" └─ 🔘 No investor directories found.")
        return False

    print(f" 📋 Found {len(investor_folders)} investors to process")
    print(f" 🔧 Creating pool with {len(investor_folders)} processes...")
    
    # Create a pool based on the number of accounts
    # This will run 'process_single_investor' for all folders at the same time
    with mp.Pool(processes=len(investor_folders)) as pool:
        results = pool.map(process_single_investor, investor_folders)

    #time.sleep(1)
    #place_grid_orders_parallel()
    
    return 

if __name__ == "__main__":
    place_grid_orders_parallel()

         