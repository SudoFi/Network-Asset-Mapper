import os 
import sys
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from bs4 import BeautifulSoup 
import ipaddress
from tqdm import tqdm
import questionary 
import getpass
import subprocess
import re

import api 

# --- Pathing and Folder Setup ---
BASE_DIR = Path(__file__).resolve().parent.parent 
FOLDERS = ["Data", "Import", "Logs", "Output"]
for folder in FOLDERS:
    (BASE_DIR / folder).mkdir(parents=True, exist_ok=True)

IMPORT_DIR = BASE_DIR / "Import"
OUTPUT_DIR = BASE_DIR / "Output"
TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
ACTIVITY_LOG_FILE = BASE_DIR / "Logs" / f"main_activity_log_{TIMESTAMP}.txt"
ERROR_LOG_FILE = BASE_DIR / "Logs" / f"main_error_log_{TIMESTAMP}.txt"

def clear_console():
    """Clears the terminal screen."""
    if os.name == 'nt':
        _ = os.system('cls')
    else:
        _ = os.system('clear')

def log_activity(message, console_only=False):
    """Prints a timestamped message to console and writes to the activity log."""
    log_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [MAIN] {message}"
    print(log_message, flush=True)
    if not console_only:
        with open(ACTIVITY_LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(log_message + "\n")

def log_error(msg):
    """Logs an error message to console and the dedicated error log."""
    log_activity(f"ERROR: {msg}")
    with open(ERROR_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.now()}] [MAIN] - {msg}\n")

def load_config():
    """Loads configuration from the config.json file in the root directory."""
    config_path = BASE_DIR / "config.json"
    if not config_path.exists():
        print(f"FATAL: Configuration file not found at {config_path}") 
        sys.exit(1) 
    with open(config_path, 'r') as f:
        config_data = json.load(f)
    log_activity(f"Configuration loaded from {config_path}")
    return config_data

# Global config variables, initialized in main()
config, DEPARTMENT_MAPPING, DEPARTMENT_HEADS, SCAN_SETTINGS, AXONIUS_API_CONFIG, AD_CONFIG, SCRIPT_SETTINGS = {}, {}, {}, {}, {}, {}, {}

def get_department(ip_str, mapping):
    """Finds a department for an IP by checking against subnet mappings in the config."""
    try:
        if not isinstance(ip_str, str) or not ip_str: return "Unassigned"
        ip = ipaddress.ip_address(ip_str)
        best_match = ("Unassigned", -1)
        for subnet_str, dept in mapping.items():
            if dept == "Unassigned": continue
            try:
                network = ipaddress.ip_network(subnet_str)
                if ip in network and network.prefixlen > best_match[1]:
                    best_match = (dept, network.prefixlen)
            except ValueError: continue
        return best_match[0]
    except ValueError: return "Invalid IP"

def import_files():
    """Imports all .csv and .xlsx files from the Import directory."""
    log_activity("Attempting to import local files...")
    imported = {}
    files_found = list(IMPORT_DIR.glob("*"))
    if not files_found:
        log_activity("No files found in 'Import' directory.")
        return imported
        
    for file in files_found:
        try:
            log_activity(f"Importing {file.name}...")
            if file.suffix == ".csv": df = pd.read_csv(file, low_memory=False)
            elif file.suffix in [".xls", ".xlsx"]: df = pd.read_excel(file)
            else: continue
            
            imported[file.stem.lower()] = df
            log_activity(f"✓ Successfully loaded data from {file.name} as '{file.stem.lower()}'")
        except Exception as e: 
            log_error(f"Failed to import or process {file.name}: {e}")
    return imported

def consolidate_data(sources, user_ad_data, axonius_users_df):
    """Consolidates data from all sources, enriches with user info, and standardizes columns."""
    log_activity("--- Consolidating all collected data ---")
    all_rows = []

    FIELD_MAP = {
        "specific_data.data.unique_id": "Asset_Unique_ID", "specific_data.data.hostname": "Hostname",
        "specific_data.data.last_seen": "Last_Seen_Device",
        "specific_data.data.last_used_users_ad_display_name_association": "User",
        "network_interfaces": "network_interfaces_obj",
        "specific_data.data.network_interfaces.ips": "_Raw_IPs_List",
        "specific_data.data.last_used_users_departments_association": "Source_User_Department",
        "Asset Unique ID": "Asset_Unique_ID", "Host Name": "Hostname",
        "Last Seen": "Last_Seen_Device", "Last Used Users AD Display Name": "User",
        "Network Interfaces: IPs": "_Raw_IPs_List",
        "Last Used Users Departments": "Source_User_Department",
    }
    
    user_info_df = pd.DataFrame()
    if 'user_ad_data' in sources and not sources['user_ad_data'].empty:
        log_activity("Preparing Active Directory user data for enrichment...")
        user_ad_data = sources['user_ad_data']
        user_info_df = user_ad_data.rename(columns={
            "User Display Name": "Primary_Username_For_Linking",
            "User Department": "Department_AD", "User Manager": "User_Manager_AD",
            "User Email": "Mail_AD", "User First Name": "First_Name_AD", "User Last Name": "Last_Name_AD"
        })
        user_info_df['Primary_Username_For_Linking'] = user_info_df['Primary_Username_For_Linking'].astype(str)
        user_info_df = user_info_df.drop_duplicates('Primary_Username_For_Linking')
    elif not axonius_users_df.empty:
        log_activity("AD data not found, preparing Axonius user data for enrichment...")
        user_info_df = axonius_users_df.rename(columns={
            "specific_data.data.username": "Primary_Username_For_Linking",
            "specific_data.data.first_name": "First_Name_AD", "specific_data.data.last_name": "Last_Name_AD",
            "specific_data.data.mail": "Mail_AD", "specific_data.data.user_manager": "User_Manager_AD"
        })
        user_info_df["Primary_Username_For_Linking"] = user_info_df["Primary_Username_For_Linking"].astype(str).str.split('\\').str[-1]
        user_info_df = user_info_df.drop_duplicates('Primary_Username_For_Linking')

    for source_name_original, df_source_original in sources.items():
        if df_source_original.empty or source_name_original in ['user_ad_data', 'Axonius_Users_RAW']: continue
        log_activity(f"Processing device data from source: {source_name_original}")
        df = df_source_original.copy()
        
        renamed_cols = {col: FIELD_MAP.get(col.replace("Aggregated: ", "").strip(), FIELD_MAP.get(col, col)) for col in df.columns}
        df = df.rename(columns=renamed_cols)

        def normalize_ips(row):
            interfaces = row.get('network_interfaces_obj'); raw_ips = row.get('_Raw_IPs_List')
            if isinstance(interfaces, list):
                all_ips = [ip for i in interfaces if isinstance(i.get('ips'), list) for ip in i['ips']]
                return list(set(all_ips))
            if isinstance(raw_ips, list): return list(set(raw_ips))
            if isinstance(raw_ips, str):
                return list(set([ip.strip() for part in raw_ips.split('||') for ip in part.split(',') if ip.strip()]))
            return []
        
        df['_IP_List'] = df.apply(normalize_ips, axis=1)
        df = df.explode('_IP_List').rename(columns={'_IP_List': 'IP Address'})
        df.dropna(subset=['IP Address'], inplace=True)

        for _, row in df.iterrows():
            ip_str = str(row.get("IP Address", "")).strip()
            if not ip_str: continue

            department_from_source = row.get("Source_User_Department")
            department = ""
            if isinstance(department_from_source, list) and department_from_source:
                department = " || ".join(str(d).strip() for d in department_from_source if pd.notna(d))
            elif pd.notna(department_from_source): department = str(department_from_source)
            
            username_source = row.get("User", "")
            user_list = []
            if isinstance(username_source, list): user_list = [str(u).strip() for u in username_source if pd.notna(u)]
            elif isinstance(username_source, str) and username_source: user_list = [u.strip() for u in username_source.split('||') if u.strip()]
            
            full_user_string = " || ".join(user_list)
            primary_user_for_link = user_list[0] if user_list else ""
            
            last_seen_val = row.get("Last_Seen_Device")
            formatted_last_seen = ''
            if pd.notna(last_seen_val):
                dt_obj = pd.to_datetime(last_seen_val, errors='coerce')
                if pd.notna(dt_obj): formatted_last_seen = dt_obj.strftime('%Y-%m-%d %H:%M:%S')

            all_rows.append({
                "Asset_Unique_ID": row.get("Asset_Unique_ID"), "IP Address": ip_str,
                "Hostname": row.get("Hostname"), "Last_Seen_Device": formatted_last_seen,
                "User": full_user_string,
                "Primary_Username_For_Linking": primary_user_for_link.split('\\')[-1] if primary_user_for_link else "",
                "Source": source_name_original, "Department_From_Source": department
            })

    if not all_rows:
        log_activity("No device data to consolidate."); return pd.DataFrame()
    devices_df = pd.DataFrame(all_rows)

    if not user_info_df.empty:
        log_activity("Enriching device data with user details...")
        final_df = pd.merge(devices_df, user_info_df, on="Primary_Username_For_Linking", how="left")
    else:
        final_df = devices_df
        for col in ["User_Full_Name_AD", "Department_AD", "User_Manager_AD", "Mail_AD", "First_Name_AD", "Last_Name_AD"]:
            if col not in final_df.columns: final_df[col] = ""

    log_activity("Assigning final department...")
    final_df['Department'] = final_df['Department_AD'].fillna(final_df['Department_From_Source'])
    final_df['Department'] = final_df['Department'].fillna(final_df['IP Address'].apply(lambda ip: get_department(ip, DEPARTMENT_MAPPING)))
    
    def derive_email(row, domain):
        if pd.notna(row.get("Mail_AD")) and row.get("Mail_AD") != "": return row["Mail_AD"]
        first_name = str(row.get("First_Name_AD", "")); last_name = str(row.get("Last_Name_AD", ""))
        if first_name and last_name:
            first_initial = first_name[0].lower()
            processed_last_name = "".join(last_name.split()).lower()
            return f"{first_initial}{processed_last_name}@{domain}"
        return ""

    default_domain = SCAN_SETTINGS.get("default_email_domain")
    if default_domain:
        log_activity(f"Deriving missing email addresses with default domain: {default_domain}")
        final_df["Mail"] = final_df.apply(lambda row: derive_email(row, default_domain), axis=1)
    else:
        final_df["Mail"] = final_df["Mail_AD"]

    log_activity("Assigning department heads...")
    final_df["Department Head"] = final_df["Department"].apply(lambda x: DEPARTMENT_HEADS.get(str(x).split('||')[0].strip(), "N/A"))
    final_df = final_df.fillna("")
    
    cols_to_keep = [
        "Asset_Unique_ID", "Department", "Department Head", "IP Address", "Hostname", 
        "Last_Seen_Device", "User", "Mail", "User_Manager_AD", "Source"
    ]
    for col in cols_to_keep:
        if col not in final_df.columns: final_df[col] = ""
    final_df = final_df.rename(columns={"User_Manager_AD": "User Manager Name"})
            
    log_activity("Dropping duplicate entries...")
    try:
        initial_rows = len(final_df)
        final_df = final_df[cols_to_keep].drop_duplicates()
        final_rows = len(final_df)
        log_activity(f"Dropped {initial_rows - final_rows} duplicate rows.")
    except MemoryError:
        log_error("A MemoryError occurred while dropping duplicates. The dataset may be too large.")
        log_activity("Proceeding with data that includes duplicates.")
    except Exception as e:
        log_error(f"An unexpected error occurred during drop_duplicates: {e}")

    log_activity("Sorting final data...")
    final_df = final_df.sort_values(by=["Department", "IP Address"])
    log_activity(f"Consolidated {len(final_df)} total IP-per-row entries.")
    return final_df

def generate_dept_summary_df(device_df):
    """Generates a pivot table matrix of Subnets vs Departments with IP counts."""
    log_activity("Generating Department Subnet Summary matrix...")
    if device_df.empty:
        log_activity("No device data to generate department summary from.")
        return pd.DataFrame()
    df = device_df.copy()
    df.dropna(subset=['IP Address', 'Department'], inplace=True)
    df = df[df['Department'] != 'Unassigned']

    def get_subnet_24(ip):
        try: return str(ipaddress.ip_network(f"{ip}/24", strict=False))
        except ValueError: return None
            
    df['Inferred_Subnet'] = df['IP Address'].apply(get_subnet_24)
    df.dropna(subset=['Inferred_Subnet'], inplace=True)
    if df.empty:
        log_activity("No valid subnets could be inferred from device IPs.")
        return pd.DataFrame()

    log_activity("Creating pivot table for department subnet counts...")
    pivot_df = pd.pivot_table(
        df, values='IP Address', index='Inferred_Subnet', columns='Department',
        aggfunc='nunique', fill_value=0
    )
    pivot_df['Total'] = pivot_df.sum(axis=1)
    pivot_df.reset_index(inplace=True)
    log_activity("Department Subnet Summary matrix generated.")
    return pivot_df

def generate_reverse_lookup_df(device_df):
    """Generates a summary of all IPs and Hostnames associated with each unique user."""
    log_activity("Generating User Reverse-Lookup Summary...")
    if device_df.empty or "User" not in device_df.columns:
        log_activity("Not enough data to generate user reverse-lookup.")
        return pd.DataFrame()
    df = device_df[['User', 'IP Address', 'Hostname']].copy()
    df.dropna(subset=['User'], inplace=True); df = df[df['User'] != '']
    df_exploded = df.assign(User=df['User'].str.split(' || ')).explode('User')
    df_exploded['User'] = df_exploded['User'].str.strip()
    df_exploded = df_exploded[df_exploded['User'] != '']
    if df_exploded.empty:
        log_activity("No valid user associations found for reverse-lookup.")
        return pd.DataFrame()
    def agg_unique_to_str(series): return ", ".join(series.dropna().astype(str).unique())
    log_activity("Grouping assets by user for reverse-lookup...")
    reverse_lookup_df = df_exploded.groupby('User').agg(
        Associated_IPs=('IP Address', agg_unique_to_str),
        Associated_Hostnames=('Hostname', agg_unique_to_str)
    ).reset_index()
    log_activity("User Reverse-Lookup Summary generated.")
    return reverse_lookup_df

def main():
    """Main function to orchestrate the data gathering and consolidation process."""
    global config, DEPARTMENT_MAPPING, DEPARTMENT_HEADS, SCAN_SETTINGS, AXONIUS_API_CONFIG, AD_CONFIG, SCRIPT_SETTINGS
    clear_console()
    log_activity("--- Script Start ---")
    config = load_config()
    DEPARTMENT_MAPPING = config.get("department_mapping", {}); DEPARTMENT_HEADS = config.get("department_heads", {})
    SCAN_SETTINGS = config.get("scan_settings", {}); AXONIUS_API_CONFIG = config.get("axonius_api", {}); 
    AD_CONFIG = config.get("ad_config", {}); SCRIPT_SETTINGS = config.get("script_settings", {}) 

    available_tasks = {
        "Active Directory Data": "AD_Pull", 
        "Axonius Device Data": "Axonius_Devices", 
        "Axonius User Data": "Axonius_Users",
        "Import Files": "Local_Files"
    }
    
    use_interactive_menu = SCRIPT_SETTINGS.get("use_interactive_menu", True)
    
    if use_interactive_menu:
        log_activity("Displaying interactive task menu...")
        selected_task_names = questionary.checkbox("Select tasks to run:", choices=list(available_tasks.keys())).ask()
        clear_console()
    else:
        log_activity("Interactive menu is disabled. Running default tasks from config.json.")
        selected_task_names = SCRIPT_SETTINGS.get("default_tasks_to_run", list(available_tasks.keys()))

    if not selected_task_names: log_activity("No tasks selected or defined to run. Exiting."); return
    log_activity(f"Selected tasks: {', '.join(selected_task_names)}")
    
    sources = {}; axonius_users_data = pd.DataFrame(); user_ad_data = pd.DataFrame()
    if "Active Directory Data" in selected_task_names:
        log_activity(f"--- Running: Active Directory Data ---")
        try:
            ps_script_path = BASE_DIR / "Scripts" / "ad.ps1"
            if not ps_script_path.exists():
                log_error(f"PowerShell script not found at: {ps_script_path}")
            else:
                log_activity("Launching PowerShell script for AD user pull. Please follow prompts in that window.")
                command = ['powershell.exe', '-ExecutionPolicy', 'Bypass', '-File', str(ps_script_path)]
                process = subprocess.Popen(command, creationflags=subprocess.CREATE_NEW_CONSOLE)
                process.wait()
                log_activity("PowerShell script finished.")
                ad_output_path = IMPORT_DIR / "user_ad_data.csv"
                if ad_output_path.exists():
                    log_activity(f"AD data CSV found. Loading it for this session.")
                    user_ad_data = pd.read_csv(ad_output_path)
                    sources['user_ad_data'] = user_ad_data
        except Exception as e: log_error(f"Failed while running the Active Directory PowerShell script: {e}")

    if 'user_ad_data' not in sources and Path(IMPORT_DIR / "user_ad_data.csv").exists():
        log_activity("Found existing user_ad_data.csv, loading it...")
        user_ad_data = pd.read_csv(Path(IMPORT_DIR / "user_ad_data.csv"))
        sources['user_ad_data'] = user_ad_data

    if "Axonius Device Data" in selected_task_names:
        log_activity(f"--- Running: Axonius Device Data ---")
        df = api.fetch_axonius_assets(AXONIUS_API_CONFIG)
        if not df.empty: sources['Axonius_Devices'] = df
    if "Axonius User Data" in selected_task_names:
        log_activity(f"--- Running: Axonius User Data ---")
        axonius_users_data = api.fetch_axonius_users(AXONIUS_API_CONFIG) 
        if not axonius_users_data.empty: sources['Axonius_Users_RAW'] = axonius_users_data
    if "Import Files" in selected_task_names:
        log_activity(f"--- Running: Import Files ---")
        local_files_data = import_files()
        if local_files_data: sources.update(local_files_data) 
    
    device_sources = {k: v for k, v in sources.items() if k not in ['user_ad_data', 'Axonius_Users_RAW']}
    if not device_sources and user_ad_data.empty and axonius_users_data.empty:
        log_activity("No data collected from any selected tasks. Exiting."); return
    log_activity("--- All data collection tasks complete. ---")
    
    final_device_df = consolidate_data(sources, user_ad_data, axonius_users_data) 
    dept_summary_df = generate_dept_summary_df(final_device_df)
    reverse_lookup_df = generate_reverse_lookup_df(final_device_df)

    has_consolidated_data = not final_device_df.empty; has_dept_summary = not dept_summary_df.empty
    has_reverse_lookup = not reverse_lookup_df.empty
    has_any_raw_data = any(isinstance(df_val, pd.DataFrame) and not df_val.empty for df_val in sources.values())

    if has_consolidated_data or has_any_raw_data:
        output_path = OUTPUT_DIR / "Consolidated_Network_Data_by_Dept.xlsx"
        log_activity(f"Writing final report to {output_path}...")
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            if has_consolidated_data: final_device_df.to_excel(writer, sheet_name="All_Device_Data", index=False)
            if has_dept_summary: dept_summary_df.to_excel(writer, sheet_name="Dept_Subnet_Counts", index=False)
            if has_reverse_lookup: reverse_lookup_df.to_excel(writer, sheet_name="User_Reverse_Lookup", index=False)
            for name, df_source in sources.items():
                if not df_source.empty:
                    df_source.to_excel(writer, sheet_name=f"RAW_{name}"[:31], index=False)
        log_activity(f"✓ Final report created successfully.")
    else:
        log_activity("No data was generated from any selected source, skipping report creation.")
    log_activity("--- Script End ---")
    
    if '--pause-on-exit' in sys.argv:
        print("\nScript has finished. Press any key to exit.")
        os.system('pause >nul')

if __name__ == "__main__":
    main()