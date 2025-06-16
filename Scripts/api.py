import requests
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

# --- Configuration for Logging ---
BASE_DIR_API = Path(__file__).resolve().parent.parent 
LOGS_DIR_API = BASE_DIR_API / "Logs"
LOGS_DIR_API.mkdir(parents=True, exist_ok=True)
TIMESTAMP_API = datetime.now().strftime('%Y%m%d_%H%M%S')
ACTIVITY_LOG_FILE_API = LOGS_DIR_API / f"api_activity_log_{TIMESTAMP_API}.txt"
ERROR_LOG_FILE_API = LOGS_DIR_API / f"api_error_log_{TIMESTAMP_API}.txt"

# --- Logging Helpers for this module ---
def log_api_activity(message):
    log_message = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [API] {message}"
    print(log_message, flush=True)
    with open(ACTIVITY_LOG_FILE_API, 'a', encoding='utf-8') as f:
        f.write(log_message + "\n")

def log_api_error(msg):
    log_api_activity(f"ERROR: {msg}")
    with open(ERROR_LOG_FILE_API, 'a', encoding='utf-8') as f:
        f.write(f"[{datetime.now()}] [API] - {msg}\n")

# --- API Fetching Functions ---
def fetch_axonius_assets(axonius_api_config):
    log_api_activity("Fetching asset data from Axonius...")
    if not all([axonius_api_config.get("api_url"), axonius_api_config.get("api_key"), axonius_api_config.get("api_secret")]):
        log_api_activity("Axonius API configuration for devices is missing. Skipping.")
        return pd.DataFrame()
    
    api_url = f"{axonius_api_config['api_url'].rstrip('/')}/api/devices"
    headers = {"api-key": axonius_api_config["api_key"], "api-secret": axonius_api_config["api_secret"]}
    
    all_assets_attributes = []
    page_offset = 0
    page_limit = 100 

    while True:
        payload = {
          "data": {
            "type": "entity_request_schema",
            "attributes": {
              "filter": '(("adapters_data.rapid7_nexpose_adapter.last_seen" >= date("now-30d")))',
              "page": {"offset": page_offset, "limit": page_limit},
              "fields": {
                  "devices": [
                    "specific_data.data.unique_id", "specific_data.data.hostname", "specific_data.data.last_seen",
                    "specific_data.data.name", "specific_data.data.last_used_users_ad_display_name_association",
                    "labels", "specific_data.data.os.type", "adapters", "network_interfaces", 
                    "specific_data.data.network_interfaces.mac", "specific_data.data.network_interfaces.ips",
                    "specific_data.data.public_ips", "specific_data.data.last_used_users",
                    "specific_data.data.last_used_users_departments_association", "specific_data.data.last_used_users_mail_association",
                    "specific_data.data.last_used_users_user_manager_association", "specific_data.data.last_used_users_user_manager_mail_association",
                    "specific_data.data.connection_label", "specific_data.data.adapter_properties", "specific_data.data.tags",
                    "specific_data.data.connected_devices.local_ifaces.ips", "specific_data.data.connected_devices.remote_ifaces.ips",
                    "specific_data.data.direct_connected_devices.local_ifaces.ips", "specific_data.data.direct_connected_devices.remote_ifaces.ips"
                  ]
              }
            }
          }
        }
        try:
            log_api_activity(f"Requesting Axonius devices (Page offset: {page_offset}) from {api_url}")
            response = requests.post(api_url, headers=headers, json=payload, timeout=120) 
            response.raise_for_status()
            
            assets_data = response.json().get("data", [])
            if not assets_data:
                log_api_activity("No more device assets returned or empty page.")
                break 
            
            current_page_attributes = [asset['attributes'] for asset in assets_data]
            all_assets_attributes.extend(current_page_attributes)
            
            if len(current_page_attributes) < page_limit:
                log_api_activity("Last page of device assets reached.")
                break 
            
            page_offset += page_limit
        except Exception as e:
            log_api_error(f"Axonius device API call failed (Page offset: {page_offset}): {e}")
            return pd.DataFrame(all_assets_attributes) 

    if not all_assets_attributes:
        log_api_activity("Axonius API call successful, but no device assets were returned.")
        return pd.DataFrame()

    df = pd.json_normalize(all_assets_attributes)
    log_api_activity(f"Successfully retrieved and parsed {len(df)} device assets from Axonius.")
    return df

def fetch_axonius_users(axonius_api_config):
    log_api_activity("Fetching user data from Axonius...")
    if not all([axonius_api_config.get("api_url"), axonius_api_config.get("api_key"), axonius_api_config.get("api_secret")]):
        log_api_activity("Axonius API configuration for users is missing. Skipping.")
        return pd.DataFrame()

    api_url = f"{axonius_api_config['api_url'].rstrip('/')}/api/users"
    headers = {"api-key": axonius_api_config["api_key"], "api-secret": axonius_api_config["api_secret"]}
    
    all_users_attributes = []
    page_offset = 0
    page_limit = 100 

    while True:
        payload = {
            "data": {
                "type": "entity_request_schema",
                "attributes": {
                    "page": {"offset": page_offset, "limit": page_limit},
                    "fields": { 
                        "users": [
                            "adapters", "specific_data.data.username", "specific_data.data.domain",
                            "specific_data.data.first_name", "specific_data.data.last_name", "specific_data.data.mail",
                            "specific_data.data.last_seen", "labels", "specific_data.data.user_manager",
                            "specific_data.data.user_department", "specific_data.data.connection_label"
                        ]
                    }
                }
            }
        }
        try:
            log_api_activity(f"Requesting Axonius users (Page offset: {page_offset}) from {api_url}")
            response = requests.post(api_url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            
            users_data = response.json().get("data", [])
            if not users_data:
                log_api_activity("No more users returned or empty page.")
                break 
            
            current_page_attributes = [user['attributes'] for user in users_data]
            all_users_attributes.extend(current_page_attributes)
            
            if len(current_page_attributes) < page_limit:
                log_api_activity("Last page of users reached.")
                break
            page_offset += page_limit
        except Exception as e:
            log_api_error(f"Axonius user API call failed (Page offset: {page_offset}): {e}")
            return pd.DataFrame(all_users_attributes)

    if not all_users_attributes:
        log_api_activity("Axonius API call successful, but no users were returned.")
        return pd.DataFrame()

    df = pd.json_normalize(all_users_attributes)
    log_api_activity(f"Successfully retrieved and parsed {len(df)} users from Axonius.")
    return df