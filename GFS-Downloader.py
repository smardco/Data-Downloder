#!/usr/bin/env python3
"""
A script to automatically download Global Forecast System (GFS) weather data.

This script reads the configuration from a YAML file, determines the latest available GFS run cycle, creates the download URLs, and downloads the files in parallel to speed up the process.
"""
import requests
import yaml
import logging
import concurrent.futures
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, parse_qs, urlencode
from pathlib import Path
from typing import Dict, List, Any, Tuple

# Setting your path to config file here
CONFIG_FILENAME = "configGFS.yaml"

# Set up the log file
def setup_logging(config: Dict[str, Any]) -> None:
    log_date = datetime.now().strftime('%Y%m%d')
    log_config = config.get('logging', {})
    base_log_path = Path(log_config.get('file', 'gfs_download.log'))
    log_stem = base_log_path.stem 
    log_suffix = base_log_path.suffix
    new_filename = f"{log_stem}_{log_date}{log_suffix}"
    log_file = base_log_path.parent / new_filename
    log_level_str = log_config.get('level', 'INFO').upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logging.getLogger().handlers = []
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y/%m/%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler()
        ]
    )
    logging.info("Logger configured.")


# Funtion to set the cycle (00, 06, 12, 18 UTC)
def get_latest_cycle_hour(config: Dict[str, Any], now: datetime = None) -> Tuple[datetime.date, str]:
    gfs_config = config.get('gfs_api', {})
    delay_hours = gfs_config.get('availability_delay_hours', 4)       #4 it stand for 4 hour delay
    if now is None:
        now = datetime.now(timezone.utc) - timedelta(hours=delay_hours)
    hour = now.hour
    available_cycles = [0, 6, 12, 18]
    selected_cycle = max([h for h in available_cycles if h <= hour])
    return now.date(), f"{selected_cycle:02d}"

# Generate a list of forecast hours (e.g., '000', '003', '006')
def generate_forecast_hour_list(config: Dict[str, Any]) -> List[str]:
    forecast_config = config.get('forecast', {})
    max_hour = forecast_config.get('max_hour', 48)
    step = forecast_config.get('step', 1)
    return [f"{i:03d}" for i in range(0, max_hour + 1, step)]

# Generate a complete download URL based on parameters
def generate_auto_gfs_links(config: Dict[str, Any], date_obj: datetime.date, cycle_hour: str, forecast_hours: List[str]) -> List[str]:
    date_compact = date_obj.strftime("%Y%m%d")
    api_config = config.get('gfs_api', {})
    base_url = api_config.get('base_url')
    variables = api_config.get('variables', [])
    levels = api_config.get('level', [])
    region = api_config.get('region', {})

    var_query = "".join([f"&var_{v}=on" for v in variables])
    lev_query = "".join([f"&lev_{l}=on" for l in levels])
    region_query = "&" + urlencode(region) if region else ""
    
    links = []
    for fh in forecast_hours:
        file_param = f"gfs.t{cycle_hour}z.pgrb2.0p25.f{fh}"
        dir_param = f"/gfs.{date_compact}/{cycle_hour}/atmos"
        link = f"{base_url}?file={file_param}&dir={dir_param}{var_query}{lev_query}{region_query}&subregion="
        links.append(link)
    return links

# Generate file name from the URL.
def extract_filename_from_url(url: str) -> str | None:
    try:
        query = parse_qs(urlparse(url).query)
        return query.get("file", [None])[0]
    except (IndexError, AttributeError):
        return None

# Downloads a single file from a URL and saves it.
def download_file(url: str, save_path: Path, timeout: int, chunk_size: int) -> Tuple[str, str]:
    filename = save_path.name
    try:
        response = requests.get(url, stream=True, timeout=timeout)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    f.write(chunk)
            if save_path.stat().st_size > 0:
                return "SUCCESS", f"Downloaded: {filename}"
            else:
                return "WARNING", f"Downloaded but empty: {filename}"
        else:
            return "FAILED", f"Status {response.status_code} for {filename}"
    except requests.exceptions.RequestException as e:
        return "ERROR", f"Request failed for {filename} ({e})"

# Verifying the completeness and integrity of the downloaded file.
def verify_downloaded_files(target_folder: Path, expected_filenames: List[str]) -> None:
    logging.info("--- Starting Verification ---")
    if not target_folder.is_dir():
        logging.error(f"Verification failed: Target directory not found at {target_folder}")
        return

    actual_files = {f.name for f in target_folder.iterdir()}
    expected_files = set(expected_filenames)
    
    missing_files = expected_files - actual_files
    if not missing_files:
        logging.info("Completeness Check: PASSED. All expected files are present.")
    else:
        logging.error(f"Completeness Check: FAILED. Found {len(missing_files)} missing file(s).")
        for filename in sorted(list(missing_files)):
            logging.error(f"  - MISSING: {filename}")

    empty_files_found = 0
    for filename in sorted(list(actual_files)):
        file_path = target_folder / filename
        try:
            if file_path.stat().st_size == 0:
                logging.warning(f"Integrity Check: FAILED. File is empty: {filename}")
                empty_files_found += 1
        except OSError as e:
            logging.error(f"Could not check size of {filename}: {e}")

    if empty_files_found == 0:
        logging.info("Integrity Check: PASSED. No empty files found.")
    else:
        logging.warning(f"Integrity Check: Found {empty_files_found} empty file(s).")
    logging.info("--- Verification Finished ---")

#The main orchestrator for downloading GFS data in parallel.
def download_gfs_data_concurrently(config: Dict[str, Any]) -> None:
    default_config = config.get('default', {})
    download_config = config.get('download', {})
    
    model_name = default_config.get('model_name', 'gfs')
    loc = default_config.get('loc', 'unknown')
    base_folder = Path(default_config.get('base_folder', 'downloaded_data'))
    max_workers = download_config.get('max_workers', 5)
    timeout = download_config.get('timeout', 60)
    chunk_size = download_config.get('chunk_size', 8192)

    date_obj, cycle_hour = get_latest_cycle_hour(config)
    date_sls = date_obj.strftime("%Y/%m/%d")
    logging.info(f"Starting GFS download for date: {date_obj.strftime('%Y-%m-%d')}, cycle: {cycle_hour}Z")

    forecast_hours = generate_forecast_hour_list(config)
    links = generate_auto_gfs_links(config, date_obj, cycle_hour, forecast_hours)
    
    if not links:
        logging.error("No download links were generated. Exiting.")
        return

    expected_filenames = [fn for fn in (extract_filename_from_url(url) for url in links) if fn]
    target_folder = base_folder / model_name / loc / date_sls / cycle_hour
    target_folder.mkdir(parents=True, exist_ok=True)
    
    tasks_to_run = []
    for url in links:
        filename = extract_filename_from_url(url)
        if not filename:
            logging.warning(f"Skipping URL (filename not found): {url}")
            continue
        
        save_path = target_folder / filename
        if save_path.exists() and save_path.stat().st_size > 0:
            logging.info(f"Skipping existing file: {filename}")
        else:
            tasks_to_run.append({'url': url, 'save_path': save_path})
            
    if not tasks_to_run:
        logging.info("No new downloads needed. All files are up to date.")
    else:
        total_tasks = len(tasks_to_run)
        logging.info(f"Found {total_tasks} new or incomplete files to download.")
        logging.info(f"Starting concurrent download with {max_workers} workers.")
        
        completed_count = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_task = {executor.submit(download_file, task['url'], task['save_path'], timeout, chunk_size): task for task in tasks_to_run}
            for future in concurrent.futures.as_completed(future_to_task):
                completed_count += 1
                status, message = future.result()
                log_func = logging.info if status == "SUCCESS" else logging.warning
                log_func(f"({completed_count}/{total_tasks}) {message}")

    logging.info("Download process finished.")
    verify_downloaded_files(target_folder, expected_filenames)

# The main funtion
def main() -> None:
    try:
        with open(CONFIG_FILENAME, 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"CRITICAL: Configuration file '{CONFIG_FILENAME}' not found. Exiting.")
        return
    except yaml.YAMLError as e:
        print(f"CRITICAL: Failed to parse YAML configuration file: {e}. Exiting.")
        return

    setup_logging(config)
    try:
        download_gfs_data_concurrently(config)
    except Exception as e:
        logging.critical(f"A critical error occurred in the main process: {e}", exc_info=True)

if __name__ == "__main__":
    main()
