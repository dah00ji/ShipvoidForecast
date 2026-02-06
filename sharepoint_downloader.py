#!/usr/bin/env python3
"""
SharePoint File Downloader for Shipvoid Forecast

Downloads the newest Shipvoid Forecast and Legacy Unbilled Carton files
from the SharePoint/Teams folder for DC 6031.

Dependencies:
    uv pip install msal requests --index-url https://pypi.ci.artifacts.walmart.com/artifactory/api/pypi/external-pypi/simple --allow-insecure-host pypi.ci.artifacts.walmart.com
"""

import fnmatch
from datetime import datetime
from pathlib import Path
from typing import Optional

import msal
import requests


# SharePoint Site Configuration for DC 6031
SITE_ID = "teams.wal-mart.com,325387a1-0544-4b80-9300-a6f286277aec,9e991740-8e18-4cec-961c-48f33e3e4a53"
DRIVE_ID = "b!oYdTMkQFgEuTAKbyhid67EAXmZ4YjuxMlhxI8z4-SlMPeioAITEgQK3oT5WiuC2r"
FOLDER_PATH = "Shipvoid Forecast/6031"

# Azure AD Configuration - use Walmart's tenant
TENANT_ID = "wal-mart.com"
# Default public client ID for device code flow (replace if you have a specific app)
CLIENT_ID = "04b07795-8ddb-461a-bbee-02f9e1bf7b46"  # Azure CLI public client ID

# File patterns to download
SHIPVOID_PATTERN = "Shipvoid*.xlsm"  # Excel format with Inhouse and Crossdock sheets
LEGACY_PATTERN = "Legacy*.csv"


class SharePointDownloader:
    """Downloads files from SharePoint/Teams using Microsoft Graph API."""
    
    def __init__(
        self,
        client_id: str = CLIENT_ID,
        tenant_id: str = TENANT_ID,
        site_id: str = SITE_ID,
        drive_id: str = DRIVE_ID,
    ):
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.site_id = site_id
        self.drive_id = drive_id
        self.scopes = ["Files.Read.All", "Sites.Read.All"]
        self.access_token: Optional[str] = None
        self.graph_base_url = "https://graph.microsoft.com/v1.0"
        self.token_cache_file = Path.home() / ".sharepoint_token_cache.json"
    
    def _get_msal_app(self) -> tuple:
        """Create MSAL public client application with token cache."""
        cache = msal.SerializableTokenCache()
        
        if self.token_cache_file.exists():
            cache.deserialize(self.token_cache_file.read_text())
        
        app = msal.PublicClientApplication(
            self.client_id,
            authority=f"https://login.microsoftonline.com/{self.tenant_id}",
            token_cache=cache
        )
        
        return app, cache
    
    def authenticate(self, force_refresh: bool = False) -> str:
        """Authenticate with Microsoft Graph using device code flow."""
        app, cache = self._get_msal_app()
        
        # Try cached token first
        accounts = app.get_accounts()
        if accounts and not force_refresh:
            result = app.acquire_token_silent(self.scopes, account=accounts[0])
            if result and "access_token" in result:
                self.access_token = result["access_token"]
                print("[OK] Using cached authentication token")
                return self.access_token
        
        # Device code flow for interactive auth
        print("[AUTH] Authentication required...")
        flow = app.initiate_device_flow(scopes=self.scopes)
        
        if "user_code" not in flow:
            raise Exception(f"Failed to create device flow: {flow.get('error_description', 'Unknown error')}")
        
        print(f"\n{flow['message']}\n")
        
        result = app.acquire_token_by_device_flow(flow)
        
        if "access_token" not in result:
            raise Exception(f"Authentication failed: {result.get('error_description', 'Unknown error')}")
        
        self.access_token = result["access_token"]
        
        if cache.has_state_changed:
            self.token_cache_file.write_text(cache.serialize())
        
        print("[OK] Authentication successful!")
        return self.access_token
    
    def _make_request(self, endpoint: str, **kwargs) -> requests.Response:
        """Make an authenticated request to Microsoft Graph."""
        if not self.access_token:
            self.authenticate()
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            **kwargs.pop("headers", {})
        }
        
        url = f"{self.graph_base_url}{endpoint}" if endpoint.startswith("/") else endpoint
        response = requests.get(url, headers=headers, **kwargs)
        
        # Handle token expiration
        if response.status_code == 401:
            print("[WARN] Token expired, re-authenticating...")
            self.authenticate(force_refresh=True)
            headers["Authorization"] = f"Bearer {self.access_token}"
            response = requests.get(url, headers=headers, **kwargs)
        
        return response
    
    def list_folder_contents(self, folder_path: str) -> list[dict]:
        """List all items in a SharePoint folder."""
        encoded_path = folder_path.replace("/", ":/").lstrip(":")
        if encoded_path:
            endpoint = f"/drives/{self.drive_id}/root:/{encoded_path}:/children"
        else:
            endpoint = f"/drives/{self.drive_id}/root/children"
        
        items = []
        next_link = endpoint
        
        while next_link:
            response = self._make_request(next_link)
            response.raise_for_status()
            data = response.json()
            items.extend(data.get("value", []))
            next_link = data.get("@odata.nextLink")
        
        return items
    
    def find_newest_file(self, folder_path: str, pattern: str) -> Optional[dict]:
        """Find the newest file matching a pattern in the folder."""
        items = self.list_folder_contents(folder_path)
        files = [item for item in items if "file" in item]
        
        matching = [f for f in files if fnmatch.fnmatch(f["name"], pattern)]
        
        if not matching:
            return None
        
        # Sort by lastModifiedDateTime descending
        matching.sort(key=lambda x: x.get("lastModifiedDateTime", ""), reverse=True)
        return matching[0]
    
    def download_file(self, file_item: dict, destination_dir: Path, filename: Optional[str] = None) -> Path:
        """Download a file from SharePoint."""
        destination_dir = Path(destination_dir)
        destination_dir.mkdir(parents=True, exist_ok=True)
        
        filename = filename or file_item["name"]
        destination_path = destination_dir / filename
        
        download_url = file_item.get("@microsoft.graph.downloadUrl")
        
        if not download_url:
            item_id = file_item["id"]
            response = self._make_request(f"/drives/{self.drive_id}/items/{item_id}")
            response.raise_for_status()
            item_data = response.json()
            download_url = item_data.get("@microsoft.graph.downloadUrl")
        
        if not download_url:
            raise Exception(f"Could not get download URL for {filename}")
        
        print(f"[DOWNLOAD] Downloading {filename}...")
        
        response = requests.get(download_url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        
        with open(destination_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    progress = (downloaded / total_size) * 100
                    print(f"\r  Progress: {progress:.1f}%", end="", flush=True)
        
        print(f"\n[OK] Saved to {destination_path}")
        return destination_path


def download_shipvoid_files(destination_dir: Path = Path(".")) -> tuple[Optional[Path], Optional[Path]]:
    """
    Download the newest Shipvoid Forecast and Legacy Unbilled Carton files.
    
    Args:
        destination_dir: Directory to save the downloaded files
    
    Returns:
        Tuple of (shipvoid_file_path, legacy_file_path), either can be None if not found
    """
    print("="*60)
    print("SharePoint File Downloader - DC 6031")
    print("="*60)
    print()
    
    downloader = SharePointDownloader()
    downloader.authenticate()
    
    print(f"\nSearching for files in: {FOLDER_PATH}")
    
    shipvoid_path = None
    legacy_path = None
    
    # Find and download Shipvoid Forecast
    print(f"\nLooking for: {SHIPVOID_PATTERN}")
    shipvoid_file = downloader.find_newest_file(FOLDER_PATH, SHIPVOID_PATTERN)
    if shipvoid_file:
        print(f"  Found: {shipvoid_file['name']}")
        shipvoid_path = downloader.download_file(shipvoid_file, destination_dir)
    else:
        print(f"  [WARN] No files matching '{SHIPVOID_PATTERN}' found")
    
    # Find and download Legacy Unbilled Cartons
    print(f"\nLooking for: {LEGACY_PATTERN}")
    legacy_file = downloader.find_newest_file(FOLDER_PATH, LEGACY_PATTERN)
    if legacy_file:
        print(f"  Found: {legacy_file['name']}")
        legacy_path = downloader.download_file(legacy_file, destination_dir)
    else:
        print(f"  [WARN] No files matching '{LEGACY_PATTERN}' found")
    
    print()
    print("="*60)
    print("Download Summary:")
    print(f"  Shipvoid Forecast: {shipvoid_path or 'Not found'}")
    print(f"  Legacy Unbilled:   {legacy_path or 'Not found'}")
    print("="*60)
    
    return shipvoid_path, legacy_path


if __name__ == "__main__":
    download_shipvoid_files()
