#!/usr/bin/env python3
"""Lookup Pendo segment IDs by name using the REST API."""
import os
import sys
import json
import requests

def lookup_segments(search_term=None, app_id=None):
    """Lookup segments, optionally filtering by name."""
    api_key = os.environ.get('PENDO_API_KEY') or os.environ.get('PENDO_INTEGRATION_KEY')
    if not api_key:
        print("Error: PENDO_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)
    
    url = "https://app.pendo.io/api/v1/segment"
    headers = {"x-pendo-integration-key": api_key}
    params = {}
    
    if app_id:
        params["appId"] = app_id
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    
    segments = response.json()
    
    # Filter by search term if provided
    if search_term:
        search_lower = search_term.lower()
        segments = [s for s in segments if search_lower in s.get('name', '').lower()]
    
    # Print results
    for seg in segments:
        print(f"{seg['id']}\t{seg['name']}")
        if seg.get('description'):
            print(f"  → {seg['description']}")

if __name__ == "__main__":
    search = sys.argv[1] if len(sys.argv) > 1 else None
    app_id = sys.argv[2] if len(sys.argv) > 2 else None
    
    if search and search in ['-h', '--help']:
        print("Usage: python lookup_segments.py [search_term] [app_id]")
        print("\nExamples:")
        print("  python lookup_segments.py                    # List all segments")
        print("  python lookup_segments.py 'JAPAN'            # Search for JAPAN")
        print("  python lookup_segments.py 'paying' -323232   # With specific app")
        sys.exit(0)
    
    lookup_segments(search, app_id)
