# splunk_mcp_server.py
import os
import time
from typing import List, Dict
from mcp.server.fastmcp import FastMCP
from splunklib.client import Service # Corrected import for splunklib.client

# ===============================
# Configuration
# ===============================
# These environment variables must be set where this server process runs.
SPLUNK_HOST = os.environ.get("SPLUNK_HOST") # e.g., "your-splunk-instance"
SPLUNK_PORT = int(os.environ.get("SPLUNK_PORT", 8089)) # e.g., 8089
SPLUNK_TOKEN = os.environ.get("SPLUNK_TOKEN") # Splunk authentication token

# Initialize the MCP server instance
mcp_splunk = FastMCP("Splunk Logs Server")

@mcp_splunk.tool()
def query_splunk_logs(query: str) -> List[Dict]:
    """
    Queries Splunk for logs using the Splunk SDK.
    This function is exposed as an MCP tool.

    Args:
        query: The Splunk query string.

    Returns:
        A list of dictionaries, where each dictionary represents a Splunk event.
        Returns a list with an error dictionary on failure.
    """
    if not SPLUNK_HOST or not SPLUNK_TOKEN:
        return [{"error": "Splunk configuration (host or token) missing for MCP server."}]
    try:
        # Connect to Splunk service
        service = Service(host=SPLUNK_HOST, port=SPLUNK_PORT, token=SPLUNK_TOKEN)
        
        # Create a search job
        job = service.jobs.create(query)
        
        # Wait for the job to complete
        while not job.is_done():
            time.sleep(0.5)
        
        # Fetch and process results
        results = []
        for result in job.results():
            if isinstance(result, dict): # Ensure we only append actual event data
                results.append(result)
        
        # Clean up the job
        job.cancel()
        return results
    except Exception as e:
        print(f"Error in Splunk MCP server while querying: {e}")
        return [{"error": f"Failed to query Splunk: {e}"}]

if __name__ == "__main__":
    # This block runs the MCP server.
    # For development, you can run `mcp dev splunk_mcp_server.py`
    # For production, this would be a long-running service.
    print("Starting Splunk MCP Server...")
    mcp_splunk.run(transport="stdio")
    print("Splunk MCP Server stopped.")
