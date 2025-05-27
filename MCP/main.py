# main.py
import os
import time
import asyncio
from typing import List, Dict, Optional
from langchain_core.runnables import RunnablePassthrough
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
import hashlib
import json
from datetime import datetime, timedelta

# Import MCP Client and related
from langchain_mcp_adapters.client import MultiServerMCPClient
from mcp.client.stdio import StdioServerParameters

# ===============================
# Configuration
# ===============================
# Paths to your MCP server scripts.
# These paths should be relative to where main.py is executed,
# or absolute paths.
SPLUNK_SERVER_PATH = os.environ.get("SPLUNK_SERVER_PATH", "./splunk_mcp_server.py")
CLOUDWATCH_SERVER_PATH = os.environ.get("CLOUDWATCH_SERVER_PATH", "./cloudwatch_mcp_server.py")
POSTGRES_SERVER_PATH = os.environ.get("POSTGRES_SERVER_PATH", "./postgres_mcp_server.py")
EMAIL_SERVER_PATH = os.environ.get("EMAIL_SERVER_PATH", "./email_mcp_server.py")

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# LLM setup
llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0, api_key=OPENAI_API_KEY)

# Initialize MCP Client
# This client will manage connections to your various MCP servers.
# For StdioServerParameters, it will launch the server scripts as subprocesses.
# For production, you'd likely use HttpServerParameters if your servers are remote.
mcp_client = MultiServerMCPClient(
    {
        "splunk": StdioServerParameters(command="python", args=[SPLUNK_SERVER_PATH]),
        "cloudwatch": StdioServerParameters(command="python", args=[CLOUDWATCH_SERVER_PATH]),
        "postgres": StdioServerParameters(command="python", args=[POSTGRES_SERVER_PATH]),
        "email": StdioServerParameters(command="python", args=[EMAIL_SERVER_PATH]),
    }
)

# ===============================
# Helper Functions (now using MCP client)
# ===============================

async def get_splunk_logs_mcp(log_id: str) -> str:
    """
    Retrieves detailed logs from Splunk via the MCP Splunk Server.
    """
    query = f"search log_correlation_id=\"{log_id}\" | sort _time"
    # Call the 'query_splunk_logs' tool on the 'splunk' MCP server
    splunk_results = await mcp_client.call_tool("splunk", "query_splunk_logs", {"query": query})
    
    if splunk_results and not splunk_results[0].get("error"):
        # Convert list of dicts to string representation
        return "\n".join([str(event) for event in splunk_results])
    
    error_msg = splunk_results[0].get("error", "Unknown error") if splunk_results else "No response"
    print(f"Error getting Splunk logs via MCP: {error_msg}")
    return ""

async def get_cloudwatch_logs_mcp(log_id: str) -> str:
    """
    Retrieves logs from CloudWatch via the MCP CloudWatch Server,
    first by trace ID, then by time range.
    """
    cloudwatch_logs_events = []
    log_group_name = "/aws/lambda/your-lambda-function-name" # <<< IMPORTANT: Replace with your actual CloudWatch Log Group Name

    # 1. Try to get logs by trace ID
    trace_query = f"{{ traceId = \"{log_id}\" }}"
    end_time_trace_search = datetime.now()
    start_time_trace_search = end_time_trace_search - timedelta(minutes=5) # Search last 5 minutes for trace ID

    trace_results = await mcp_client.call_tool(
        "cloudwatch", "get_cloudwatch_events",
        {
            "log_group_name": log_group_name,
            "start_time_iso": start_time_trace_search.isoformat() + "Z",
            "end_time_iso": end_time_trace_search.isoformat() + "Z",
            "filter_pattern": trace_query
        }
    )
    if trace_results and not trace_results[0].get("error"):
        cloudwatch_logs_events.extend(trace_results)
    else:
        print(f"No direct CloudWatch logs found for traceId '{log_id}' or error: {trace_results[0].get('error') if trace_results else 'No response'}")

    # 2. Fallback: Search by time range if trace ID search yields no useful results
    #    This part heavily depends on `get_approximate_time_from_log_id`.
    if not cloudwatch_logs_events: # If no events found by trace ID
        approximate_time = get_approximate_time_from_log_id(log_id)
        if approximate_time:
            start_time_range = approximate_time - timedelta(seconds=5)
            end_time_range = approximate_time + timedelta(seconds=5)
            
            time_range_results = await mcp_client.call_tool(
                "cloudwatch", "get_cloudwatch_events",
                {
                    "log_group_name": log_group_name,
                    "start_time_iso": start_time_range.isoformat() + "Z",
                    "end_time_iso": end_time_range.isoformat() + "Z",
                    "filter_pattern": "" # No specific filter pattern for broad time search
                }
            )
            if time_range_results and not time_range_results[0].get("error"):
                 cloudwatch_logs_events.extend(time_range_results)
            else:
                print(f"No CloudWatch logs found in time range for '{log_id}' or error: {time_range_results[0].get('error') if time_range_results else 'No response'}")
        else:
            print(f"Warning: Could not get approximate time for log_id '{log_id}', skipping time-range CloudWatch search.")
    
    # Process the logs to remove duplicates and combine messages.
    unique_logs_messages = {}
    for log_event in cloudwatch_logs_events:
        # Ensure 'timestamp', 'message', and 'logStreamName' fields are present
        if all(k in log_event for k in ['timestamp', 'message', 'logStreamName']):
            # Create a unique key for deduplication
            log_id_key = f"{log_event['logStreamName']}-{log_event['timestamp']}"
            if log_id_key not in unique_logs_messages:
                unique_logs_messages[log_id_key] = log_event['message']
        else:
            print(f"Skipping malformed CloudWatch log event: {log_event}")
    
    return "\n".join(unique_logs_messages.values())

async def get_all_logs_mcp(log_id: str) -> str:
    """
    Retrieves logs from both Splunk and CloudWatch for a given log ID via MCP.
    """
    splunk_logs = await get_splunk_logs_mcp(log_id)
    cloudwatch_logs = await get_cloudwatch_logs_mcp(log_id)
    return f"Splunk Logs:\n{splunk_logs}\n\nCloudWatch Logs:\n{cloudwatch_logs}"

async def get_previous_errors_mcp(error_signature: str) -> List[Dict]:
    """
    Retrieves previous errors from the database via the MCP PostgreSQL Server.
    """
    results = await mcp_client.call_tool("postgres", "get_previous_errors", {"error_signature": error_signature})
    
    if results and not results[0].get("error"):
        # The PostgreSQL server returns a list of dicts.
        # Ensure timestamps are converted back to datetime objects if needed for comparison.
        for row in results:
            if isinstance(row.get('first_occurrence_timestamp'), str):
                row['first_occurrence_timestamp'] = datetime.fromisoformat(row['first_occurrence_timestamp'].replace('Z', '+00:00'))
            if isinstance(row.get('last_occurrence_timestamp'), str):
                row['last_occurrence_timestamp'] = datetime.fromisoformat(row['last_occurrence_timestamp'].replace('Z', '+00:00'))
        return results
    
    error_msg = results[0].get("error", "Unknown error") if results else "No response"
    print(f"Error getting previous errors via MCP: {error_msg}")
    return []

async def store_error_mcp(error_signature: str, summary: str, category: str, log_id: str) -> None:
    """
    Stores the error information in the PostgreSQL database via the MCP PostgreSQL Server.
    """
    response = await mcp_client.call_tool(
        "postgres", "store_error",
        {
            "error_signature": error_signature,
            "summary": summary,
            "category": category,
            "log_id": log_id
        }
    )
    if response and response.get("status") == "error":
        print(f"Error storing error via MCP: {response.get('message', 'Unknown error')}")
    else:
        print("Error stored successfully via MCP.")

async def send_email_mcp(message: str) -> None:
    """
    Sends the error notification email via the MCP Email Server.
    """
    if not message or message == "No errors to report.":
        print("No email to send (no errors).")
        return

    # Assuming a default subject or you can pass it from the agent state
    subject = "API Error Notification"
    response = await mcp_client.call_tool("email", "send_notification_email", {"subject": subject, "body": message})
    
    if response and response.get("status") == "error":
        print(f"Error sending email via MCP: {response.get('message', 'Unknown error')}")
    else:
        print("Email sent successfully via MCP.")

# Re-use existing functions that don't directly interact with external systems
def summarize_and_categorize(logs: str) -> Dict:
    """
    Summarizes the error logs and categorizes the issue using an LLM.
    This function remains synchronous as it only interacts with the LLM locally.
    """
    prompt = f"""You are an expert at analyzing application logs. \
    Summarize the following error logs and categorize the issue into one of the \
    following types: Database Connectivity Error, Authentication Failure, \
    Resource Exhaustion, Invalid Input, External API Failure, Timeout Error, Configuration Error, Other. \
    Provide the summary and category in JSON format.

    Logs:
    {logs}

    Output in JSON format:
    {{
        "summary": "...",
        "category": "..."
    }}
    """
    try:
        response = llm.invoke([HumanMessage(content=prompt)])
        # Parse the JSON response
        return json.loads(response.content)
    except json.JSONDecodeError:
        print(f"Error decoding JSON from LLM: {response.content}")
        return {"summary": "Error analyzing logs", "category": "Other"}
    except Exception as e:
        print(f"Error during LLM summarization: {e}")
        return {"summary": "Error analyzing logs", "category": "Other"}

def should_group(current_error: Dict, previous_errors: List[Dict]) -> bool:
    """
    Determines if the current error should be grouped with previous errors.
    """
    if not previous_errors:
        return False

    similarity_threshold = 0.8

    for previous_error in previous_errors:
        current_signature = f"{current_error['category']} - {current_error['summary']}"
        previous_signature = f"{previous_error['category']} - {previous_error['summary']}"
        similarity = string_similarity(current_signature, previous_signature)
        if similarity >= similarity_threshold:
            return True
    return False

def string_similarity(str1: str, str2: str) -> float:
    """
    Calculates the similarity between two strings using a simple approach.
    """
    if not str1 or not str2:
        return 0.0
    max_len = max(len(str1), len(str2))
    common_chars = 0
    for i in range(min(len(str1), len(str2))):
        if str1[i] == str2[i]:
            common_chars += 1
    return common_chars / max_len

def group_errors(grouped_errors: Dict, new_error: Dict, log_id: str) -> Dict:
    """
    Groups the new error with existing grouped errors.
    """
    if not grouped_errors:
        grouped_errors = {
            "category": new_error["category"],
            "summary": new_error["summary"],
            "count": 1,
            "log_ids": [log_id],
            "first_occurrence": datetime.now(),
            "last_occurrence": datetime.now(),
        }
        return {"grouped_error": grouped_errors}

    grouped_errors["count"] += 1
    grouped_errors["log_ids"].append(log_id)
    grouped_errors["last_occurrence"] = datetime.now()
    return {"grouped_error": grouped_errors}

def format_notification(grouped_errors: Dict) -> str:
    """
    Formats the error information into a human-readable email message.
    """
    if "grouped_error" not in grouped_errors:
        return "No errors to report."

    error_data = grouped_errors["grouped_error"]
    message = f"""
    API Error Alert

    Category: {error_data['category']}
    Summary: {error_data['summary']}
    Count: {error_data['count']}
    First Occurrence: {error_data['first_occurrence']}
    Last Occurrence: {error_data['last_occurrence']}
    Log IDs: {', '.join(error_data['log_ids'])}
    """
    return message

def get_approximate_time_from_log_id(log_id: str) -> Optional[datetime]:
    """
    <<< IMPORTANT: Placeholder function. You MUST implement this
    based on how you can derive a timestamp from your actual log_id format.
    If your log_id itself contains a timestamp, parse it here.
    Otherwise, you might need to query a lightweight index first to get the timestamp.
    """
    # Example: If log_id is like "API_ERROR_20231027T103000_XYZ"
    # try:
    #     timestamp_str = log_id.split('_')[2] # Extract "20231027T103000"
    #     return datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S")
    # except (IndexError, ValueError):
    #     pass

    # For demonstration, returning current time minus a minute for any "test" log_id
    if "test" in log_id.lower():
        return datetime.now() - timedelta(minutes=1)
    
    print(f"Warning: Could not derive approximate time from log_id: {log_id}. CloudWatch time-range search may be less effective.")
    return None

# ===============================
# LangGraph Workflow Definition
# ===============================
class GraphState:
    """
    State class for the LangGraph workflow.
    """
    log_id: str
    logs: str = ""
    error_info: Dict = {}
    grouped_errors: Dict = {}
    should_group: bool = False
    message: str = ""

# Define the LangGraph workflow
def define_workflow():
    """
    Defines the LangGraph workflow for API error analysis using MCP.
    """
    workflow = StateGraph(GraphState)

    # All nodes that interact with MCP tools must be asynchronous.
    # Nodes that only process local data (like summarize_and_categorize) can remain synchronous.
    workflow.add_node("get_log_id", lambda state: {"log_id": "your_actual_log_id_from_alert"}) # <<< IMPORTANT: Replace with dynamic log ID
    workflow.add_node("retrieve_logs", lambda state: get_all_logs_mcp(state.log_id))
    workflow.add_node("analyze_error", lambda state: summarize_and_categorize(state.logs))
    workflow.add_node("check_grouping", check_grouping_decision)
    workflow.add_node("group_errors", group_errors_action)
    workflow.add_node("format_notification", lambda state: {"message": format_notification(state.grouped_errors)})
    workflow.add_node("send_email", lambda state: send_email_mcp(state.message))

    workflow.set_entry_point("get_log_id")
    workflow.add_edge("get_log_id", "retrieve_logs")
    workflow.add_edge("retrieve_logs", "analyze_error")
    workflow.add_edge("analyze_error", "check_grouping")

    async def check_grouping_decision(state: GraphState):
        """
        Check if the current error should be grouped with previous errors via MCP.
        """
        error_signature = hashlib.md5(f"{state.error_info['category']}-{state.error_info['summary']}".encode()).hexdigest()
        previous_errors = await get_previous_errors_mcp(error_signature)
        should_group_flag = should_group(state.error_info, previous_errors)
        return "group" if should_group_flag else "new_error"

    async def group_errors_action(state: GraphState):
        """
        Group the current error with existing grouped errors or start a new group,
        and store/update in DB via MCP.
        """
        error_signature = hashlib.md5(f"{state.error_info['category']} - {state.error_info['summary']}".encode()).hexdigest()
        previous_errors = await get_previous_errors_mcp(error_signature) # Get previous errors via MCP
        
        grouped_error_data = {}
        if previous_errors:
            # If there are previous errors, initialize grouped_error_data with the first one.
            first_prev_error = previous_errors[0]
            grouped_error_data = {
                "category": first_prev_error['category'],
                "summary": first_prev_error['summary'],
                "count": first_prev_error['occurrence_count'],
                "log_ids": first_prev_error['log_ids'],
                "first_occurrence": first_prev_error['first_occurrence_timestamp'],
                "last_occurrence": first_prev_error['last_occurrence_timestamp'],
            }

        updated_group = group_errors(grouped_error_data, state.error_info, state.log_id)
        
        # Store the error in the database via MCP
        await store_error_mcp(error_signature, state.error_info['summary'], state.error_info['category'], state.log_id)
        
        return {"grouped_errors": updated_group}

    workflow.add_conditional_edges(
        "check_grouping",
        check_grouping_decision,
        {
            "group": "group_errors",
            "new_error": "format_notification",
        },
    )

    workflow.add_edge("group_errors", "format_notification")
    workflow.add_edge("format_notification", "send_email")
    workflow.set_finish("send_email")

    return workflow

async def main():
    """
    Main asynchronous function to execute the LangGraph workflow.
    """
    workflow = define_workflow()
    chain = workflow.compile()

    # IMPORTANT: Start the MCP client. This will launch the subprocesses for StdioServerParameters.
    # In a real application, you'd manage this lifecycle carefully (e.g., using a context manager
    # or ensuring it's started once at application startup).
    await mcp_client.start()
    print("MCP Client started and connected to servers.")

    try:
        # Example usage: Replace "your_actual_log_id_from_alert" with the real log ID
        inputs = {"log_id": "test_log_id_example_123"} # <<< IMPORTANT: Replace this
        print(f"Invoking workflow for log_id: {inputs['log_id']}")
        result = await chain.ainvoke(inputs) # Use ainvoke for async chain
        print(f"Workflow execution complete. Final state: {result}")
    finally:
        # IMPORTANT: Stop the MCP client to clean up subprocesses.
        await mcp_client.stop()
        print("MCP Client stopped.")

if __name__ == "__main__":
    # Run the asynchronous main function
    asyncio.run(main())

