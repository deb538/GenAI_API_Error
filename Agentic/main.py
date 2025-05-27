import os
import time
from typing import List, Dict, Optional
from langchain_core.runnables import RunnablePassthrough
from langgraph.graph import StateGraph, END
from langchain_openai import ChatOpenAI
from langchain_core.messages import BaseMessage, HumanMessage
from psycopg2 import connect
from psycopg2.extras import DictCursor
from smtplib import SMTP
from email.message import EmailMessage
from datetime import datetime, timedelta
import hashlib
import json

# ===============================
# Configuration
# ===============================
# Environment variables (replace with your actual values)
SPLUNK_URL = os.environ.get("SPLUNK_URL")  # e.g., "https://your-splunk-instance:8089"
SPLUNK_TOKEN = os.environ.get("SPLUNK_TOKEN")
CLOUDWATCH_REGION = os.environ.get("CLOUDWATCH_REGION")  # e.g., "us-east-1"
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_DB = os.environ.get("POSTGRES_DB")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
EMAIL_FROM = os.environ.get("EMAIL_FROM")
EMAIL_TO = os.environ.get("EMAIL_TO")  # Comma-separated list
EMAIL_SMTP_SERVER = os.environ.get("EMAIL_SMTP_SERVER")
EMAIL_SMTP_PORT = int(os.environ.get("EMAIL_SMTP_PORT", 587))  # Default to 587 if not set
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

# LLM setup
llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0, api_key=OPENAI_API_KEY)  # Or gpt-4

# ===============================
# Helper Functions
# ===============================
def connect_to_postgres():
    """
    Establishes a connection to the PostgreSQL database.
    Handles connection errors and retries.
    """
    try:
        conn = connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        raise  # Re-raise to stop execution.  Consider a retry mechanism if appropriate

def query_splunk(query: str) -> List[Dict]:
    """
    Queries Splunk using the Splunk SDK.
    Handles potential errors during the Splunk query.

    Args:
        query: The Splunk query string.

    Returns:
        A list of dictionaries, where each dictionary represents a Splunk event.
        Returns an empty list on error.
    """
    try:
        from splunklib.service import Service  # Import here to avoid issues if splunklib isn't always needed

        service = Service(
            host=SPLUNK_URL.split("//")[1].split(":")[0],  # Extract hostname from URL
            port=int(SPLUNK_URL.split(":")[-1]),
            token=SPLUNK_TOKEN,
        )
        job = service.jobs.create(query)
        while not job.is_done():
            time.sleep(0.5)
        results = []
        for result in job.results():
            if isinstance(result, dict):  # Only process actual events, not metadata
                results.append(result)
        job.cancel()
        return results
    except Exception as e:
        print(f"Error querying Splunk: {e}")
        return [] #Crucial: Return empty list on error, so the program does not crash.

def query_cloudwatch(log_group_name: str, start_time: datetime, end_time: datetime, filter_pattern: str = "") -> List[Dict]:
    """
    Queries CloudWatch Logs using the AWS SDK (boto3).

    Args:
        log_group_name: The name of the CloudWatch Log Group.
        start_time: The start time for the query.
        end_time: The end time for the query.
        filter_pattern: Optional filter pattern.

    Returns:
        A list of log events (dictionaries).  Returns an empty list on error.
    """
    try:
        import boto3  # Import inside the function
        client = boto3.client(
            "logs",
            region_name=CLOUDWATCH_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )

        response = client.filter_log_events(
            logGroupName=log_group_name,
            startTime=int(start_time.timestamp() * 1000),  # Convert to milliseconds
            endTime=int(end_time.timestamp() * 1000),
            filterPattern=filter_pattern,
        )
        events = response.get('events', [])
        return events
    except Exception as e:
        print(f"Error querying CloudWatch: {e}")
        return []

def get_splunk_logs(log_id: str) -> str:
    """
    Retrieves detailed logs from Splunk for a given log ID.

    Args:
        log_id: The log correlation ID.

    Returns:
        A string containing the relevant log entries, or an empty string on error.
    """
    query = f"search log_correlation_id=\"{log_id}\" | sort _time"  # Added sort
    splunk_results = query_splunk(query)
    if splunk_results:
        return "\n".join([str(event) for event in splunk_results])  # Convert events to string
    return ""

def get_cloudwatch_logs(log_id: str) -> str:
    """
    Retrieves logs from CloudWatch, first by trace ID, then by time range.

    Args:
        log_id:  The log correlation ID, which might be a trace ID in CloudWatch.

    Returns:
        A string containing the CloudWatch log entries, or an empty string.
    """
    cloudwatch_logs = []

    # 1. Try to get logs by trace ID (if applicable)
    trace_query = f"{{ traceId = \"{log_id}\" }}" # KVP syntax
    cloudwatch_results = query_cloudwatch(
        log_group_name="/aws/lambda/your-lambda-function-name",  # Replace this
        start_time=datetime.now() - timedelta(minutes=5),  # Set appropriate time range
        end_time=datetime.now(),
        filter_pattern=trace_query
    )
    if cloudwatch_results:
        cloudwatch_logs.extend(cloudwatch_results)


    # 2. Fallback: Search by time range (if trace ID search fails)
    #    This requires that you have a timestamp from Splunk or other source.
    #    For this example, we'll assume you can extract a timestamp.
    #    You'd need to modify this part to get the actual timestamp.
    #    This is a *very* broad search and should be used cautiously.
    #    It also assumes you know the log group.
    #
    #    IMPORTANT:  In a real-world scenario, you'd need to get the timestamp
    #               from the Splunk alert or from a related log entry.
    #               This is just a placeholder.
    #
    #    For this example, let's assume you have a function `get_approximate_time_from_log_id`
    #    that can give you an *approximate* time.  This is NOT ideal, and you should
    #    strive to get the *exact* timestamp if possible.
    #
    #    Also, you'd need to know the relevant CloudWatch Log Group.

    approximate_time = get_approximate_time_from_log_id(log_id) # Replace this
    if approximate_time:
        start_time = approximate_time - timedelta(seconds=5)
        end_time = approximate_time + timedelta(seconds=5)
        time_range_results = query_cloudwatch(
            log_group_name="/aws/lambda/your-lambda-function-name",  # Replace this
            start_time=start_time,
            end_time=end_time,
        )
        if time_range_results:
             cloudwatch_logs.extend(time_range_results)
    
    # Process the logs to remove duplicates and combine messages.
    unique_logs = {}
    for log_event in cloudwatch_logs:
        log_id = log_event['logStreamName'] + "-" + str(log_event['timestamp']) # Create unique ID
        if log_id not in unique_logs:
            unique_logs[log_id] = log_event
    
    return "\n".join([event['message'] for event in unique_logs.values()])

def get_all_logs(log_id: str) -> str:
    """
    Retrieves logs from both Splunk and CloudWatch for a given log ID.

    Args:
        log_id: The log correlation ID.

    Returns:
        A string containing combined logs from Splunk and CloudWatch.
    """
    splunk_logs = get_splunk_logs(log_id)
    cloudwatch_logs = get_cloudwatch_logs(log_id)
    return f"Splunk Logs:\n{splunk_logs}\n\nCloudWatch Logs:\n{cloudwatch_logs}"

def summarize_and_categorize(logs: str) -> Dict:
    """
    Summarizes the error logs and categorizes the issue using an LLM.

    Args:
        logs: The combined log data from Splunk and CloudWatch.

    Returns:
        A dictionary containing the summary and category.
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

    Args:
        current_error: The summary and category of the current error.
        previous_errors: A list of previous error dictionaries (from the database).

    Returns:
        True if the error should be grouped, False otherwise.
    """
    if not previous_errors:
        return False  # No previous errors to group with

    # Define similarity threshold (adjust as needed)
    similarity_threshold = 0.8  # Example: Require 80% similarity

    for previous_error in previous_errors:
        # Simple string comparison (consider more sophisticated methods like embeddings)
        current_signature = f"{current_error['category']} - {current_error['summary']}"
        previous_signature = f"{previous_error['category']} - {previous_error['summary']}"

        similarity = string_similarity(current_signature, previous_signature) # Use the function

        if similarity >= similarity_threshold:
            return True
    return False

def string_similarity(str1: str, str2: str) -> float:
    """
    Calculates the similarity between two strings using a simple approach.
    Consider using a more robust library like FuzzyWuzzy for real applications.

    Args:
        str1: The first string.
        str2: The second string.

    Returns:
        A float representing the similarity (0 to 1).
    """
    if not str1 or not str2:
        return 0.0  # Handle empty strings
    max_len = max(len(str1), len(str2))
    common_chars = 0
    for i in range(min(len(str1), len(str2))):
        if str1[i] == str2[i]:
            common_chars += 1
    return common_chars / max_len

def group_errors(grouped_errors: Dict, new_error: Dict, log_id: str) -> Dict:
    """
    Groups the new error with existing grouped errors.

    Args:
        grouped_errors: A dictionary of grouped errors.
        new_error: The new error to group.
        log_id: The log ID of the new error.

    Returns:
        The updated dictionary of grouped errors.
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
        return {"grouped_error": grouped_errors}  # Return inside the function

    grouped_errors["count"] += 1
    grouped_errors["log_ids"].append(log_id)
    grouped_errors["last_occurrence"] = datetime.now()
    return {"grouped_error": grouped_errors} # Return the updated grouped error


def format_notification(grouped_errors: Dict) -> str:
    """
    Formats the error information into a human-readable email message.

    Args:
        grouped_errors: The grouped error information.

    Returns:
        A string containing the formatted email message.
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

def send_email(message: str) -> None:
    """
    Sends the error notification email.

    Args:
        message: The email message to send.
    """
    if not message or message == "No errors to report.":
        print("No email to send (no errors).")
        return

    email = EmailMessage()
    email["From"] = EMAIL_FROM
    email["To"] = EMAIL_TO
    email["Subject"] = "API Error Notification"
    email.set_content(message)

    try:
        with SMTP(EMAIL_SMTP_SERVER, EMAIL_SMTP_PORT) as smtp:
            smtp.starttls()  # Use starttls for security
            smtp.login(EMAIL_FROM, os.environ.get("EMAIL_PASSWORD"))  # Store password securely
            smtp.send_message(email)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Error sending email: {e}")

def get_previous_errors(error_signature: str) -> List[Dict]:
    """
    Retrieves previous errors from the database based on the error signature.

    Args:
        error_signature: The unique signature of the error.

    Returns:
        A list of dictionaries representing previous errors.  Returns an empty list on error or no results.
    """
    conn = connect_to_postgres()
    cursor = conn.cursor(cursor_factory=DictCursor)
    try:
        cursor.execute(
            "SELECT summary, category, first_occurrence_timestamp, last_occurrence_timestamp, occurrence_count, log_ids FROM error_logs WHERE error_signature = %s",
            (error_signature,),
        )
        results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error querying database: {e}")
        return [] # Return empty list on error
    finally:
        cursor.close()
        conn.close()

def store_error(error_signature: str, summary: str, category: str, log_id: str) -> None:
    """
    Stores the error information in the PostgreSQL database.

    Args:
        error_signature: The unique signature of the error.
        summary: The error summary.
        category: The error category.
        log_id: The log ID.
    """
    conn = connect_to_postgres()
    cursor = conn.cursor()
    now = datetime.now()
    try:
        # Check if the error signature already exists
        cursor.execute("SELECT occurrence_count, log_ids FROM error_logs WHERE error_signature = %s", (error_signature,))
        existing_error = cursor.fetchone()

        if existing_error:
            # Update existing error
            new_count = existing_error[0] + 1
            new_log_ids = existing_error[1] + [log_id]  # Assuming log_ids is stored as an array
            cursor.execute(
                """
                UPDATE error_logs
                SET last_occurrence_timestamp = %s, occurrence_count = %s, log_ids = %s
                WHERE error_signature = %s
                """,
                (now, new_count, new_log_ids, error_signature),
            )
        else:
            # Insert new error
            cursor.execute(
                """
                INSERT INTO error_logs (error_signature, summary, category, first_occurrence_timestamp, last_occurrence_timestamp, occurrence_count, log_ids)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (error_signature, summary, category, now, now, 1, [log_id]),
            )
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error storing error in database: {e}")
    finally:
        cursor.close()
        conn.close()

def get_approximate_time_from_log_id(log_id: str) -> Optional[datetime]:
    """
    Placeholder function to get an *approximate* timestamp from a log ID.
    This is NOT ideal and should be replaced with a proper method to extract
    the timestamp from the log ID or a related source.

    Args:
        log_id: The log ID.

    Returns:
        An approximate datetime object, or None if the timestamp cannot be determined.
    """
    # Replace this with your actual logic to extract the timestamp
    # from the log ID or a related source.
    #
    # Example (replace with your actual logic):
    # if "ERROR-12345" in log_id:
    #     return datetime(2024, 7, 24, 10, 30, 0)  # Example timestamp
    return None  # Return None if no timestamp can be determined

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

# Define the LangGraph workflow
def define_workflow():
    """
    Defines the LangGraph workflow for API error analysis.
    """
    workflow = StateGraph(GraphState)

    workflow.add_node("get_log_id", lambda state: {"log_id": "your_log_id_from_alert"}) # Placeholder
    workflow.add_node("retrieve_logs", lambda state: {"logs": get_all_logs(state.log_id)})
    workflow.add_node("analyze_error", lambda state: {"error_info": summarize_and_categorize(state.logs)})
    workflow.add_node("check_grouping", check_grouping_decision) # Renamed for clarity
    workflow.add_node("group_errors", group_errors_action) # Renamed for clarity
    workflow.add_node("format_notification", lambda state: {"message": format_notification(state.grouped_errors)})
    workflow.add_node("send_email", lambda state: send_email(state.message))

    workflow.set_entry_point("get_log_id")
    workflow.add_edge("get_log_id", "retrieve_logs")
    workflow.add_edge("retrieve_logs", "analyze_error")
    workflow.add_edge("analyze_error", "check_grouping")

    # Define the conditional edges for grouping
    def check_grouping_decision(state):
        """
        Check if the current error should be grouped with previous errors.
        """
        error_signature = hashlib.md5(f"{state.error_info['category']}-{state.error_info['summary']}".encode()).hexdigest()
        previous_errors = get_previous_errors(error_signature)
        should_group_flag = should_group(state.error_info, previous_errors)
        return "group" if should_group_flag else "new_error"

    def group_errors_action(state):
        """
        Group the current error with existing grouped errors or start a new group.
        """
        error_signature = hashlib.md5(f"{state.error_info['category']} - {state.error_info['summary']}".encode()).hexdigest()
        previous_errors = get_previous_errors(error_signature) # Get previous errors
        if previous_errors:
            # If there are previous errors, group with the first one.
            grouped_error = {
                "category": previous_errors[0]['category'],
                "summary": previous_errors[0]['summary'],
                "count": previous_errors[0]['occurrence_count'],
                "log_ids": previous_errors[0]['log_ids'],
                "first_occurrence": previous_errors[0]['first_occurrence_timestamp'],
                "last_occurrence": previous_errors[0]['last_occurrence_timestamp'],
            }
        else:
            grouped_error = {} # start a new group

        updated_group = group_errors(grouped_error, state.error_info, state.log_id)
        # Store the error in the database
        store_error(error_signature, state.error_info['summary'], state.error_info['category'], state.log_id)
        return {"grouped_errors": updated_group}

    workflow.add_conditional_edges(
        "check_grouping",
        check_grouping_decision,  # Use the renamed function
        {
            "group": "group_errors",
            "new_error": "format_notification",
        },
    )

    workflow.add_edge("group_errors", "format_notification")
    workflow.add_edge("format_notification", "send_email")
    workflow.set_finish("send_email")

    return workflow

def main():
    """
    Main function to execute the LangGraph workflow.
    """
    workflow = define_workflow()
    chain = workflow.compile()

    # Example usage:  Replace "your_log_id_from_alert" with the actual log ID
    #                extracted from your Splunk alert.
    inputs = {"log_id": "your_log_id_from_alert"}  #  <--- Replace this
    result = chain.invoke(inputs)
    print(f"Workflow result: {result}")

if __name__ == "__main__":
    main()
