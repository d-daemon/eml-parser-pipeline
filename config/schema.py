"""
config/schema.py
-----------------
Defines table schemas for parsed email messages and attachments.
"""

from typing import List, Dict

# --------------------------------------------------------------------
# Message schema
# --------------------------------------------------------------------
MESSAGE_SCHEMA: List[Dict[str, str]] = [
    {"name": "email_id", "type": "STRING"},
    {"name": "stream_id", "type": "STRING"},
    {"name": "timestamp", "type": "TIMESTAMP"},
    {"name": "platform", "type": "STRING"},
    {"name": "speaker_name", "type": "STRING"},
    {"name": "speaker_contact", "type": "STRING"},
    {"name": "message", "type": "STRING"},
    {"name": "message_id", "type": "STRING"},
    {"name": "with_attachment", "type": "BOOLEAN"},
    {"name": "batch_dt", "type": "DATE"},
    # Optional metadata filters
    {"name": "line_of_business", "type": "STRING"},
    {"name": "region", "type": "STRING"},
    {"name": "entity", "type": "STRING"},
    {"name": "last_ingestion_ts", "type": "TIMESTAMP"},
]

# --------------------------------------------------------------------
# Attachment schema
# --------------------------------------------------------------------
ATTACHMENT_SCHEMA: List[Dict[str, str]] = [
    {"name": "email_id", "type": "STRING"},
    {"name": "attachment_name", "type": "STRING"},
    {"name": "content_id", "type": "STRING"},
    {"name": "message_id", "type": "STRING"},
    {"name": "stream_id", "type": "STRING"},
    {"name": "batch_dt", "type": "DATE"},
    # Optional metadata filters
    {"name": "line_of_business", "type": "STRING"},
    {"name": "region", "type": "STRING"},
    {"name": "entity", "type": "STRING"},
    {"name": "last_ingestion_ts", "type": "TIMESTAMP"},
]
