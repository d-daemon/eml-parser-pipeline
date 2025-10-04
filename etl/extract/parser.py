"""
etl/extract/parser.py
---------------------
Generic EmailParser for extracting messages and attachments from .eml files.

- Decodes MIME content
- Extracts body, metadata, and attachments
- Returns results as DataFrames
"""

import base64
import quopri
from email import policy
from email.parser import BytesParser
from pathlib import Path
from io import BytesIO
import pandas as pd
from bs4 import BeautifulSoup
from etl.core.logger import get_logger
from etl.core.utils import extract_message_id, extract_message_timestamp, extract_speaker_name, extract_speaker_contact, extract_message_content

logger = get_logger(__name__)


class EmailParser:
    """
    Lightweight parser for .eml files.

    Outputs two DataFrames:
    - messages_df: parsed message content
    - attachments_df: parsed attachment metadata
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    def decode_content(self, part):
        """
        Decode an email MIME part depending on encoding.
        """
        content = part.get_payload()
        cte = part.get("Content-Transfer-Encoding", "").lower()
        charset = part.get_content_charset() or "utf-8"

        try:
            if cte == "base64":
                if isinstance(content, str):
                    content = content.encode("ascii")
                decoded = base64.b64decode(content)
            elif cte == "quoted-printable":
                if isinstance(content, str):
                    content = content.encode("ascii")
                decoded = quopri.decodestring(content)
            elif cte in ["7bit", "8bit", "binary"]:
                decoded = content.encode(charset) if isinstance(content, str) else content
            else:
                decoded = content.encode(charset) if isinstance(content, str) else content

            # Try to decode to string
            if isinstance(decoded, bytes):
                return decoded.decode(charset, errors="replace")
            return decoded
        except Exception as e:
            self.logger.error(f"Failed to decode content: {e}")
            return None

    def parse_email(self, file_path: str):
        """
        Parse one .eml file into messages and attachments DataFrames.
        Clean, single-pass version — no redundant walking.
        """
        messages = []
        attachments = []
        email_id = Path(file_path).stem

        try:
            with open(file_path, "rb") as f:
                msg = BytesParser(policy=policy.default).parse(f)

            # --------------------------------------------------
            # Extract body (prefer plain text, fallback to HTML)
            # --------------------------------------------------
            body_part = msg.get_body(preferencelist=("plain", "html"))
            if body_part:
                body = self.decode_content(body_part)
                soup = BeautifulSoup(body, "html.parser")
                text = soup.get_text(" ", strip=True) if soup else body

                if "message id:" in text.lower():
                    message_id = extract_message_id(text)
                    timestamp = extract_message_timestamp(text)
                    speaker_name = extract_speaker_name(text)
                    speaker_contact = extract_speaker_contact(text)
                    message_content = extract_message_content(text)

                    messages.append({
                        "email_id": email_id,
                        "message_id": message_id,
                        "timestamp": timestamp,
                        "speaker_name": speaker_name,
                        "speaker_contact": speaker_contact,
                        "message": message_content,
                        "with_attachment": None,  # set later
                    })
                else:
                    self.logger.warning(f"No Message ID found in body for {file_path}")
            else:
                self.logger.warning(f"No text/plain or text/html body found in {file_path}")

            # --------------------------------------------------
            # Extract attachments
            # --------------------------------------------------
            for part in msg.iter_attachments():
                filename = part.get_filename()
                if not filename:
                    continue
                content_id = part.get("Content-ID")
                if content_id:
                    content_id = content_id.strip("<>")
                attachments.append({
                    "email_id": email_id,
                    "attachment_name": filename,
                    "content_id": content_id,
                    "content_type": part.get_content_type(),
                })

            return pd.DataFrame(messages), pd.DataFrame(attachments)

        except Exception as e:
            self.logger.error(f"Error parsing {file_path}: {e}")
            return pd.DataFrame(), pd.DataFrame()
