"""
examples/generate_sample_eml.py
-------------------------------
Generate RFC-compliant .eml files for testing the ETL pipeline.

Each email:
- Has a unique Message-ID (UUID4)
- Includes one unique message text
- May include one text attachment with its own Content-ID
- Uses realistic MIME headers (multipart/mixed)
"""

import os
import uuid
import argparse
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SPEAKERS = [
    ("Alice Example", "alice@example.com"),
    ("Bob Demo", "bob@example.com"),
    ("Carol Test", "carol@example.com"),
]

MESSAGES = [
    "Hello team, please find attached the report.",
    "Reminder: standup meeting tomorrow at 9am.",
    "Here is the updated project plan.",
    "Let's schedule a call next week.",
    "Attached is the invoice for last month.",
]


def generate_eml(output_dir: str, count: int = 5):
    os.makedirs(output_dir, exist_ok=True)

    for i in range(count):
        # ------------------------------------------------------------------
        # Header metadata
        # ------------------------------------------------------------------
        sender_name, sender_email = SPEAKERS[i % len(SPEAKERS)]
        recipient_name, recipient_email = SPEAKERS[(i + 1) % len(SPEAKERS)]
        timestamp = datetime.utcnow() - timedelta(days=i * 3)
        message_id = f"<{uuid.uuid4()}@example.com>"

        # Create base multipart email
        msg = MIMEMultipart("mixed")
        msg["From"] = f"{sender_name} <{sender_email}>"
        msg["To"] = f"{recipient_name} <{recipient_email}>"
        msg["Subject"] = f"Test Email {i+1}"
        msg["Date"] = timestamp.strftime("%a, %d %b %Y %H:%M:%S +0000")
        msg["Message-ID"] = message_id

        # ------------------------------------------------------------------
        # Email body (plain text)
        # ------------------------------------------------------------------
        # Ensure each email gets its own unique message
        body_text = MESSAGES[i % len(MESSAGES)]
        body = f"""\
Message ID: {message_id.strip('<>')}
{timestamp.isoformat()}Z {sender_name} - {sender_email} says:
{body_text}
"""
        msg.attach(MIMEText(body, "plain", "utf-8"))

        # ------------------------------------------------------------------
        # Optional attachment (with Content-ID)
        # ------------------------------------------------------------------
        # For demo variety, alternate between emails with and without attachments
        if i % 2 == 0:  # every other email gets an attachment
            filename = f"attachment_{i}.txt"
            content_id = f"<{uuid.uuid4()}@example.com>"
            attachment_content = f"This is a fake attachment for email {i}."

            attachment = MIMEText(attachment_content, "plain", "utf-8")
            attachment.add_header("Content-ID", content_id)
            attachment.add_header("Content-Disposition", f'attachment; filename="{filename}"')

            msg.attach(attachment)

        # ------------------------------------------------------------------
        # Save to .eml file
        # ------------------------------------------------------------------
        file_path = os.path.join(output_dir, f"sample_{i+1}.eml")
        with open(file_path, "wb") as f:
            f.write(msg.as_bytes())

        print(f"Generated {file_path} with Message-ID {message_id}")

    print(f"\nCreated {count} sample .eml files in {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate sample .eml files for ETL testing")
    parser.add_argument("--output", type=str, default="examples/sample_emails", help="Output directory")
    parser.add_argument("--count", type=int, default=5, help="Number of .eml files to generate")
    args = parser.parse_args()

    generate_eml(args.output, args.count)
