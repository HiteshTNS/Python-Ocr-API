# html_utils.py
import json

def build_html_body(payload: dict) -> str:
    # Simple example: format JSON response prettily in HTML body
    pretty_json = json.dumps(payload, indent=2)
    html = f"""
    <html>
      <body>
        <h2>OCR Result Payload</h2>
        <pre>{pretty_json}</pre>
      </body>
    </html>
    """
    return html
