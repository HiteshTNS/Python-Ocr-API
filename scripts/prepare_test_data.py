import base64

def load_base64_from_file(file_path: str) -> str:
    with open(file_path, "r") as f:
        raw_data = f.read().strip()
    
    # Remove common `data:"..."` wrappers if they exist
    prefix = 'data:"'
    if raw_data.startswith(prefix) and raw_data.endswith('"'):
        raw_data = raw_data[len(prefix):-1]
    
    # Verify it’s valid base64 by decoding once
    try:
        _ = base64.b64decode(raw_data)
    except Exception as e:
        raise ValueError(f"Invalid base64 data in file: {e}")
    
    return raw_data

def create_test_data_list(base64_str: str, count: int = 5) -> list:
    """Returns a list of dicts, each with unique fileId and base64 PDF string."""
    return [
        {"fileId": f"fileid_{i+1}", "base64_pdf": base64_str}
        for i in range(count)
    ]

def convert_base64_to_bytes(base64_str: str) -> bytes:
    return base64.b64decode(base64_str)

if __name__ == "__main__":
    base64_file = r"C:\Users\st\Desktop\Python-Ocr-API\scripts\sample_base64pdf.txt"
    
    # Load base64 PDF string from file
    base64_pdf = load_base64_from_file(base64_file)
    print("Loaded base64 PDF data from file.")
    
    # Create list of 1000 test entries each with unique fileId
    test_data = create_test_data_list(base64_pdf, count=5)
    print(f"Created test data list with {len(test_data)} entries.")
    
    # Convert one example from base64 to bytes to illustrate usage
    example_bytes = convert_base64_to_bytes(test_data[0]["base64_pdf"])
    print(f"Example PDF bytes length: {len(example_bytes)}")
    
    # You can now use 'test_data' list for your async API calls or testing
