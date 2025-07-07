import os

def process_all_pdfs(folder_path: str, output_json_path: str) -> str:
    # (Insert your robust PDF extraction code here)
    # This should save the JSON and return its path
    # For example:
    # process_folder(folder_path, output_json_path)
    # return output_json_path
    # (Assuming process_folder is your function from previous code)
    from app.services.extractor import process_folder
    process_folder(folder_path, output_json_path)
    return output_json_path
