def process_all_pdfs(folder_path: str, output_json_path: str):
    from app.services.extractor import process_folder
    processed_count, total_files = process_folder(folder_path, output_json_path)
    if total_files == 0:
        return False, output_json_path
    percent = processed_count / total_files
    return percent >= 0.9, output_json_path
