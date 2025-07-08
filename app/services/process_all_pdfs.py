def process_all_pdfs(folder_path: str, output_json_path: str,batch_size:int):
    from app.services.extractor import process_folder_fast
    processed_count, total_files = process_folder_fast(folder_path, output_json_path,batch_size)
    if total_files == 0 or processed_count ==0:
        return False, output_json_path, "No file exist in the folder or files are corrupted"
    percent = processed_count / total_files
    return percent >= 0.9, output_json_path, ""
