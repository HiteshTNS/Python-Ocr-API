def process_all_pdfs(batch_size:int):
    from app.services.extractor import process_folder_fast_s3
    processed_count, total_files = process_folder_fast_s3(batch_size)
    if total_files == 0 or processed_count ==0:
        return False, "No file exist in the folder or files are corrupted"
    percent = processed_count / total_files
    return percent >= 0.9, "",processed_count,total_files
