from os import getcwd

StudyApp_root_path = getcwd() + "\\"

pdf_path = "D:\\READING LIST"
chunk_database_path = StudyApp_root_path + "data\\pdf_text.db"
token_json_path = StudyApp_root_path + "data\\token_json"

log_file_path = StudyApp_root_path + "data\\process.log"
log_database_path = StudyApp_root_path + "data\\log_message.db"
buffer_json_path = StudyApp_root_path + "data\\buffer.json"