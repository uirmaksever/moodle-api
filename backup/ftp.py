import pysftp
import subprocess
from utils import progressbar
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

CURRENT_TIMESTAMP = datetime.datetime.now().strftime("%Y%m%d-%H%M")
# CURRENT_TIMESTAMP = "20211204-1748"
HOST = os.environ["HOST"]
USER = os.environ["USER"]
PKEY = os.environ["PKEY"]
CWD = os.environ["CWD"]
COMMAND = os.environ["COMMAND"]
REMOTE_KEYFILE_NAME = os.environ["REMOTE_KEYFILE_NAME"]

DB_HOST = os.environ["COMMAND"] 
DB_USER = os.environ["DB_USER"]
DB_USER_PASSWORD = os.environ["DB_USER_PASSWORD"]
DB_NAME = os.environ["DB_NAME"]
EXTRA_OPTS = os.environ["EXTRA_OPTS"]
DB_BACKUP_FILENAME = os.environ["DB_BACKUP_FILENAME"]

FOLDERS_TO_BACKUP = ["moodle_git", "moodledata"]
DBEXCLUDE = ["performance_schema", "information_schema"]

def get_backup_filename(foldername):
    print(f"Backup time is: {CURRENT_TIMESTAMP}")
    return f"{foldername}_backup_{CURRENT_TIMESTAMP}.zip"

def zip_folder(foldername):
    backup_filename = get_backup_filename(foldername=foldername)
    backup_filepath = f"/home/bitnami/{backup_filename}"
    command = f'sudo zip -r {backup_filepath} /bitnami/{foldername}'
    print(f"Starting to zip: {backup_filename}")
#    remote_call = f"echo {PKEY} | ssh -tt {USER}@{HOST} {command}"
    remote_call = f"ssh -i {REMOTE_KEYFILE_NAME} {USER}@{HOST} {command}"

    print(remote_call)
    zip_call = subprocess.Popen(remote_call, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    # zip_call.communicate() # f"{PKEY}".encode('utf-8')
    
    print(f"BACKUP FILE ZIPPED: {backup_filename}")
    return backup_filepath, backup_filename

def create_database_backup():
    ignored_tables_strs = [f"--ignore-table {DB_NAME}.{table_name}" for table_name in DBEXCLUDE]
    ignored_tables = " ".join(ignored_tables_strs)
    db_filename =  f"{DB_NAME}_{CURRENT_TIMESTAMP}.sql"
    db_filepath = f"/home/bitnami/{db_filename}"
    dump_command = f"mysqldump -h {DB_HOST} -u {DB_USER} -p{DB_USER_PASSWORD} --single-transaction {ignored_tables} {DB_NAME}  > {db_filename}"
    remote_call = f'echo {PKEY} | ssh -tt {USER}@{HOST} "{dump_command}"'
    print(remote_call)
    dump_call = subprocess.Popen(remote_call, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return db_filepath, db_filename

def download_backup(backup_filepath, backup_filename):
    backup_directory = "backups/" + CURRENT_TIMESTAMP
    with pysftp.Connection(HOST, username=USER, private_key=REMOTE_KEYFILE_NAME, private_key_pass=PKEY) as sftp:
        with sftp.cd(CWD):             # temporarily chdir to public
            # sftp.put('/my/local/filename')  # upload file to public/ on remote
            if not os.path.isdir(backup_directory):
                os.mkdir(backup_directory) # !todo: if it does not exist now
            local_backup_path = f"{backup_directory}/{backup_filename}"
            print(f"GETTING FILE: {backup_filepath}")
            sftp.get(backup_filepath,
                local_backup_path,
                callback=lambda x,y: progressbar(x,y),
                preserve_mtime=True)         # get a remote file
            
            print(f"BACKUP DOWNLOADED AT: {backup_filepath}")
            # # Remove the file
            # sftp.execute(f"rm {backup_filepath}")
            # print("BACKUP DELETED FROM SERVER")

if __name__ == "__main__":
    # for foldername in FOLDERS_TO_BACKUP:
    #     backup_filepath, backup_filename = zip_folder(foldername)
    #     download_backup(backup_filepath, backup_filename)
    #     print("\n", "####", "\n", "####", "\n")
    moodle_backup_filepath, moodle_backup_filename = zip_folder("moodle_git")
    download_backup(moodle_backup_filepath, moodle_backup_filename)

    moodledata_backup_filepath, moodledata_backup_filename = zip_folder("moodledata")
    download_backup(moodledata_backup_filepath, moodledata_backup_filename)

    db_filepath, db_filename = create_database_backup()
    download_backup(db_filepath, db_filename)
