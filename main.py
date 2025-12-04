import subprocess
import sys

def run_script(script_name):
    print(f"=== Running {script_name} ===")
    try:
        result = subprocess.run([sys.executable, script_name], check=True)
        print(f"{script_name} finished successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f"Error running {script_name}")
        print("Error:", e)

if __name__ == "__main__":
    run_script("fetch_data.py")
    run_script("process_data.py")
    run_script("db_upload.py")
