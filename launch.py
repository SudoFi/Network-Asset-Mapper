import sys
import subprocess
from pathlib import Path
import os

def main():
    """
    Main launcher for the Network Asset Mapper.
    This script runs the prerequisite check and then the main application.
    """
    print("--- Network Asset Mapper Launcher ---")
    
    # Define the project's root directory based on this script's location.
    root_dir = Path(__file__).resolve().parent
    scripts_dir = root_dir / "Scripts"
    
    # Define the full paths to the scripts that need to be executed.
    check_script_path = scripts_dir / "check.py"
    main_script_path = scripts_dir / "main.py"
    
    # --- Step 1: Run the prerequisite checker ---
    print("\n--- Running Prerequisite Check ---")
    print("A new window will open for the check. Please review it and press Enter when it's complete.")
    
    try:
        # On Windows, open check.py in a new console window.
        # On other systems (macOS/Linux), it will run in the current terminal.
        if os.name == 'nt':
            process = subprocess.Popen([sys.executable, str(check_script_path)], creationflags=subprocess.CREATE_NEW_CONSOLE)
            process.wait() # Wait for the new console window to be closed before proceeding.
        else:
            subprocess.run([sys.executable, str(check_script_path)], check=True)

        print("\n--- Prerequisite Check Finished. Starting Main Script ---\n")
    
    except FileNotFoundError:
        print(f"ERROR: Could not find the prerequisite checker script at {check_script_path}")
    except Exception as e:
        print(f"An error occurred while running the prerequisite checker: {e}")
        input("Press Enter to exit.")
        return

    # --- Step 2: Run the main application script ---
    try:
        # Execute main.py and pass the '--pause-on-exit' argument.
        # This tells main.py to pause before closing, which is useful when double-clicking.
        subprocess.run([sys.executable, str(main_script_path), '--pause-on-exit'], check=True)
        
    except FileNotFoundError:
        print(f"ERROR: Could not find the main script at {main_script_path}")
    except Exception as e:
        print(f"An error occurred while running the main script: {e}")

if __name__ == "__main__":
    main()