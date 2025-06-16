import sys
import subprocess
import pkg_resources

# A dictionary of required libraries.
# Key: The package name used by 'pip install'.
# Value: The module name used for 'import'.
REQUIRED_PACKAGES = {
    'pandas': 'pandas',
    'openpyxl': 'openpyxl',
    'requests': 'requests',
    'beautifulsoup4': 'bs4',
    'tqdm': 'tqdm',
    'questionary': 'questionary',
    'ldap3': 'ldap3'
}

def run_command(command):
    """Executes a console command, hiding its output for a cleaner interface."""
    try:
        # Use check_call to ensure errors are caught.
        subprocess.check_call(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def check_and_install_packages():
    """
    Verifies all required packages are installed and up-to-date.
    Installs any missing packages and upgrades existing ones.
    """
    print("--- Checking for required Python libraries ---")
    
    # Get a set of all currently installed packages for quick lookups.
    installed_packages = {pkg.key for pkg in pkg_resources.working_set}
    
    for pkg_name, import_name in REQUIRED_PACKAGES.items():
        print(f"Checking for '{pkg_name}'...", end='', flush=True)
        
        # Check if the package is already installed.
        if pkg_name in installed_packages:
            print(" Found. Checking for updates...")
            # If found, run pip install with --upgrade.
            run_command([sys.executable, "-m", "pip", "install", "--upgrade", pkg_name])
        else:
            print(" Missing. Installing now...")
            # If not found, run pip install.
            if not run_command([sys.executable, "-m", "pip", "install", pkg_name]):
                print(f"ERROR: Failed to install '{pkg_name}'. Please install it manually.")

    print("\n--- Prerequisite check complete. ---")

if __name__ == "__main__":
    check_and_install_packages()
    # Pause the window so the user can see the final status.
    input("Press Enter to continue...")