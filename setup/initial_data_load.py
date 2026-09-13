"""
Initial Data Load Script
Downloads drug performance dataset from Kaggle and uploads to UC volume
"""
import os
import shutil
import subprocess
from pathlib import Path

def install_kagglehub():
    """Install kagglehub if not available"""
    subprocess.check_call(["pip", "install", "kagglehub", "--quiet"])

def download_and_upload():
    """Download from Kaggle and upload to Unity Catalog volume"""
    
    # Install dependency
    print("Installing kagglehub...")
    install_kagglehub()
    
    import kagglehub
    
    # Download dataset
    print("Downloading dataset from Kaggle...")
    path = kagglehub.dataset_download("thedevastator/drug-performance-evaluation")
    print(f"✓ Dataset downloaded to: {path}")
    
    # Define volume path
    volume_path = "/Volumes/drug_evaluation/landing/raw_data"
    
    # Copy CSV files to Unity Catalog volume
    source_files = [
        (f"{path}/Drug.csv", f"{volume_path}/Drug.csv"),
        (f"{path}/Drug_clean.csv", f"{volume_path}/Drug_clean.csv")
    ]
    
    print("\nCopying files to Unity Catalog volume...")
    for src, dest in source_files:
        shutil.copy2(src, dest)
        print(f"✓ Copied: {Path(dest).name}")
    
    print("\n✅ All files uploaded successfully!")
    print(f"Volume location: {volume_path}")
    
    # Verify
    print("\nFiles in volume:")
    for file in os.listdir(volume_path):
        file_path = os.path.join(volume_path, file)
        if os.path.isfile(file_path):
            size_mb = os.path.getsize(file_path) / (1024 * 1024)
            print(f"  • {file} ({size_mb:.2f} MB)")

if __name__ == "__main__":
    download_and_upload()