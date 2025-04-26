#!/usr/bin/env python3
"""
ZIP Extractor

This script extracts data ZIP archives in a specified directory.

Usage:
  python extract_youtube_data_zips.py input_directory [output_directory]

If output_directory is not specified, files will be extracted to the same directory.

The script will:
1. Find all ZIP files in the input directory
2. Extract each ZIP file to the output directory
3. Maintain directory structure within ZIP files
"""

import os
import sys
import zipfile
import logging
from pathlib import Path
from typing import List, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class YouTubeDataExtractor:
    """
    Class to extract YouTube data ZIP files.
    """
    
    def __init__(self, input_dir: str, output_dir: Optional[str] = None):
        """
        Initialize the extractor with input and output directories.
        
        Args:
            input_dir: Path to directory containing ZIP files
            output_dir: Path where extracted files will be placed (defaults to input_dir)
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else self.input_dir
        
    def extract(self) -> None:
        """
        Main extraction function that orchestrates the workflow.
        """
        logger.info(f"Looking for ZIP files in {self.input_dir}")
        logger.info(f"Output will be extracted to {self.output_dir}")
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Find all zip files
        zip_files = self._find_zip_files()
        if not zip_files:
            logger.error(f"No ZIP files found in {self.input_dir}")
            return
        
        logger.info(f"Found {len(zip_files)} ZIP files to extract")
        
        # Extract each zip file
        total_extracted = 0
        for zip_file in zip_files:
            try:
                files_extracted = self._extract_zip(zip_file)
                total_extracted += files_extracted
                logger.info(f"Extracted {files_extracted} files from {zip_file.name}")
            except Exception as e:
                logger.error(f"Error extracting {zip_file}: {str(e)}")
        
        logger.info(f"Extraction complete. Total files extracted: {total_extracted}")
    
    def _find_zip_files(self) -> List[Path]:
        """
        Find all ZIP files in the input directory.
        
        Returns:
            List of Path objects for all ZIP files found
        """
        return list(self.input_dir.glob("**/*.zip"))
    
    def _extract_zip(self, zip_path: Path) -> int:
        """
        Extract a single ZIP file.
        
        Args:
            zip_path: Path to the ZIP file to extract
            
        Returns:
            Number of files extracted
        """
        # For organization, create a subfolder with the zip file's name (without extension)
        extract_dir = self.output_dir / zip_path.stem
        extract_dir.mkdir(parents=True, exist_ok=True)
        
        files_extracted = 0
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # Get list of file names in the archive
            file_list = zip_ref.namelist()
            
            # Extract all files
            for file in file_list:
                try:
                    zip_ref.extract(file, path=extract_dir)
                    files_extracted += 1
                except Exception as e:
                    logger.warning(f"Could not extract {file}: {str(e)}")
        
        return files_extracted


def main():
    """
    Main entry point for the script.
    
    Parses command-line arguments and runs the extractor.
    """
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage: ZIPextractor.py input_directory [output_directory]")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_dir = sys.argv[2] if len(sys.argv) == 3 else None
    
    # Validate input directory exists
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)
    
    # Create extractor and run
    extractor = YouTubeDataExtractor(input_dir, output_dir)
    extractor.extract()


if __name__ == "__main__":
    main()