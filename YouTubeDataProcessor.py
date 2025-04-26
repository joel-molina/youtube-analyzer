#!/usr/bin/env python3
"""
YouTube Data Processor for Hadoop

This script processes YouTube data files (.txt) and converts them to a Hadoop-friendly format.
It handles special characters in IDs, adds appropriate headers, and produces TSV files ready
for import into Hadoop.

Usage:
  python process_youtube_data.py input_directory output_file

The script will:
1. Recursively find all .txt data files in the input directory
2. Parse and validate each file
3. Combine data into a single output file in TSV format with proper headers
"""

import os
import sys
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Iterator, Optional, TextIO

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Define constants
COLUMN_NAMES = [
    "video_id", "uploader", "age", "category", "length", "views", 
    "rating", "ratings_count", "comments_count"
]
# We'll dynamically add related_id columns based on the maximum found

class YouTubeDataProcessor:
    """
    Class to process YouTube data files and convert them for Hadoop ingest.
    
    This class follows the Single Responsibility Principle by focusing solely on
    processing YouTube data files, and provides methods that do one thing well.
    """
    
    def __init__(self, input_dir: str, output_path: str):
        """
        Initialize the processor with input directory and output path.
        
        Args:
            input_dir: Path to directory containing YouTube data files
            output_path: Path where the output file will be written (can be file or directory)
        """
        self.input_dir = Path(input_dir)
        self.output_path = Path(output_path)
        self.max_related_ids = 0
        
        # If output_path is a directory, create it and set default filename
        if self.output_path.suffix == "":  # No file extension - treat as directory
            self.output_path.mkdir(parents=True, exist_ok=True)
            self.output_file = self.output_path / "youtube_data.tsv"
        else:
            # Ensure parent directory exists
            self.output_path.parent.mkdir(parents=True, exist_ok=True)
            self.output_file = self.output_path
        
    def process(self) -> None:
        """
        Main processing function that orchestrates the entire workflow.
        """
        logger.info(f"Starting to process files from {self.input_dir}")
        logger.info(f"Output will be written to {self.output_file}")
        
        # Find all .txt files
        data_files = self._find_data_files()
        if not data_files:
            logger.error(f"No data files found in {self.input_dir}")
            return
        
        logger.info(f"Found {len(data_files)} data files to process")
        
        # First pass: determine the maximum number of related IDs
        self._determine_max_related_ids(data_files)
        logger.info(f"Maximum number of related IDs found: {self.max_related_ids}")
        
        # Second pass: process all files and write to output
        self._process_files(data_files)
        
        logger.info(f"Processing complete. Output written to {self.output_file}")
    
    def _find_data_files(self) -> List[Path]:
        """
        Recursively find all .txt data files in the input directory.
        
        Returns:
            List of Path objects for all .txt files found
        """
        data_files = []
        for path in self.input_dir.rglob("*.txt"):
            # Ignore files that don't seem to be data files (like logs)
            if path.name.lower() == "log.txt":
                continue
            if path.is_file():
                data_files.append(path)
        return data_files
    
    def _determine_max_related_ids(self, data_files: List[Path]) -> None:
        """
        Scan all files to determine the maximum number of related video IDs. 
        
        Args:
            data_files: List of data files to scan
        """
        for file_path in data_files:
            try:
                with file_path.open("r", encoding="utf-8", errors="replace") as f:
                    for line in f:
                        parts = line.strip().split("\t")
                        # The first 9 elements are metadata; the rest are related IDs
                        related_ids_count = len(parts) - 9
                        self.max_related_ids = max(self.max_related_ids, related_ids_count)
            except Exception as e:
                logger.warning(f"Error scanning {file_path}: {str(e)}")
    
    def _build_header(self) -> List[str]:
        """
        Build the header row based on the maximum number of related IDs found.
        
        Returns:
            List of column headers
        """
        header = COLUMN_NAMES.copy()
        for i in range(1, self.max_related_ids + 1):
            header.append(f"related_id_{i}")
        return header
    
    def _process_files(self, data_files: List[Path]) -> None:
        """
        Process all files and write to the output file.
        
        Args:
            data_files: List of data files to process
        """
        header = self._build_header()
        
        with self.output_file.open("w", encoding="utf-8", newline="") as out_file:
            writer = csv.writer(out_file, delimiter="\t", quoting=csv.QUOTE_MINIMAL)
            
            # Write header
            writer.writerow(header)
            
            total_records = 0
            for file_path in data_files:
                try:
                    records_processed = self._process_file(file_path, writer)
                    total_records += records_processed
                    logger.info(f"Processed {records_processed} records from {file_path}")
                except Exception as e:
                    logger.error(f"Error processing {file_path}: {str(e)}")
            
            logger.info(f"Total records processed: {total_records}")
    
    def _process_file(self, file_path: Path, writer: csv.writer) -> int:
        """
        Process a single file and write rows to the output.
        
        Args:
            file_path: Path to the file to process
            writer: CSV writer object for output
            
        Returns:
            Number of records processed
        """
        records_processed = 0
        
        with file_path.open("r", encoding="utf-8", errors="replace") as f:
            for line in f:
                parts = line.strip().split("\t")
                
                # Skip lines that don't match the expected format
                if len(parts) < 9:
                    logger.warning(f"Skipping malformed line in {file_path}")
                    continue
                
                # Prepare row with exact number of columns (padding with empty strings if needed)
                row = parts[:9]  # First 9 columns are metadata
                
                # Add related IDs, padding with empty strings if needed
                related_ids = parts[9:]
                row.extend(related_ids)
                
                # Pad with empty strings if we have fewer related IDs than the max
                while len(row) < 9 + self.max_related_ids:
                    row.append("")
                
                writer.writerow(row)
                records_processed += 1
        
        return records_processed


def main():
    """
    Main entry point for the script.
    
    Parses command-line arguments and runs the processor.
    """
    if len(sys.argv) != 3:
        print("Usage: python process_youtube_data.py input_directory output_file")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_file = sys.argv[2]
    
    # Validate input directory exists
    if not os.path.isdir(input_dir):
        print(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)
    
    # Create processor and run
    processor = YouTubeDataProcessor(input_dir, output_file)
    processor.process()


if __name__ == "__main__":
    main()