#!/usr/bin/env python3
"""
Bulk Import Loader using hapi-fhir-cli

This script uses the hapi-fhir-cli bulk-import command to load NDJSON files
into a HAPI FHIR server in a specific order to maintain referential integrity.

Uses util/ndjson_discovery.py to discover files following naming conventions.
"""

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path to import util module
sys.path.insert(0, str(Path(__file__).parent.parent))

from util.ndjson_discovery import find_ndjson_files


class BulkImportLoader:
    """
    Loads FHIR resources using hapi-fhir-cli bulk-import command.
    
    Creates temporary subdirectories with symlinks to load resources
    one type at a time in a specific order.
    """
    
    # Resource loading order to maintain referential integrity
    RESOURCE_ORDER = [
        "Organization",
        "Location", 
        "Endpoint",
        "Practitioner",
        "OrganizationAffiliation",
        "PractitionerRole",
    ]
    
    @staticmethod
    def validate_cli_available(*, cli_path: str) -> bool:
        """
        Check if hapi-fhir-cli is available and executable.
        
        Args:
            cli_path: Path to hapi-fhir-cli executable
            
        Returns:
            True if CLI is available, False otherwise
        """
        # First check if command is in PATH using shutil.which
        resolved_path = shutil.which(cli_path)
        if resolved_path is None:
            return False
        
        # Then verify it runs (hapi-fhir-cli uses 'help' not '--help')
        try:
            result = subprocess.run(
                [cli_path, "help"],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError, PermissionError):
            return False
    
    @staticmethod
    def _create_temp_directory(*, source_dir: Path, resource_type: str) -> Path:
        """
        Create temporary subdirectory for a specific resource type.
        
        Args:
            source_dir: Source directory containing NDJSON files
            resource_type: FHIR resource type
            
        Returns:
            Path to temporary directory
        """
        temp_dir = source_dir / f".bulk_import_tmp_{resource_type}"
        temp_dir.mkdir(exist_ok=True)
        return temp_dir
    
    @staticmethod
    def _create_symlink(*, source_file: Path, temp_dir: Path) -> Path:
        """
        Create symlink in temporary directory.
        
        Args:
            source_file: Original NDJSON file
            temp_dir: Temporary directory
            
        Returns:
            Path to created symlink
        """
        symlink_path = temp_dir / source_file.name
        
        # Remove existing symlink if present
        if symlink_path.exists() or symlink_path.is_symlink():
            symlink_path.unlink()
        
        # Create new symlink
        symlink_path.symlink_to(source_file.absolute())
        return symlink_path
    
    @staticmethod
    def _cleanup_temp_directory(*, temp_dir: Path, verbose: bool = False) -> None:
        """
        Remove temporary directory and all contents.
        
        Args:
            temp_dir: Temporary directory to remove
            verbose: Enable verbose logging
        """
        if temp_dir.exists():
            if verbose:
                print(f"  🧹 Cleaning up: {temp_dir}")
            shutil.rmtree(temp_dir)
    
    @staticmethod
    def _run_bulk_import(
        *,
        cli_path: str,
        temp_dir: Path,
        target_url: str,
        port: int,
        fhir_version: str,
        verbose: bool = False
    ) -> subprocess.CompletedProcess:
        """
        Execute hapi-fhir-cli bulk-import command.
        
        Args:
            cli_path: Path to hapi-fhir-cli executable
            temp_dir: Directory containing NDJSON files to import
            target_url: Target HAPI server URL
            port: Port for CLI temporary server
            fhir_version: FHIR version (e.g., 'r4')
            verbose: Enable verbose logging
            
        Returns:
            CompletedProcess result
            
        Raises:
            subprocess.CalledProcessError: If command fails
        """
        command = [
            cli_path,
            "bulk-import",
            "-v", fhir_version,
            "--port", str(port),
            "--source-directory", str(temp_dir),
            "--target-base", target_url
        ]
        
        if verbose:
            print(f"  🔧 Command: {' '.join(command)}")
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False  # We'll handle errors manually
        )
        
        return result
    
    @staticmethod
    def load_resources(
        *,
        source_dir: Path,
        cli_path: str = "hapi-fhir-cli",
        target_url: str = "http://localhost:8080/fhir",
        port: int = 9090,
        fhir_version: str = "r4",
        cleanup: bool = True,
        verbose: bool = False,
        stop_on_error: bool = True
    ) -> Dict[str, bool]:
        """
        Load FHIR resources using hapi-fhir-cli bulk-import.
        
        Args:
            source_dir: Directory containing NDJSON files
            cli_path: Path to hapi-fhir-cli executable
            target_url: Target HAPI server URL
            port: Port for CLI temporary server
            fhir_version: FHIR version
            cleanup: Whether to cleanup temporary directories
            verbose: Enable verbose logging
            stop_on_error: Stop on first error
            
        Returns:
            Dictionary mapping resource type to success status
            
        Raises:
            FileNotFoundError: If source directory doesn't exist
            RuntimeError: If hapi-fhir-cli is not available
        """
        if not source_dir.exists():
            raise FileNotFoundError(
                f"bulk_import_loader.py Error: Source directory not found: {source_dir}"
            )
        
        if not source_dir.is_dir():
            raise ValueError(
                f"bulk_import_loader.py Error: Not a directory: {source_dir}"
            )
        
        # Validate CLI is available
        if not BulkImportLoader.validate_cli_available(cli_path=cli_path):
            raise RuntimeError(
                f"bulk_import_loader.py Error: hapi-fhir-cli not found or not executable: {cli_path}"
            )
        
        print("=" * 80)
        print("HAPI FHIR Bulk Import Loader")
        print("=" * 80)
        print(f"Source Directory:  {source_dir}")
        print(f"Target Server:     {target_url}")
        print(f"CLI Path:          {cli_path}")
        print(f"CLI Port:          {port}")
        print(f"FHIR Version:      {fhir_version}")
        print(f"Resource Order:    {', '.join(BulkImportLoader.RESOURCE_ORDER)}")
        print("=" * 80)
        print()
        
        # Discover NDJSON files
        print("🔍 Discovering NDJSON files...")
        discovered_files = find_ndjson_files(
            directory=source_dir,
            resource_types=BulkImportLoader.RESOURCE_ORDER
        )
        
        if not discovered_files:
            print("  ⚠️  No matching NDJSON files found")
            return {}
        
        print(f"  ✓ Found {len(discovered_files)} resource types:")
        for resource_type, file_path in discovered_files.items():
            print(f"    • {resource_type}: {file_path.name}")
        print()
        
        # Load resources in order
        print("🚀 Starting bulk import process...")
        print()
        
        results: Dict[str, bool] = {}
        temp_directories: List[Path] = []
        
        for resource_type in BulkImportLoader.RESOURCE_ORDER:
            if resource_type not in discovered_files:
                print(f"⏭️  Skipping {resource_type} (no file found)")
                print()
                continue
            
            source_file = discovered_files[resource_type]
            print(f"📂 Loading {resource_type} from: {source_file.name}")
            
            temp_dir = None
            try:
                # Create temporary directory and symlink
                temp_dir = BulkImportLoader._create_temp_directory(
                    source_dir=source_dir,
                    resource_type=resource_type
                )
                temp_directories.append(temp_dir)
                
                if verbose:
                    print(f"  📁 Created temp directory: {temp_dir}")
                
                symlink = BulkImportLoader._create_symlink(
                    source_file=source_file,
                    temp_dir=temp_dir
                )
                
                if verbose:
                    print(f"  🔗 Created symlink: {symlink}")
                
                # Run bulk import - ALWAYS show the command
                command = [
                    cli_path,
                    "bulk-import",
                    "-v", fhir_version,
                    "--port", str(port),
                    "--source-directory", str(temp_dir),
                    "--target-base", target_url
                ]
                print(f"  🔧 Running command:")
                print(f"     {' '.join(command)}")
                print(f"  ⏳ Executing bulk import...")
                
                result = BulkImportLoader._run_bulk_import(
                    cli_path=cli_path,
                    temp_dir=temp_dir,
                    target_url=target_url,
                    port=port,
                    fhir_version=fhir_version,
                    verbose=verbose
                )
                
                # Always show output
                if result.stdout:
                    print(f"\n  📋 Output:\n{result.stdout}")
                if result.stderr:
                    print(f"\n  ⚠️  Stderr:\n{result.stderr}")
                
                if result.returncode == 0:
                    print(f"  ✅ Successfully imported {resource_type}")
                    results[resource_type] = True
                else:
                    print(f"  ❌ Failed to import {resource_type} (exit code: {result.returncode})")
                    results[resource_type] = False
                    
                    if stop_on_error:
                        print(f"\n⚠️  Stopping on error (--stop-on-error is enabled)")
                        break
                
                # Cleanup temp directory if requested
                if cleanup:
                    BulkImportLoader._cleanup_temp_directory(
                        temp_dir=temp_dir,
                        verbose=verbose
                    )
                    temp_directories.remove(temp_dir)
                
            except Exception as error:
                print(f"  ❌ Exception during import: {error}")
                results[resource_type] = False
                
                if stop_on_error:
                    print(f"\n⚠️  Stopping on error")
                    break
            
            print()
        
        # Final cleanup if needed
        if cleanup:
            for temp_dir in temp_directories:
                BulkImportLoader._cleanup_temp_directory(
                    temp_dir=temp_dir,
                    verbose=verbose
                )
        
        # Print summary
        print("=" * 80)
        print("IMPORT SUMMARY")
        print("=" * 80)
        successful = sum(1 for success in results.values() if success)
        failed = sum(1 for success in results.values() if not success)
        print(f"Successful: {successful}")
        print(f"Failed:     {failed}")
        print(f"Skipped:    {len(BulkImportLoader.RESOURCE_ORDER) - len(results)}")
        print("=" * 80)
        
        if failed > 0:
            print("\n⚠️  Some resources failed to import. Check the logs above for details.")
        else:
            print("\n✅ All resources imported successfully!")
        
        return results


def main():
    """Main entry point for command-line execution."""
    parser = argparse.ArgumentParser(
        description="Load FHIR resources using hapi-fhir-cli bulk-import",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s /path/to/ndjson/files
  %(prog)s /path/to/ndjson --target-url http://localhost:8080/fhir
  %(prog)s /path/to/ndjson --port 9090 --verbose
  %(prog)s /path/to/ndjson --cli-path /usr/local/bin/hapi-fhir-cli --no-cleanup
        """
    )
    
    parser.add_argument(
        "source_dir",
        type=Path,
        help="Directory containing NDJSON files"
    )
    
    parser.add_argument(
        "--cli-path",
        type=str,
        default="hapi-fhir-cli",
        help="Path to hapi-fhir-cli executable (default: hapi-fhir-cli in PATH)"
    )
    
    parser.add_argument(
        "--target-url",
        type=str,
        default="http://localhost:8080/fhir",
        help="Target HAPI FHIR server URL (default: http://localhost:8080/fhir)"
    )
    
    parser.add_argument(
        "--port",
        type=int,
        default=9090,
        help="Port for CLI temporary server (default: 9090)"
    )
    
    parser.add_argument(
        "--fhir-version",
        type=str,
        default="r4",
        choices=["dstu2", "dstu3", "r4", "r5"],
        help="FHIR version (default: r4)"
    )
    
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Keep temporary directories (for debugging)"
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue loading even if a resource fails"
    )
    
    args = parser.parse_args()
    
    try:
        results = BulkImportLoader.load_resources(
            source_dir=args.source_dir,
            cli_path=args.cli_path,
            target_url=args.target_url,
            port=args.port,
            fhir_version=args.fhir_version,
            cleanup=not args.no_cleanup,
            verbose=args.verbose,
            stop_on_error=not args.continue_on_error
        )
        
        # Exit with error code if any imports failed
        failed_count = sum(1 for success in results.values() if not success)
        sys.exit(1 if failed_count > 0 else 0)
        
    except Exception as error:
        print(f"\n❌ Fatal error: {error}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
