#!/usr/bin/env python3
"""Dependency checker for FHIRTableSaw.

This module checks for required Python packages at startup to prevent
mid-execution crashes due to missing dependencies.

Usage:
    from check_dependencies import require_dependencies
    require_dependencies()
"""

from __future__ import annotations

import sys
from typing import NamedTuple


class Dependency(NamedTuple):
    """A required Python package."""
    
    package: str
    import_name: str | None = None
    min_version: str | None = None
    reason: str = ""


# Core dependencies used across the project
REQUIRED_DEPENDENCIES: tuple[Dependency, ...] = (
    Dependency("sqlalchemy", "sqlalchemy", "2.0", "Database ORM"),
    Dependency("psycopg", "psycopg", "3.0", "PostgreSQL driver"),
    Dependency("duckdb", "duckdb", "0.9", "DuckDB for fast processing"),
    Dependency("httpx", "httpx", "0.24", "HTTP client for FHIR API"),
    Dependency("fhir.resources", "fhir.resources", "7.0", "FHIR resource models"),
)

# Optional but commonly used dependencies
OPTIONAL_DEPENDENCIES: tuple[Dependency, ...] = (
    Dependency("pandas", "pandas", None, "Data manipulation (used by some scripts)"),
    Dependency("great_expectations", "great_expectations", None, "Data validation (testing)"),
    Dependency("fhir4ds", "fhir4ds", None, "FHIR data science utilities"),
)


def check_package(*, package: str, import_name: str | None = None, min_version: str | None = None) -> bool:
    """Check if a package is installed and optionally meets version requirements.
    
    Args:
        package: Package name as it appears in pip
        import_name: Import name if different from package name
        min_version: Minimum required version (major.minor format)
    
    Returns:
        True if package is available and meets requirements, False otherwise
    """
    import_name = import_name or package
    
    try:
        module = __import__(import_name)
    except ImportError:
        return False
    
    if min_version:
        try:
            # Try to get version from module
            version = getattr(module, "__version__", None)
            if version:
                # Extract major.minor
                version_parts = version.split(".")[:2]
                current = ".".join(version_parts)
                
                # Simple string comparison works for major.minor
                if current < min_version:
                    return False
        except Exception:  # noqa: BLE001
            # If version check fails, assume it's okay
            pass
    
    return True


def require_dependencies(*, include_optional: bool = False, exit_on_failure: bool = True) -> bool:
    """Check for required dependencies and optionally exit if any are missing.
    
    Args:
        include_optional: Whether to check optional dependencies
        exit_on_failure: Whether to exit the program if dependencies are missing
    
    Returns:
        True if all required dependencies are available, False otherwise
    """
    missing: list[tuple[str, str]] = []
    
    # Check required dependencies
    for dep in REQUIRED_DEPENDENCIES:
        if not check_package(package=dep.package, import_name=dep.import_name, min_version=dep.min_version):
            reason = f" ({dep.reason})" if dep.reason else ""
            version = f" >={dep.min_version}" if dep.min_version else ""
            missing.append((dep.package + version, reason))
    
    # Check optional dependencies if requested
    if include_optional:
        for dep in OPTIONAL_DEPENDENCIES:
            if not check_package(package=dep.package, import_name=dep.import_name, min_version=dep.min_version):
                reason = f" ({dep.reason})" if dep.reason else ""
                version = f" >={dep.min_version}" if dep.min_version else ""
                missing.append((dep.package + version, reason + " [OPTIONAL]"))
    
    if missing:
        print("=" * 80, file=sys.stderr)
        print("ERROR: Missing required Python dependencies", file=sys.stderr)
        print("=" * 80, file=sys.stderr)
        print("\nThe following packages are not installed or don't meet version requirements:\n", file=sys.stderr)
        for pkg, reason in missing:
            print(f"  - {pkg}{reason}", file=sys.stderr)
        print("\nTo install all required dependencies, run:", file=sys.stderr)
        print("\n  pip install -r requirements.txt", file=sys.stderr)
        print("\nOr install individually:", file=sys.stderr)
        for pkg, _ in missing:
            if "[OPTIONAL]" not in pkg:
                print(f"  pip install {pkg.split()[0]}", file=sys.stderr)
        print("\n" + "=" * 80, file=sys.stderr)
        
        if exit_on_failure:
            sys.exit(1)
        return False
    
    return True


def list_dependencies() -> None:
    """Print a list of all dependencies and their status."""
    print("=" * 80)
    print("FHIRTableSaw Dependencies")
    print("=" * 80)
    
    print("\nRequired Dependencies:")
    for dep in REQUIRED_DEPENDENCIES:
        installed = check_package(package=dep.package, import_name=dep.import_name, min_version=dep.min_version)
        status = "✓ Installed" if installed else "✗ Missing"
        version = f" (>={dep.min_version})" if dep.min_version else ""
        reason = f" - {dep.reason}" if dep.reason else ""
        print(f"  {status:12} {dep.package}{version}{reason}")
    
    print("\nOptional Dependencies:")
    for dep in OPTIONAL_DEPENDENCIES:
        installed = check_package(package=dep.package, import_name=dep.import_name, min_version=dep.min_version)
        status = "✓ Installed" if installed else "○ Not installed"
        version = f" (>={dep.min_version})" if dep.min_version else ""
        reason = f" - {dep.reason}" if dep.reason else ""
        print(f"  {status:12} {dep.package}{version}{reason}")
    
    print("\n" + "=" * 80)


def main() -> int:
    """Main entry point for CLI usage."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Check FHIRTableSaw Python dependencies"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all dependencies and their status"
    )
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Include optional dependencies in checks"
    )
    
    args = parser.parse_args()
    
    if args.list:
        list_dependencies()
        return 0
    
    success = require_dependencies(
        include_optional=args.include_optional,
        exit_on_failure=False
    )
    
    if success:
        print("✓ All required dependencies are installed")
        return 0
    else:
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
