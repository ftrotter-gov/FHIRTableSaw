# HAPI Loader Study

This directory contains tools for loading FHIR resources into a HAPI FHIR server.

## Quick Start

The unified `hapi_manager.py` tool provides all functionality for managing HAPI FHIR Docker containers:

```bash
# Create and start a new instance
python hload/hapi_manager.py run --name my-hapi --port 8081 --data-dir /path/to/storage

# List all tracked instances
python hload/hapi_manager.py list

# Get detailed info about an instance
python hload/hapi_manager.py info --name my-hapi

# Load implementation guide
python hload/hapi_manager.py load-ig --name my-hapi

# Bulk import NDJSON files
python hload/hapi_manager.py bulk-import /path/to/ndjson --name my-hapi

# Delete instance (keeps data)
python hload/hapi_manager.py delete --name my-hapi

# Delete instance and all data (requires confirmation)
python hload/hapi_manager.py delete --name my-hapi --remove-data
```

## Main Tool

### hapi_manager.py

Unified CLI tool for managing HAPI FHIR Docker containers with instance tracking.

**Features:**

- **Instance tracking**: Automatically tracks containers in `.env` registry
- **Port conflict detection**: Prevents starting containers on already-used ports
- **Custom storage locations**: Store container data anywhere on your system
- **Named instances**: Reference containers by name instead of remembering ports
- **Integrated commands**: All HAPI operations in one tool
- **Shell command wrapping**: Uses proven bash scripts under the hood

**Requirements:**

- Docker installed and running
- Python 3.6+
- `hapi-fhir-cli` installed for bulk import operations

#### Commands

##### run - Create/Start Container

Start or create a HAPI FHIR container with custom name, port, and storage location.

```bash
python hload/hapi_manager.py run --name CONTAINER_NAME --port PORT --data-dir DATA_DIR
```

**Arguments:**

- `--name`: Unique container name (required)
- `--port`: Port to expose (required, must be unique)
- `--data-dir`: Data directory path - can be relative or absolute (required)

**What it does:**

- Creates `{data-dir}/_container_storage/` for Docker volume mount
- Allows you to keep README files and notes in `{data-dir}/`
- Registers instance in `.env` tracking file
- Checks for port conflicts with other tracked instances

**Example:**

```bash
python hload/hapi_manager.py run --name my-hapi --port 8081 --data-dir /Volumes/eBolt/hapi_instances/my_hapi
```

This creates:

- `/Volumes/eBolt/hapi_instances/my_hapi/_container_storage/` (Docker volume)
- Container accessible at `http://localhost:8081/fhir`

##### delete - Remove Container

Stop and remove a HAPI FHIR container, optionally removing its data.

```bash
python hload/hapi_manager.py delete --name CONTAINER_NAME [--remove-data]
```

**Arguments:**

- `--name`: Container name (required)
- `--data-dir`: Data directory path (optional, retrieved from registry if not specified)
- `--remove-data`: Remove data directory (requires typing 'DELETE' to confirm)

**Example:**

```bash
# Delete container but keep data
python hload/hapi_manager.py delete --name my-hapi

# Delete container and all data (with confirmation prompt)
python hload/hapi_manager.py delete --name my-hapi --remove-data
```

##### list - Show All Instances

List all tracked HAPI FHIR instances from the registry.

```bash
python hload/hapi_manager.py list
```

**Output example:**

```text
====================================================================================================
TRACKED HAPI FHIR INSTANCES
====================================================================================================
NAME                      PORT     STATUS       DATA_DIR
----------------------------------------------------------------------------------------------------
my-hapi                   8081     running      /Volumes/eBolt/hapi_instances/my_hapi
test-instance             8082     running      /tmp/test_hapi
====================================================================================================
Total: 2 instance(s)
```

##### info - Show Instance Details

Display detailed information about a specific instance.

```bash
python hload/hapi_manager.py info --name CONTAINER_NAME
```

**Example:**

```bash
python hload/hapi_manager.py info --name my-hapi
```

**Output:**

- Port number
- Data directory location
- Storage path
- Server URL
- Current Docker container status (running/stopped/not found)

##### load-ig - Upload Implementation Guide

Upload an implementation guide package to a HAPI server.

```bash
python hload/hapi_manager.py load-ig IG_FILE --name CONTAINER_NAME
# OR
python hload/hapi_manager.py load-ig IG_FILE --port PORT
```

**Arguments:**

- `IG_FILE`: Path to IG package file (.tgz) - positional, required
- `--name`: Container name (port looked up from registry)
- `--port`: Port number (if not using --name)

**Example:**

```bash
# Using tracked instance name
python hload/hapi_manager.py load-ig ./ndh_package.tgz --name my-hapi

# Using port directly
python hload/hapi_manager.py load-ig /path/to/my-ig.tgz --port 8081

# With absolute path
python hload/hapi_manager.py load-ig /Volumes/eBolt/packages/ndh_package.tgz --name my-hapi
```

##### bulk-import - Import NDJSON Files

Bulk import NDJSON files into a HAPI FHIR server using `hapi-fhir-cli`.

```bash
python hload/hapi_manager.py bulk-import SOURCE_DIR --name CONTAINER_NAME [OPTIONS]
# OR
python hload/hapi_manager.py bulk-import SOURCE_DIR --port PORT [OPTIONS]
```

**Arguments:**

- `source_dir`: Directory containing NDJSON files (required)
- `--name`: Container name (port looked up from registry)
- `--port`: Port number (if not using --name)
- `--cli-path`: Path to hapi-fhir-cli executable (default: hapi-fhir-cli in PATH)
- `--verbose`: Enable verbose logging
- `--no-cleanup`: Keep temporary directories for debugging
- `--continue-on-error`: Continue loading even if a resource fails

**Example:**

```bash
# Using tracked instance name
python hload/hapi_manager.py bulk-import /path/to/ndjson --name my-hapi --verbose

# Using port directly with options
python hload/hapi_manager.py bulk-import /path/to/ndjson --port 8081 --continue-on-error
```

**How it works:**

- Uses `util/ndjson_discovery.py` to discover NDJSON files
- Loads resources in correct order: Organization → Location → Endpoint → Practitioner → OrganizationAffiliation → PractitionerRole
- Creates temporary subdirectories with symlinks (no file copying)
- Wraps the existing `bulk_import_loader.py` functionality

## Instance Registry

The `.env` file in the `hload/` directory tracks all container instances:

**Format:** `NAME|PORT|DATA_DIR|STATUS`

**Example:**

```text
my-hapi|8081|/Volumes/eBolt/hapi_instances/my_hapi|running
test-instance|8082|/tmp/test_hapi|running
```

**Note:** This file is auto-managed by `hapi_manager.py`. Do not edit manually.

## Data Directory Structure

When you create an instance with `--data-dir /path/to/my_instance`, the structure is:

```text
/path/to/my_instance/
├── _container_storage/          # Docker volume mount (PostgreSQL data)
├── README.md                    # Your notes (optional)
└── config.txt                   # Your configuration (optional)
```

The `_container_storage/` directory is used by Docker. You can add other files to the parent directory for documentation, configuration, or notes.

## Legacy Scripts

The original shell scripts are preserved for reference:

- `run_hapi_docker.sh` - Start HAPI container (replaced by `hapi_manager.py run`)
- `hard_delete_hapi_docker.sh` - Delete container (replaced by `hapi_manager.py delete`)
- `load_ig.sh` - Load IG (replaced by `hapi_manager.py load-ig`)
- `bulk_import_loader.py` - Bulk import (wrapped by `hapi_manager.py bulk-import`)

**Note:** The legacy scripts do not support instance tracking or custom storage locations. Use `hapi_manager.py` for all new workflows.

## File Naming Convention

All tools follow the naming conventions from `NamingConventions.md`:

- `ResourceType.ndjson` (exact match, preferred)
- `ResourceType.descriptor.ndjson` (with descriptor)
- `ResourceType.descriptor1.descriptor2.ndjson` (multiple descriptors)

Only `.ndjson` files are processed (NOT `.ndjson.gz`)

## Troubleshooting

### Port Already in Use

If you get a "port already in use" error:

1. Check tracked instances: `python hload/hapi_manager.py list`
2. Choose a different port or stop the conflicting container

### Container Not Starting

Check Docker logs:

```bash
docker logs CONTAINER_NAME
```

### Instance Not Found in Registry

If an instance isn't tracked but the container exists:

```bash
# List Docker containers
docker ps -a

# Use --port directly instead of --name
python hload/hapi_manager.py load-ig --port 8081
```

### Cleaning Up Registry

If the registry gets out of sync with actual Docker containers, you can manually edit `hload/.env` or delete it to start fresh.

## Examples

### Complete Workflow

```bash
# 1. Create a new HAPI instance
python hload/hapi_manager.py run \
  --name production-hapi \
  --port 8081 \
  --data-dir /Volumes/eBolt/hapi_prod

# 2. Wait for container to be ready (check logs)
docker logs production-hapi

# 3. Load implementation guide
python hload/hapi_manager.py load-ig ./ndh_package.tgz --name production-hapi

# 4. Bulk import NDJSON data
python hload/hapi_manager.py bulk-import \
  /Volumes/eBolt/palantir/ndjson/initial \
  --name production-hapi \
  --verbose

# 5. Check instance info
python hload/hapi_manager.py info --name production-hapi

# 6. Access FHIR server
# http://localhost:8081/fhir
```

### Multiple Instances

```bash
# Development instance
python hload/hapi_manager.py run \
  --name dev-hapi \
  --port 8081 \
  --data-dir ./hapi_dev

# Testing instance
python hload/hapi_manager.py run \
  --name test-hapi \
  --port 8082 \
  --data-dir ./hapi_test

# Production instance
python hload/hapi_manager.py run \
  --name prod-hapi \
  --port 8083 \
  --data-dir /Volumes/eBolt/hapi_prod

# List all instances
python hload/hapi_manager.py list
```

=====================
