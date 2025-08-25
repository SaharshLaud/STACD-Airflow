# STACD-Airflow
 Executable geospatial workflows using the STACD spec: define DAGs in YAML, run on Airflow, capture lineage/provenance automatically, and selectively recompute affected branches on updates.

## Overview

STACD (STAC Extension with DAGs) extends the SpatioTemporal Asset Catalog (STAC) specification to support complex geospatial workflow management with complete lineage tracking. This reference implementation uses Apache Airflow to demonstrate:

- **Workflow Definition**: Define geospatial processing workflows in YAML using STACD specifications
- **Selective Recomputation**: Automatically identify and recompute only affected datasets when algorithms or data are updated
- **Complete Lineage Tracking**: Maintain full provenance information for all datasets and processing steps
- **Version Management**: Track versions of algorithms, datasets, and workflows
- **Error Recovery**: Resume execution from failure points

## Repository Structure

```
STACD-Airflow/
├── requirements.txt              # Python dependencies
├── stacd_classes.py             # STACD class definitions (DAG, Algorithm_Type, Dataset_Type, etc.)
├── stacd_db_handler.py          # Database operations and schema management
├── stacd_db_init.py            # Database initialization from YAML repositories
├── stacd_dag_generator.py      # Generate Airflow DAGs from STACD YAML specifications
├── stacd_dag.yaml              # Example STACD workflow definition (v1.0)
├── stacd_dag_v2.yaml           # Example STACD workflow definition (v2.0) - for updatedag testing
├── stacd_algorithm_repo.yaml   # Algorithm instance repository
└── stacd_dataset_repo.yaml     # Dataset instance repository
```

## File Descriptions

### Core Implementation Files

- **`stacd_classes.py`**: Defines the STACD class structure including `DAG`, `Algorithm_Type`, `Dataset_Type`, `Algorithm_Instance`, and `Dataset_Instance` classes with YAML serialization support.

- **`stacd_db_handler.py`**: Manages the SQLite database schema and operations. Creates tables for algorithm records, dataset records, execution logs, DAG records, and run configurations. Provides functions for version management and lineage tracking.

- **`stacd_db_init.py`**: Initializes the STACD database from YAML repository files. Loads algorithm and dataset instances into the database for workflow execution.

- **`stacd_dag_generator.py`**: Converts STACD YAML workflow definitions into executable Airflow DAG Python files. Handles dependency resolution, task creation, and database integration.

### Configuration Files

- **`stacd_dag.yaml`**: Example terrain and LULC processing workflow definition with 5 algorithms and 8 dataset types.

- **`stacd_dag_v2.yaml`**: Updated version of the workflow for testing DAG update functionality.

- **`stacd_algorithm_repo.yaml`**: Repository of algorithm instances with version information and asset links.

- **`stacd_dataset_repo.yaml`**: Repository of root input dataset instances required for workflow execution.

## Installation & Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Initialize STACD Database

Initialize the database with algorithm and dataset repositories:

```bash
python3 stacd_db_init.py stacd_algorithm_repo.yaml stacd_dataset_repo.yaml
```

### 3. Generate Airflow DAG

Create an Airflow DAG from the STACD workflow definition:

```bash
python3 stacd_dag_generator.py stacd_dag.yaml
```

This will generate `terrain_and_lulc_workflow_stacd_dag.py` in the `../dags/` directory (or current directory if dags folder doesn't exist).

### 4. Setup Airflow Directory Structure

Create the required Airflow directory structure:

```bash
mkdir -p ~/airflow/dags
mkdir -p ~/airflow/logs
mkdir -p ~/airflow/plugins

# Move generated DAG to Airflow dags directory
mv *_stacd_dag.py ~/airflow/dags/

# Copy STACD files to Airflow directory for imports
cp stacd_*.py ~/airflow/
```

### 5. Start Airflow

Start Airflow in standalone mode:

```bash
cd ~/airflow
airflow standalone
```

**Access Airflow UI**: Open your browser and go to `http://localhost:8080`

Default credentials are usually displayed in the terminal output when Airflow starts.

### 6. Database Monitoring (Optional)

Monitor the STACD database using sqlite_web:

```bash
sqlite_web ~/airflow/stacd_database.db --port 8081
```

**Access Database UI**: Open your browser and go to `http://localhost:8081`

## Usage

### Execution Types

STACD supports several execution types for different workflow scenarios:

#### 1. Full Execution
Execute the entire workflow from scratch:

```json
{
  "execution_type": "fullexec",
  "region": "Jharkhand_Dumka_Masalia",
  "year": "2018",
  "model": "IndiaSAT_CI_PANINDIA"
}
```

#### 2. Algorithm Update
Selectively recompute when an algorithm is updated:

```json
{
  "execution_type": "updatealgo",
  "updated_entity": "LULC_Algorithm",
  "version": "2",
  "region": "Jharkhand_Dumka_Masalia",
  "year": "2018",
  "model": "IndiaSAT_CI_PANINDIA"
}
```

#### 3. Dataset Update
Selectively recompute when input data is updated:

```json
{
  "execution_type": "updatedata",
  "updated_entity": "COPERNICUS/S2_HARMONIZED",
  "version": "2",
  "region": "Jharkhand_Dumka_Masalia",
  "year": "2018",
  "model": "IndiaSAT_CI_PANINDIA"
}
```

#### 4. Resume Execution
Resume from a failed task:

```json
{
  "execution_type": "resumeexec",
  "region": "Jharkhand_Dumka_Masalia",
  "year": "2018",
  "model": "IndiaSAT_CI_PANINDIA"
}
```

#### 5. DAG Update
Handle workflow definition updates:

```bash
# First, generate the updated DAG
python3 stacd_dag_generator.py stacd_dag_v2.yaml

# Move to Airflow dags directory
mv *_stacd_dag.py ~/airflow/dags/

# Trigger with updatedag execution type
{
  "execution_type": "updatedag",
  "region": "Jharkhand_Dumka_Masalia",
  "year": "2018",
  "model": "IndiaSAT_CI_PANINDIA"
}
```

### Running Workflows

1. **Access Airflow UI** at `http://localhost:8080`
2. **Find your DAG** (e.g., "terrain_and_lulc_workflow")
3. **Click the DAG** to view details
4. **Trigger DAG** with custom configuration using the play button
5. **Paste execution JSON** in the configuration field
6. **Monitor execution** through the Airflow UI

### Monitoring and Debugging

#### Airflow Logs
- **Location**: `~/airflow/logs/`
- **Task Logs**: Available in Airflow UI under each task instance

#### Database Monitoring
- **SQLite Web UI**: `http://localhost:8081`
- **Tables to Monitor**:
  - `algorithm_records`: Algorithm versions and metadata
  - `dataset_records`: Dataset versions and lineage
  - `execution_logs`: Task execution history
  - `dags`: Workflow definitions and versions
  - `run_config`: Execution configurations

#### Key Database Queries
```sql
-- View all algorithm versions
SELECT * FROM algorithm_records ORDER BY name, version;

-- View dataset lineage for a specific region
SELECT * FROM dataset_records WHERE did = 'Jharkhand_Dumka_Masalia';

-- View execution history
SELECT * FROM execution_logs ORDER BY timestamp DESC;

-- View DAG versions
SELECT * FROM dags ORDER BY id, version;
```

## Features

### Automatic Lineage Tracking
- Every dataset execution records its creating algorithm, version, and input datasets
- Complete upstream lineage can be reconstructed for any output
- Lineage visualization available through Airflow UI plugins

### Selective Recomputation
- **Algorithm Updates**: Only downstream datasets are recomputed
- **Dataset Updates**: Only algorithms using the updated data are re-executed
- **Dependency Resolution**: Automatic identification of affected workflow components

### Version Management
- **Algorithm Versioning**: Track code changes and execution environment
- **Dataset Versioning**: Maintain multiple versions of datasets with full provenance
- **DAG Versioning**: Handle workflow definition updates intelligently

### Error Recovery
- **Failure Tracking**: Automatic storage of failure information
- **Resume Capability**: Restart from exact failure point
- **State Preservation**: Maintain execution context across recoveries

## Example Workflow

The included example processes terrain and land-use/land-cover (LULC) data:

1. **Input Data**: DEM, Sentinel satellite imagery, Machine learning models
2. **Processing**: Terrain classification, LULC classification
3. **Vectorization**: Convert raster outputs to vector formats
4. **Cross-Analysis**: Combine terrain and LULC results

This workflow demonstrates:
- **Multi-input algorithms** (LULC uses both satellite data and ML models)
- **Parallel processing** (Terrain and LULC can run simultaneously)
- **Complex dependencies** (Vectorization depends on classification outputs)
- **Cross-product analysis** (Final step combines both processing branches)

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all STACD files are in the Airflow directory or Python path
2. **Database Permissions**: Check write permissions for the database file location
3. **DAG Not Appearing**: Verify DAG file syntax and check Airflow scheduler logs
4. **Task Failures**: Check task logs in Airflow UI and database execution_logs table

### Debug Mode

Enable detailed logging by adding to your DAG trigger configuration:
```json
{
  "execution_type": "fullexec",
  "debug": true,
  ...
}
```

## Contributing

This is a reference implementation of the STACD specification. For the complete research paper and technical details, see the [STACD paper](https://doi.org/10.1145/3759536.3763803).


***

For questions and issues, please refer to the research paper for detailed technical information.
[8](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/50540967/85246215-6569-41be-b172-714da06caaa5/stacd_algorithm_repo.yaml)
[9](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/50540967/7b91a5c4-c42f-478b-a7aa-bd0912f9ea16/stacd_classes.py)
[10](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/50540967/8f2b1c4c-f931-4231-9e85-180f51953976/stacd_dag.yaml)
