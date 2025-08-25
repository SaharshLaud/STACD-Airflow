import yaml
import datetime
import os
import sys
import json
from stacd_db_handler import init_db, Session, AlgorithmRecord, DatasetRecord, DAGRecord
from stacd_classes import Algorithm_Instance, Dataset_Instance, AlgorithmVersion, DatasetVersion

def load_yaml_repo(yaml_path):
    """Load YAML repository file and return parsed data"""
    with open(yaml_path, 'r') as file:
        repo_data = yaml.load(file, Loader=yaml.Loader)
    return repo_data

def initialize_database_from_repos(algrepo_path, datarepo_path, dag_yaml_path=None):
    """Initialize the database using algorithms and datasets from repository YAML files"""
    print(f"Initializing STACD database from repositories...")
    
    # Initialize database tables
    init_db()
    
    # Load algorithm repository
    if os.path.exists(algrepo_path):
        alg_repo = load_yaml_repo(algrepo_path)
        print(f"Loaded {len(alg_repo)} algorithm versions from {algrepo_path}")
    else:
        alg_repo = []
        print(f"Warning: Algorithm repository file '{algrepo_path}' not found")
    
    # Load dataset repository
    if os.path.exists(datarepo_path):
        dataset_repo = load_yaml_repo(datarepo_path)
        print(f"Loaded {len(dataset_repo)} dataset versions from {datarepo_path}")
    else:
        dataset_repo = []
        print(f"Warning: Dataset repository file '{datarepo_path}' not found")
    
    # Load DAG definition if provided
    dag_definition = None
    if dag_yaml_path and os.path.exists(dag_yaml_path):
        dag_config = load_yaml_repo(dag_yaml_path)
        for item in dag_config:
            if hasattr(item, 'id') and hasattr(item, 'name'):  # DAG object
                dag_definition = item
                break
    
    # Add to database
    session = Session()
    try:
        # Add algorithms to database
        for algo in alg_repo:
            # Check if it's new STACD format (Algorithm_Instance) or legacy format (AlgorithmVersion)
            if isinstance(algo, Algorithm_Instance):
                # New STACD format
                algorithm_name = algo.type  # Use type as name
                algorithm_version = int(algo.version)
                algorithm_assets = algo.assets if algo.assets else {}
                algorithm_date = algo.date if algo.date else datetime.datetime.now()
                
                # Check if this version already exists
                existing = session.query(AlgorithmRecord).filter_by(
                    name=algorithm_name,
                    version=algorithm_version
                ).first()
                
                if not existing:
                    new_algo = AlgorithmRecord(
                        name=algorithm_name,
                        version=algorithm_version,
                        algorithm_type=algorithm_name,
                        assets=json.dumps(algorithm_assets),
                        date=algorithm_date if isinstance(algorithm_date, datetime.datetime) else datetime.datetime.strptime(str(algorithm_date), "%Y-%m-%d %H:%M:%S"),
                        extra="Imported from STACD Algorithm_Instance"
                    )
                    session.add(new_algo)
                    print(f"Added algorithm: {algorithm_name} v{algorithm_version}")
                    
            elif isinstance(algo, AlgorithmVersion) or hasattr(algo, 'name'):
                # Legacy format
                algorithm_name = algo.name
                algorithm_version = int(algo.version)
                algorithm_date = algo.date if algo.date else datetime.datetime.now()
                
                existing = session.query(AlgorithmRecord).filter_by(
                    name=algorithm_name,
                    version=algorithm_version
                ).first()
                
                if not existing:
                    new_algo = AlgorithmRecord(
                        name=algorithm_name,
                        version=algorithm_version,
                        algorithm_type=algorithm_name,
                        assets=json.dumps({"code": f"https://github.com/algorithms/{algorithm_name}"}),
                        date=algorithm_date if isinstance(algorithm_date, datetime.datetime) else datetime.datetime.strptime(str(algorithm_date), "%Y-%m-%d %H:%M:%S"),
                        extra="Imported from legacy AlgorithmVersion"
                    )
                    session.add(new_algo)
                    print(f"Added algorithm: {algorithm_name} v{algorithm_version}")
            else:
                print(f"Warning: Unrecognized algorithm format: {type(algo)}")
        
        # Add datasets to database
        for dataset in dataset_repo:
            # Check if it's new STACD format (Dataset_Instance) or legacy format (DatasetVersion)
            if isinstance(dataset, Dataset_Instance):
                # New STACD format
                dataset_name = dataset.type  # Use type as name
                dataset_version = int(dataset.version)
                dataset_params = dataset.params if dataset.params else []
                
                # Extract region from params
                region = "default"
                if dataset_params:
                    for param in dataset_params:
                        if isinstance(param, dict) and 'region' in param:
                            region = param['region']
                            break
                
                existing = session.query(DatasetRecord).filter_by(
                    name=dataset_name,
                    did=region,
                    version=dataset_version
                ).first()
                
                if not existing:
                    new_dataset = DatasetRecord(
                        name=dataset_name,
                        did=region,
                        version=dataset_version,
                        dataset_type=dataset_name,
                        algname=dataset.alg_name if dataset.alg_name else "",
                        algversion=0,  # Will be set during execution
                        alginputs=json.dumps(dataset.alg_inputs) if dataset.alg_inputs else "{}",
                        params=json.dumps(dataset_params),
                        input_datasets=json.dumps(dataset.input_datasets) if dataset.input_datasets else "[]",
                        date=dataset.date if dataset.date else datetime.datetime.now(),
                        extra="Imported from STACD Dataset_Instance"
                    )
                    session.add(new_dataset)
                    print(f"Added dataset: {dataset_name} ({region}) v{dataset_version}")
                    
            elif isinstance(dataset, DatasetVersion) or hasattr(dataset, 'name'):
                # Legacy format
                existing = session.query(DatasetRecord).filter_by(
                    name=dataset.name,
                    did=dataset.did,
                    version=dataset.version
                ).first()
                
                if not existing:
                    new_dataset = DatasetRecord(
                        name=dataset.name,
                        did=dataset.did,
                        version=int(dataset.version),
                        dataset_type=dataset.name,
                        algname=dataset.algname,
                        algversion=int(dataset.algversion) if dataset.algversion and dataset.algversion != "" else 0,
                        alginputs=json.dumps(dataset.alginputs) if dataset.alginputs else "{}",
                        params=json.dumps({"region": dataset.did}),
                        input_datasets="[]",
                        date=dataset.date if isinstance(dataset.date, datetime.datetime) else datetime.datetime.strptime(str(dataset.date), "%Y-%m-%d %H:%M:%S"),
                        extra="Imported from legacy DatasetVersion"
                    )
                    session.add(new_dataset)
                    print(f"Added dataset: {dataset.name} ({dataset.did}) v{dataset.version}")
            else:
                print(f"Warning: Unrecognized dataset format: {type(dataset)}")
        
        # Add DAG definition if available
        '''if dag_definition:
            existing_dag = session.query(DAGRecord).filter_by(id=dag_definition.id).first()
            if not existing_dag:
                new_dag = DAGRecord(
                    id=dag_definition.id,
                    name=dag_definition.name,
                    version=dag_definition.version,
                    description=dag_definition.description,
                    params=json.dumps(dag_definition.params)
                )
                session.add(new_dag)
                print(f"Added DAG: {dag_definition.name} v{dag_definition.version}")'''
        
        session.commit()
        print("STACD database initialization complete!")
        
    except Exception as e:
        session.rollback()
        print(f"Error initializing database: {e}")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python stacd_db_init.py <algrepo_path> <datarepo_path> [dag_yaml_path]")
        sys.exit(1)
    
    algrepo_path = sys.argv[1]
    datarepo_path = sys.argv[2]
    dag_yaml_path = sys.argv[3] if len(sys.argv) > 3 else None
    
    initialize_database_from_repos(algrepo_path, datarepo_path, dag_yaml_path)
