from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import datetime
import json
import os

# Define the base model for SQLAlchemy
Base = declarative_base()

# Algorithm records table - stores algorithm instances with STACD compliance
class AlgorithmRecord(Base):
    __tablename__ = 'algorithm_records'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)  # Algorithm Type ID from STACD
    version = Column(Integer, nullable=False)
    date = Column(DateTime, default=datetime.datetime.now)
    algorithm_type = Column(String)  # Algorithm Type name for STACD
    assets = Column(Text)  # JSON string storing code repository links
    extra = Column(Text)

    def __repr__(self):
        return f"AlgorithmRecord(name={self.name}, version={self.version}, type={self.algorithm_type})"

# Dataset records table - stores dataset instances with STACD compliance
class DatasetRecord(Base):
    __tablename__ = 'dataset_records'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)  # Dataset Type ID from STACD
    did = Column(String, nullable=False)  # Dataset instance identifier
    version = Column(Integer, nullable=False)
    algname = Column(String)  # Creating algorithm name
    algversion = Column(Integer)  # Creating algorithm version
    date = Column(DateTime, default=datetime.datetime.now)
    dataset_type = Column(String)  # Dataset Type name for STACD
    alginputs = Column(Text)  # JSON string of algorithm inputs
    params = Column(Text)  # JSON string of dataset parameters
    input_datasets = Column(Text)  # JSON array of input dataset references
    extra = Column(Text)

    def __repr__(self):
        return f"DatasetRecord(name={self.name}, did={self.did}, version={self.version})"

# Execution logs table - tracks DAG execution history
class ExecutionLog(Base):
    __tablename__ = 'execution_logs'
    
    id = Column(Integer, primary_key=True)
    run_config_id = Column(Integer, ForeignKey('run_config.id'))
    dag_run_id = Column(String)
    task_id = Column(String)
    algorithm_name = Column(String)
    algorithm_version = Column(Integer)
    dataset_name = Column(String)
    dataset_version = Column(Integer)
    status = Column(String)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    execution_type = Column(String)
    region = Column(String)
    details = Column(Text)

    def __repr__(self):
        return f"ExecutionLog(task_id={self.task_id}, status={self.status})"

# Optional DAGs table for workflow tracking
class DAGRecord(Base):
    __tablename__ = 'dags'

    pk_id = Column(Integer, primary_key=True)  # Auto-increment primary key    
    id = Column(String, nullable=False)
    name = Column(String, nullable=False)
    version = Column(String, nullable=False)
    description = Column(Text)
    params = Column(Text)  # JSON string of DAG parameters
    nodes = Column(Text)   # JSON string of algorithm and dataset nodes
    date = Column(DateTime, default=datetime.datetime.now)
    
    __table_args__ = (
        {'extend_existing': True}
    )


    def __repr__(self):
        return f"DAGRecord(id={self.id}, name={self.name}, version={self.version})"
    
# RunConfig table to store the intent of each DAG run
class RunConfig(Base):
    __tablename__ = 'run_config'
    id = Column(Integer, primary_key=True)
    dag_run_id = Column(String, unique=True, nullable=False, index=True)
    execution_type = Column(String, nullable=False)
    tasks_to_run = Column(Text) # JSON list of task IDs
    timestamp = Column(DateTime, default=datetime.datetime.now)

    def __repr__(self):
        return f"RunConfig(dag_run_id={self.dag_run_id}, type={self.execution_type})"


# Database configuration
DB_PATH = os.path.abspath('/home/sash_unix/airflow/stacd_database.db')  
engine = create_engine(f'sqlite:///{DB_PATH}')
Session = sessionmaker(bind=engine)

def init_db():
    """Create all tables in the database"""
    Base.metadata.create_all(engine)
    print(f"Database initialized at {DB_PATH}")
    print("Created tables: algorithm_records, dataset_records, execution_logs, dags")

def get_latest_algorithm_version(algorithm_name):
    """Get the latest version of an algorithm from the database"""
    session = Session()
    try:
        latest = session.query(AlgorithmRecord).filter_by(
            name=algorithm_name
        ).order_by(AlgorithmRecord.version.desc()).first()
        return latest.version if latest else None
    finally:
        session.close()

def get_latest_dataset_version(dataset_name, did):
    """Get the latest version of a dataset from the database"""
    session = Session()
    try:
        latest = session.query(DatasetRecord).filter_by(
            name=dataset_name,
            did=did
        ).order_by(DatasetRecord.version.desc()).first()
        return latest.version if latest else None
    finally:
        session.close()

def get_algorithm_record(algorithm_name, version):
    """Get a specific version of an algorithm from the database"""
    session = Session()
    try:
        algo_record = session.query(AlgorithmRecord).filter_by(
            name=algorithm_name,
            version=version
        ).first()
        return algo_record
    finally:
        session.close()

def add_algorithm_version(name, version, algorithm_type="", assets=None, date=None):
    """Add a new algorithm version to the database"""
    session = Session()
    try:
        existing = session.query(AlgorithmRecord).filter_by(
            name=name,
            version=version
        ).first()
        
        if existing:
            return existing
        
        new_algo = AlgorithmRecord(
            name=name,
            version=version,
            algorithm_type=algorithm_type,
            assets=json.dumps(assets) if assets else "{}",
            date=date or datetime.datetime.now(),
            extra="Added via STACD API"
        )
        
        session.add(new_algo)
        session.commit()
        return new_algo
    except Exception as e:
        session.rollback()
        print(f"Error adding algorithm version: {e}")
        return None
    finally:
        session.close()

def add_dataset_version(name, did, version, dataset_type="", algname="", algversion=0, 
                       alginputs=None, params=None, input_datasets=None, date=None):
    """Add a new dataset version to the database"""
    session = Session()
    try:
        existing = session.query(DatasetRecord).filter_by(
            name=name,
            did=did,
            version=version
        ).first()
        
        if existing:
            return existing
        
        new_dataset = DatasetRecord(
            name=name,
            did=did,
            version=version,
            dataset_type=dataset_type,
            algname=algname,
            algversion=algversion,
            alginputs=json.dumps(alginputs) if alginputs else "{}",
            params=json.dumps(params) if params else "{}",
            input_datasets=json.dumps(input_datasets) if input_datasets else "[]",
            date=date or datetime.datetime.now(),
            extra="Added via STACD API"
        )
        
        session.add(new_dataset)
        session.commit()
        return new_dataset
    except Exception as e:
        session.rollback()
        print(f"Error adding dataset version: {e}")
        return None
    finally:
        session.close()

def log_execution(task_id, algorithm_name, algorithm_version, dataset_name, dataset_version,
                 status, execution_type, region, details, dag_run_id, run_config_id=None):
    """Log execution details to the database"""
    session = Session()
    try:
        log_entry = ExecutionLog(
            run_config_id=run_config_id,
            dag_run_id=dag_run_id,
            task_id=task_id,
            algorithm_name=algorithm_name,
            algorithm_version=algorithm_version,
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            status=status,
            execution_type=execution_type,
            region=region,
            details=details
        )
        
        session.add(log_entry)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error logging execution: {e}")
    finally:
        session.close()

def add_run_config(dag_run_id, execution_type, tasks_to_run):
    """Add a new run configuration to the database and return its ID."""
    session = Session()
    try:
        # Check if a config for this dag_run_id already exists
        existing_run = session.query(RunConfig).filter_by(dag_run_id=dag_run_id).first()
        if existing_run:
            return existing_run.id

        new_run_config = RunConfig(
            dag_run_id=dag_run_id,
            execution_type=execution_type,
            tasks_to_run=json.dumps(tasks_to_run)
        )
        session.add(new_run_config)
        session.commit()
        return new_run_config.id
    except Exception as e:
        session.rollback()
        print(f"Error adding run config: {e}")
        return None
    finally:
        session.close()


def get_dataset_lineage(dataset_name, did, version):
    """Get complete lineage for a dataset - for visualization"""
    session = Session()
    try:
        lineage = []
        processed = set()
        
        def fetch_lineage(ds_name, ds_did, ds_version, depth=0):
            if depth > 10:  # Prevent infinite recursion
                return
            
            key = f"{ds_name}_{ds_did}_{ds_version}"
            if key in processed:
                return
            processed.add(key)
            
            # Get dataset record
            dataset = session.query(DatasetRecord).filter_by(
                name=ds_name,
                did=ds_did,
                version=ds_version
            ).first()
            
            if dataset:
                # Add to lineage
                lineage_entry = {
                    "name": dataset.name,
                    "did": dataset.did,
                    "version": dataset.version,
                    "dataset_type": dataset.dataset_type,
                    "algname": dataset.algname,
                    "algversion": dataset.algversion,
                    "input_datasets": json.loads(dataset.input_datasets) if dataset.input_datasets else []
                }
                lineage.append(lineage_entry)
                
                # Recursively fetch input datasets
                if dataset.input_datasets:
                    input_datasets = json.loads(dataset.input_datasets)
                    for input_ds in input_datasets:
                        if isinstance(input_ds, list) and len(input_ds) >= 2:
                            fetch_lineage(input_ds[0], input_ds[1], input_ds[2] if len(input_ds) > 2 else 1, depth + 1)
        
        fetch_lineage(dataset_name, did, version)
        return lineage
    finally:
        session.close()

def get_dag_nodes(dag_id, version):
    """Get all nodes for a specific DAG version"""
    session = Session()
    try:
        dag_record = session.query(DAGRecord).filter_by(id=dag_id, version=version).first()
        if dag_record and dag_record.nodes:
            return json.loads(dag_record.nodes)
        return {'algorithm_nodes': [], 'dataset_nodes': []}
    finally:
        session.close()

def add_dag_with_nodes(dag_id, name, version, description, params,
                       algorithm_nodes, dataset_nodes, # These are just IDs for algorithm and dataset nodes
                       all_algorithm_types, all_dataset_types): # NEW: Full type objects
    """Add a DAG record with node information"""
    session = Session()
    try:
        existing = session.query(DAGRecord).filter_by(id=dag_id, version=version).first()
        if existing:
            return existing

        # Store detailed algorithm and dataset type definitions
        nodes_info = {
            'algorithm_types': [
                {
                    'id': algo_type.id,
                    'name': algo_type.name,
                    'inputs': algo_type.inputs,
                    'params': algo_type.params,
                    'input_datasets': algo_type.input_datasets,
                    'outputs': algo_type.outputs
                }
                for algo_type in all_algorithm_types # Use the full objects passed
            ],
            'dataset_types': [
                {
                    'id': ds_type.id,
                    'name': ds_type.name
                }
                for ds_type in all_dataset_types # Use the full objects passed
            ]
        }

        new_dag = DAGRecord(
            id=dag_id,
            name=name,
            version=version,
            description=description,
            params=json.dumps(params),
            nodes=json.dumps(nodes_info) # Store detailed nodes info
        )

        session.add(new_dag)
        session.commit()
        return new_dag
    except Exception as e:
        session.rollback()
        print(f"Error adding DAG with nodes: {e}")
        return None
    finally:
        session.close()


def get_latest_dag_version(dag_id):
    """Get the latest version of a DAG"""
    session = Session()
    try:
        latest = session.query(DAGRecord).filter_by(id=dag_id).order_by(DAGRecord.version.desc()).first()
        return latest.version if latest else None
    finally:
        session.close()

def compare_dag_nodes(dag_id, old_version, new_version):
    
    # Compare nodes between two DAG versions and return new nodes.
    
    old_nodes = get_dag_nodes(dag_id, old_version)
    new_nodes = get_dag_nodes(dag_id, new_version)

    old_algo_node_ids = set([a['id'] for a in old_nodes.get('algorithm_types', [])])
    old_dataset_node_ids = set([d['id'] for d in old_nodes.get('dataset_types', [])])
    new_algo_node_ids = set([a['id'] for a in new_nodes.get('algorithm_types', [])])
    new_dataset_node_ids = set([d['id'] for d in new_nodes.get('dataset_types', [])])

    # Find new nodes
    added_algo_nodes = list(new_algo_node_ids - old_algo_node_ids)
    added_dataset_nodes = list(new_dataset_node_ids - old_dataset_node_ids)

    return {
        'added_algorithm_nodes': added_algo_nodes,
        'added_dataset_nodes': added_dataset_nodes,
        'old_algorithm_types': old_nodes.get('algorithm_types', []), # Pass old definitions for comparison later
        'old_dataset_types': old_nodes.get('dataset_types', [])
    }


if __name__ == '__main__':
    init_db()
