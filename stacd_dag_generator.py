import yaml
import os
import sys
from datetime import datetime, timedelta
from stacd_classes import DAG, Dataset_Type, Algorithm_Type, Algorithm_Instance, Dataset_Instance

def create_dag_params(params_list):
    """Create a dict of DAG params with default values from the DAG specification params list"""
    params_dict = {}
    
    # Add DAG-specific params from the specification
    for param in params_list:
        params_dict[param] = None  # Default to None, will be filled by user at trigger time
    
    # Add standard execution control params
    params_dict['execution_type'] = 'fullexec'
    params_dict['updated_entity'] = None
    params_dict['version'] = None
    
    return params_dict

def generate_stacd_dag(yaml_path, output_path):
    """Generate an Airflow DAG script from STACD YAML configuration with database integration"""
    
    # Read the YAML configuration
    with open(yaml_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.Loader)
    
    # Extract STACD components
    dag_definition = None
    dataset_types = []
    algorithm_types = []
    
    for item in config:
        if isinstance(item, DAG):
            dag_definition = item
        elif isinstance(item, Dataset_Type):
            dataset_types.append(item)
        elif isinstance(item, Algorithm_Type):
            algorithm_types.append(item)
    
    if not dag_definition:
        raise ValueError("No DAG definition found in YAML file")
    
    # Create dynamic params dict from DAG specification
    dag_params = create_dag_params(dag_definition.params)
    
    # Generate the DAG script
    dag_code = f'''
# Generated STACD DAG from {yaml_path}
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import json
import sys
import os

# Add database imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'new_version'))
from stacd_db_handler import (
    Session, AlgorithmRecord, DatasetRecord, ExecutionLog,
    get_latest_algorithm_version, get_latest_dataset_version,
    add_algorithm_version, add_dataset_version, log_execution,DAGRecord,add_run_config,RunConfig
)

# Default arguments for the DAG
default_args = {{
    'owner': 'STACD',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}}

# Create DAG instance with dynamic params
dag = DAG(
    '{dag_definition.id}',
    default_args=default_args,
    description='{dag_definition.description}',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    params={dag_params},
)

# Algorithm and Dataset type definitions for dependency resolution
algorithm_types_map = {{
'''
        
    # Add algorithm types mapping
    for algo_type in algorithm_types:
        dag_code += f''' '{algo_type.id}': {{
            'name': '{algo_type.name}',
            'inputs': {algo_type.inputs},
            'params': {algo_type.params},  # <-- ADD THIS LINE
            'input_datasets': {algo_type.input_datasets},
            'outputs': {algo_type.outputs}
        }},
'''


    dag_code += f'''}}

dataset_types_map = {{
'''

    # Add dataset types mapping
    for dataset_type in dataset_types:
        dag_code += f'''    '{dataset_type.id}': '{dataset_type.name}',
'''

    dag_code += f'''}}

def get_downstream_tasks_for_algorithm(updated_algorithm, all_tasks):
    """Get all tasks that should be executed when an algorithm is updated"""
    tasks_to_run = []
    
    # Add the algorithm task itself
    algo_task_id = f'execute_{{updated_algorithm.replace("/", "_").replace(" ", "_")}}'
    if algo_task_id in all_tasks:
        tasks_to_run.append(algo_task_id)

    
    return tasks_to_run

def get_downstream_tasks_for_dataset(updated_dataset, all_tasks):
    """Get all tasks that should be executed when a dataset is updated"""
    tasks_to_run = []
    
    # Add the dataset task itself
    dataset_task_id = f'process_{{updated_dataset.replace("/", "_").replace(" ", "_")}}'
    if dataset_task_id in all_tasks:
        tasks_to_run.append(dataset_task_id)
    
    return tasks_to_run

def handle_task_failure(context):
    """Store task failure information for resume execution"""
    import json
    from airflow.models import Variable
    
    task_id = context['task'].task_id
    dag_run_id = context['dag_run'].run_id
    execution_date = context['execution_date']
    dag_id = context['dag'].dag_id
    
    # Get execution parameters
    ti = context['ti']
    execution_params = ti.xcom_pull(task_ids='determine_execution_path', key='execution_params')
    run_config_id = ti.xcom_pull(task_ids='determine_execution_path', key='run_config_id')
    region = execution_params.get('region', 'default') if execution_params else 'default'
    execution_type = execution_params.get('execution_type', 'fullexec') if execution_params else 'fullexec'
    
    print(f"Task {{task_id}} failed. Storing failure information for recovery.")
    
    # Extract entity type and name from task_id
    entity_type = None
    entity_name = None
    
    if task_id.startswith('execute_'):
        entity_type = 'algorithm'
        entity_name = task_id[8:].replace('_', '/').replace(' ', '_')  # Remove 'execute_' prefix
        # Find the actual algorithm name from algorithm_types_map
        for algo_id in algorithm_types_map.keys():
            if task_id == f'execute_{{algo_id.replace("/", "_").replace(" ", "_")}}':
                entity_name = algo_id
                break
    elif task_id.startswith('process_'):
        entity_type = 'dataset'
        entity_name = task_id[8:].replace('_', '/').replace(' ', '_')  # Remove 'process_' prefix
        # Find the actual dataset name from dataset_types_map
        for ds_id in dataset_types_map.keys():
            if task_id == f'process_{{ds_id.replace("/", "_").replace(" ", "_")}}':
                entity_name = ds_id
                break
    
    # Store failure information
    failure_info = {{
        'task_id': task_id,
        'entity_type': entity_type,
        'entity_name': entity_name,
        'dag_id': dag_id,
        'dag_run_id': dag_run_id,
        'execution_date': execution_date.isoformat(),
        'error': str(context.get('exception', '')),
        'region': region,
        'recovery_attempts': 0,
        'execution_type': execution_type
    }}
    
    # Store in Airflow Variable with unique key
    Variable.set(
        f"failed_task_{{dag_run_id}}_{{task_id}}",
        json.dumps(failure_info)
    )
    
    # Log the failure to database
    log_execution(
        task_id=task_id,
        algorithm_name=entity_name if entity_type == 'algorithm' else "",
        algorithm_version=0,
        dataset_name=entity_name if entity_type == 'dataset' else "",
        dataset_version=0,
        status="error",
        execution_type=execution_type,
        region=region,
        details=str(context.get('exception', '')),
        dag_run_id=dag_run_id,
        run_config_id=run_config_id
    )
    
    print(f"Task {{task_id}} failure information stored. Use resumeexec to recover.")


def determine_execution_path(**context):
    """Determine which tasks to execute based on execution type with proper dependency resolution"""
    execution_type = context['dag_run'].conf.get('execution_type', context['params']['execution_type'])
    updated_entity = context['dag_run'].conf.get('updated_entity', context['params']['updated_entity'])
    version = context['dag_run'].conf.get('version', context['params']['version'])
    
    # Get all DAG-specific params dynamically
    dag_params = {{}}
    for param_name in {list(dag_definition.params)}:
        dag_params[param_name] = context['dag_run'].conf.get(param_name, context['params'][param_name])
    
    print(f"Execution Type: {{execution_type}}")
    print(f"Updated Entity: {{updated_entity}}")
    print(f"Version: {{version}}")
    print(f"DAG Parameters: {{dag_params}}")
    
    # Store execution parameters in XCom for later use
    context['ti'].xcom_push(key='execution_params', value={{
        'execution_type': execution_type,
        'updated_entity': updated_entity,
        'version': version,
        **dag_params
    }})
    
    # Handle resumeexec - get failed task info from Airflow Variables
    if execution_type == 'resumeexec':
        from airflow.models import Variable
        from airflow.settings import Session as AirflowSession
        import json
        
        # Find the most recent failure
        failure_info = None
        try:
            # Use the correct method to get all variables
            airflow_session = AirflowSession()
            try:
                # Query all variables that start with "failed_task_"
                query = airflow_session.query(Variable).filter(Variable.key.like('failed_task_%'))
                failure_vars = {{}}
                
                for var in query.all():
                    failure_vars[var.key] = var.val
                    
            finally:
                airflow_session.close()
            
            if failure_vars:
                # Get the most recent failure based on timestamp in the key
                most_recent_key = max(failure_vars.keys())
                failure_info = json.loads(failure_vars[most_recent_key])
                
                failed_task_id = failure_info.get('task_id')
                failed_region = failure_info.get('region', 'default')
                failed_entity_type = failure_info.get('entity_type')
                failed_entity_name = failure_info.get('entity_name')
                
                print(f"Resuming from failed task: {{failed_task_id}}")
                print(f"Failed entity: {{failed_entity_type}} - {{failed_entity_name}}")
                
                dag_params['region'] = failed_region
                context['ti'].xcom_push(key='execution_params', value={{
                    'execution_type': execution_type,
                    'updated_entity': failed_entity_name,
                    'version': version,
                    'failed_task_id': failed_task_id,
                    'failed_entity_type': failed_entity_type,
                    **dag_params
                }})
                
                updated_entity = failed_entity_name
                
        except Exception as e:
            print(f"Error retrieving failure info: {{e}}")
            execution_type = 'fullexec'

    

    elif execution_type == 'updatedag':
        from stacd_db_handler import get_latest_dag_version, compare_dag_nodes, add_algorithm_version, get_latest_algorithm_version
        import json

        current_dag_id = '{dag_definition.id}'
        current_dag_version = '{dag_definition.version}'

        new_nodes_info = {{
            'algorithm_nodes': [],
            'dataset_nodes': [],
            'affected_algorithm_nodes': []
        }}

        session = Session()
        try:
            all_versions_records = session.query(DAGRecord).filter_by(id=current_dag_id).all()
            existing_versions = [v.version for v in all_versions_records]

            print(f"Current DAG version: {{current_dag_version}}")
            print(f"Existing versions in DB: {{existing_versions}}")

            previous_version_record = None
            if len(existing_versions) > 1:
                try:
                    sorted_versions = sorted(existing_versions, key=lambda x: float(x))
                    current_index = sorted_versions.index(current_dag_version)
                    if current_index > 0:
                        previous_version = sorted_versions[current_index - 1]
                        previous_version_record = session.query(DAGRecord).filter_by(id=current_dag_id, version=previous_version).first()
                except (ValueError, IndexError):
                        previous_version_record = session.query(DAGRecord).filter(
                        DAGRecord.id == current_dag_id,
                        DAGRecord.version != current_dag_version
                    ).order_by(DAGRecord.version.desc()).first()

            if previous_version_record:
                print(f"Comparing DAG versions: {{previous_version_record.version}} -> {{current_dag_version}}")

                node_diff = compare_dag_nodes(current_dag_id, previous_version_record.version, current_dag_version)
                added_algo_nodes = node_diff.get('added_algorithm_nodes', [])
                added_dataset_nodes = node_diff.get('added_dataset_nodes', [])
                old_algorithm_type_defs = {{algo['id']: algo for algo in node_diff.get('old_algorithm_types', [])}}

                new_nodes_info['algorithm_nodes'].extend(added_algo_nodes)
                new_nodes_info['dataset_nodes'].extend(added_dataset_nodes)

                # --- START OF FINAL REFINED LOGIC ---

                # 1. Add version records for BRAND NEW algorithms
                for new_algo_id in added_algo_nodes:
                    new_algo_def = algorithm_types_map.get(new_algo_id)
                    if new_algo_def:
                        add_algorithm_version(
                            name=new_algo_id,
                            version=1,
                            algorithm_type=new_algo_def.get('name', new_algo_id),
                            assets={{"code": f"https://github.com/algorithms/{{new_algo_id}}/v1"}},
                        )
                        print(f"Added new algorithm record for '{{new_algo_id}}': v1")

                # 2. Identify SEMANTICALLY AFFECTED existing algorithms using a canonical string comparison
                affected_algorithm_nodes = []
                current_algo_ids = set(algorithm_types_map.keys())
                common_algo_ids = set(old_algorithm_type_defs.keys()).intersection(current_algo_ids)

                # Helper function to create a canonical, comparable JSON string of an algorithm's definition
                def get_canonical_algo_string(algo_def):
                    if not algo_def: return "{{}}"
                    # Build a new dict with all lists sorted to ensure consistent ordering
                    canonical_dict = {{
                        'name': algo_def.get('name', ''),
                        'inputs': sorted(list(set(algo_def.get('inputs', [])))),
                        'params': sorted([json.dumps(p, sort_keys=True) for p in algo_def.get('params', [])]),
                        'input_datasets': sorted(list(set(algo_def.get('input_datasets', [])))),
                        'outputs': sorted(list(set(algo_def.get('outputs', []))))
                    }}
                    # Return a sorted JSON string of the canonical dict
                    return json.dumps(canonical_dict, sort_keys=True)

                        
                for algo_id in common_algo_ids:
                    old_algo_def = old_algorithm_type_defs.get(algo_id, {{}})
                    current_algo_def = algorithm_types_map.get(algo_id, {{}})
                    
                    # Get the canonical strings for comparison
                    old_string = get_canonical_algo_string(old_algo_def)
                    new_string = get_canonical_algo_string(current_algo_def)

                    # --- AGGRESSIVE DEBUGGING BLOCK ---
                    # We will print the comparison for an algorithm we know didn't change
                    if algo_id == 'Terrain_Algorithm':
                        print("--- DEBUGGING UNCHANGED ALGORITHM: Terrain_Algorithm ---")
                        print(f"OLD_CANONICAL_STRING (from DB v1.0): {{old_string}}")
                        print(f"NEW_CANONICAL_STRING (from YAML v2.0): {{new_string}}")
                        print(f"ARE THEY EQUAL? -> {{old_string == new_string}}")
                        print("--- END DEBUGGING ---")
                    # --- END DEBUGGING BLOCK ---

                    # Compare the final canonical strings
                    if old_string != new_string:
                        affected_algorithm_nodes.append(algo_id)
                        print(f"Algorithm '{{algo_id}}' definition changed. Marking for re-execution.")

                        latest_algo_version = get_latest_algorithm_version(algo_id)
                        new_algo_version = (latest_algo_version if latest_algo_version is not None else 0) + 1
                        
                        add_algorithm_version(
                            name=algo_id,
                            version=new_algo_version,
                            algorithm_type=current_algo_def.get('name', algo_id),
                            assets={{"code": f"https://github.com/algorithms/{{algo_id}}/v{{new_algo_version}}"}}
                        )
                        print(f"Added new algorithm version for '{{algo_id}}': v{{new_algo_version}}")

                
                new_nodes_info['affected_algorithm_nodes'].extend(affected_algorithm_nodes)

                # --- END OF FINAL REFINED LOGIC ---

            else:
                print(f"No previous DAG version found. Assuming all nodes are new.")
                all_current_algos = list(algorithm_types_map.keys())
                new_nodes_info['algorithm_nodes'].extend(all_current_algos)
                new_nodes_info['dataset_nodes'].extend(list(dataset_types_map.keys()))
                # Add version 1 records for all algorithms in a new DAG
                for algo_id in all_current_algos:
                    algo_def = algorithm_types_map.get(algo_id)
                    if not get_latest_algorithm_version(algo_id):
                        add_algorithm_version(
                            name=algo_id, version=1, algorithm_type=algo_def.get('name', algo_id),
                            assets={{"code": f"https://github.com/algorithms/{{algo_id}}/v1"}}
                        )

            print(f"New algorithm nodes (IDs): {{new_nodes_info['algorithm_nodes']}}")
            print(f"New dataset nodes (IDs): {{new_nodes_info['dataset_nodes']}}")
            print(f"Affected existing algorithm nodes (IDs): {{new_nodes_info['affected_algorithm_nodes']}}")
            
            context['ti'].xcom_push(key='new_nodes', value=new_nodes_info)

        finally:
            session.close()



    
    # Handle version updates in database
    if version and updated_entity:
        if execution_type == 'updatealgo':
            algo_type = algorithm_types_map.get(updated_entity, {{}}).get('name', updated_entity)
            add_algorithm_version(
                name=updated_entity,
                version=int(version),
                algorithm_type=algo_type,
                assets={{"code": f"https://github.com/algorithms/{{updated_entity}}/v{{version}}"}}
            )
            print(f"Added algorithm version: {{updated_entity}} v{{version}}")
        
        elif execution_type == 'updatedata':
            dataset_type = dataset_types_map.get(updated_entity, updated_entity)
            region = dag_params.get('region', 'default')
            add_dataset_version(
                name=updated_entity,
                did=region,
                version=int(version),
                dataset_type=dataset_type,
                params={{"region": region}}
            )
            print(f"Added dataset version: {{updated_entity}} v{{version}}")
    
    # Get all task IDs (excluding the branch task itself)
    all_tasks = [task.task_id for task in context['dag'].tasks 
                 if not task.task_id.startswith('determine_execution_path')]
    
    # Determine which tasks to run based on execution type
    if execution_type == 'fullexec':
        tasks_to_run = all_tasks
        print(f"Full execution: running all {{len(all_tasks)}} tasks")
        
    elif execution_type == 'updatealgo' and updated_entity:
        tasks_to_run = get_downstream_tasks_for_algorithm(updated_entity, all_tasks)
        print(f"Algorithm update for {{updated_entity}}: running {{len(tasks_to_run)}} affected tasks")
        
    elif execution_type == 'updatedata' and updated_entity:
        tasks_to_run = get_downstream_tasks_for_dataset(updated_entity, all_tasks)
        print(f"Dataset update for {{updated_entity}}: running {{len(tasks_to_run)}} affected tasks")
        
    elif execution_type == 'resumeexec' and updated_entity:
        # Resume from failed entity - find all downstream tasks
        if failure_info and failure_info.get('entity_type') == 'algorithm':
            tasks_to_run = get_downstream_tasks_for_algorithm(updated_entity, all_tasks)
        elif failure_info and failure_info.get('entity_type') == 'dataset':
            tasks_to_run = get_downstream_tasks_for_dataset(updated_entity, all_tasks)
        else:
            failed_task_id = failure_info.get('task_id') if failure_info else None
            if failed_task_id and failed_task_id in all_tasks:
                tasks_to_run = [failed_task_id]
                for task in context['dag'].tasks:
                    if task.task_id == failed_task_id:
                        downstream_tasks = [t.task_id for t in task.downstream_list]
                        tasks_to_run.extend(downstream_tasks)
                        break
            else:
                tasks_to_run = all_tasks
        
        print(f"Resume execution for {{updated_entity}}: running {{len(tasks_to_run)}} tasks from failure point")
        
    elif execution_type == 'updatedag':
        # Get new nodes from XCom
        new_nodes = context['ti'].xcom_pull(task_ids='determine_execution_path', key='new_nodes')
        
        if new_nodes:
            tasks_to_run = []
            
            # Add tasks for new algorithm nodes
            for algo_node in new_nodes.get('algorithm_nodes', []):
                algo_task_id = f'execute_{{algo_node.replace("/", "_").replace(" ", "_")}}'
                if algo_task_id in all_tasks:
                    tasks_to_run.append(algo_task_id)
                    
                    # Add downstream tasks
                    downstream_tasks = get_downstream_tasks_for_algorithm(algo_node, all_tasks)
                    tasks_to_run.extend(downstream_tasks)
            
            # Add tasks for new dataset nodes
            for dataset_node in new_nodes.get('dataset_nodes', []):
                dataset_task_id = f'process_{{dataset_node.replace("/", "_").replace(" ", "_")}}'
                if dataset_task_id in all_tasks:
                    tasks_to_run.append(dataset_task_id)
                    
                    # Add downstream tasks
                    downstream_tasks = get_downstream_tasks_for_dataset(dataset_node, all_tasks)
                    tasks_to_run.extend(downstream_tasks)
            
            for affected_algo_id in new_nodes.get('affected_algorithm_nodes', []):
                algo_task_id = f'execute_{{affected_algo_id.replace("/", "_").replace(" ", "_")}}'
                if algo_task_id in all_tasks:
                    # Add the affected algorithm task itself
                    tasks_to_run.append(algo_task_id)
                    # Add its direct and indirect downstream tasks
                    downstream_tasks = get_downstream_tasks_for_algorithm(affected_algo_id, all_tasks)
                    tasks_to_run.extend(downstream_tasks)
                        
            # Remove duplicates while preserving order
            tasks_to_run = list(dict.fromkeys(tasks_to_run))
            
            print(f"UpdateDAG execution: running {{len(tasks_to_run)}} tasks for new nodes")
        else:
            # No new nodes, run nothing
            tasks_to_run = []
            print("UpdateDAG execution: no new nodes found")
        
    else:
        tasks_to_run = all_tasks
        print(f"Default execution: running all {{len(all_tasks)}} tasks")


    # --- ADD RUN CONFIG AND PUSH ID TO XCOM ---
    run_config_id = add_run_config(
        dag_run_id=context['dag_run'].run_id,
        execution_type=execution_type,
        tasks_to_run=tasks_to_run
    )
    context['ti'].xcom_push(key='run_config_id', value=run_config_id)
    print(f"Created RunConfig with ID: {{run_config_id}} for this run.")

    
    print(f"Tasks to run: {{tasks_to_run}}")
    return tasks_to_run


def process_dataset(dataset_type_id, **context):
    """Process a dataset based on its type with database integration"""
    ti = context['ti']
    execution_params = ti.xcom_pull(task_ids='determine_execution_path', key='execution_params')
    run_config_id = ti.xcom_pull(task_ids='determine_execution_path', key='run_config_id')
    
    execution_type = execution_params.get('execution_type', 'fullexec')
    region = execution_params.get('region', 'default')
    dag_run_id = context['dag_run'].run_id
    
    print(f"Processing dataset: {{dataset_type_id}}")

    # intentional failure for testing resumeexec
    #if execution_type != 'resumeexec' and dataset_type_id == 'COPERNICUS/S2_HARMONIZED':
    #    raise Exception("Intentional failure for resumeexec testing")

    print(f"Execution type: {{execution_type}}")
    print(f"Region: {{region}}")
    
    try:
        # Get dataset type name
        dataset_type_name = dataset_types_map.get(dataset_type_id, dataset_type_id)
        
        # Get or create dataset version
        latest_version = get_latest_dataset_version(dataset_type_id, region)
        if latest_version is None:
            latest_version = 1
        
        # For updates, increment version
        if execution_type in ['updatedata', 'fullexec']:
            new_version = latest_version + 1 if execution_type == 'updatedata' else latest_version
        else:
            new_version = latest_version
        
        print(f"Processing dataset {{dataset_type_id}} v{{new_version}}")
        
        # Log execution
        log_execution(
            task_id=context['task'].task_id,
            algorithm_name="",
            algorithm_version=0,
            dataset_name=dataset_type_id,
            dataset_version=new_version,
            status="success",
            execution_type=execution_type,
            region=region,
            details=f"Dataset {{dataset_type_id}} processed successfully",
            dag_run_id=dag_run_id,
            run_config_id=run_config_id
        )
        
        return f"Dataset {{dataset_type_id}} v{{new_version}} processed successfully"
        
    except Exception as e:
        # Log error
        log_execution(
            task_id=context['task'].task_id,
            algorithm_name="",
            algorithm_version=0,
            dataset_name=dataset_type_id,
            dataset_version=0,
            status="error",
            execution_type=execution_type,
            region=region,
            details=str(e),
            dag_run_id=dag_run_id,
            run_config_id=run_config_id
        )
        raise

# Inside the dag_code string in stacd_dag_generator.py

def execute_algorithm(algorithm_type_id, inputs, outputs, **context):
    """Execute an algorithm with given inputs and outputs with database integration"""
    ti = context['ti']
    execution_params = ti.xcom_pull(task_ids='determine_execution_path', key='execution_params')
    run_config_id = ti.xcom_pull(task_ids='determine_execution_path', key='run_config_id')
    
    execution_type = execution_params.get('execution_type', 'fullexec')
    region = execution_params.get('region', 'default')
    dag_run_id = context['dag_run'].run_id
    algorithm_version = 1 # Default initial value

    print(f"Executing algorithm: {{algorithm_type_id}}")
    print(f"Execution type: {{execution_type}}")
    print(f"Inputs: {{inputs}}")
    print(f"Outputs: {{outputs}}")
    
    try:
        # Get algorithm version
        # This will be the latest version in the DB or 1 if new.
        latest_version_db = get_latest_algorithm_version(algorithm_type_id)
        if latest_version_db is not None:
            algorithm_version = latest_version_db

        # Handle version updates
        # If 'updatealgo', use the version specified in conf
        if execution_type == 'updatealgo' and execution_params.get('updated_entity') == algorithm_type_id:
            version = execution_params.get('version') # Version passed from conf for updatealgo
            if version:
                algorithm_version = int(version)
        # If 'updatedag', the new algorithm version was already added by determine_execution_path
        # So, we just need to fetch the latest version again to get the one that was just added.
        elif execution_type == 'updatedag':
            latest_version_for_affected = get_latest_algorithm_version(algorithm_type_id)
            if latest_version_for_affected is not None:
                 algorithm_version = latest_version_for_affected

        print(f"Executing algorithm {{algorithm_type_id}} v{{algorithm_version}}")
        
        # Create output datasets
        for output_dataset in outputs:
            output_version = get_latest_dataset_version(output_dataset, region)
            
            # New version logic for dataset: increment only if it's a fullexec, 
            # or if the creating algorithm was updated/affected (updatealgo, updatedag).
            # Otherwise, use the existing latest version.
            if execution_type in ['fullexec', 'updatealgo', 'updatedag']:
                # The assumption here is that if the algorithm re-runs (due to full exec,
                # algorithm update, or DAG update affecting it), its outputs are new versions.
                # This could be refined based on actual output data hash if available.
                new_output_version = (output_version + 1) if output_version else 1
            else:
                # For other execution types (e.g., resumeexec on a task that wasn't the first failed one),
                # we don't necessarily increment the dataset version unless the logic dictates.
                # For `resumeexec`, typically you'd want to overwrite the previous failed output,
                # so incrementing might still be appropriate if the previous run failed.
                # However, the instruction was to "use the existing latest version" if not explicitly incremented.
                new_output_version = output_version if output_version else 1 

            # Add new dataset version
            add_dataset_version(
                name=output_dataset,
                did=region,
                version=new_output_version,
                dataset_type=dataset_types_map.get(output_dataset, output_dataset),
                algname=algorithm_type_id,
                algversion=algorithm_version, # Pass the correct, potentially new, algorithm_version
                alginputs=inputs,
                params={{"region": region}},
                # This part is a simplification. In a real system, you'd fetch the actual versions
                # of the input datasets that were used for *this specific run*.
                # For now, it assumes version 1 for inputs.
                input_datasets=[[inp, region, 1] for inp in inputs if inp in dataset_types_map] 
            )
            
            print(f"Created output dataset: {{output_dataset}} v{{new_output_version}}")
        
        # Log execution
        log_execution(
            task_id=context['task'].task_id,
            algorithm_name=algorithm_type_id,
            algorithm_version=algorithm_version,
            dataset_name=",".join(outputs),
            dataset_version=0, # This might need to be 'new_output_version' if only one output
            status="success",
            execution_type=execution_type,
            region=region,
            details=f"Algorithm {{algorithm_type_id}} executed successfully",
            dag_run_id=dag_run_id,
            run_config_id=run_config_id 
        )
        
        return f"Algorithm {{algorithm_type_id}} v{{algorithm_version}} executed successfully"
        
    except Exception as e:
        # Log error
        log_execution(
            task_id=context['task'].task_id,
            algorithm_name=algorithm_type_id,
            algorithm_version=algorithm_version,
            dataset_name=",".join(outputs),
            dataset_version=0,
            status="error",
            execution_type=execution_type,
            region=region,
            details=str(e),
            dag_run_id=dag_run_id,
            run_config_id=run_config_id 
        )
        raise

# Single branching task
branch_task = BranchPythonOperator(
    task_id='determine_execution_path',
    python_callable=determine_execution_path,
    provide_context=True,
    dag=dag,
)

'''
    
    # Create dataset tasks
    dataset_tasks = {}
    for dataset_type in dataset_types:
        task_id = f"process_{dataset_type.id.replace('/', '_').replace(' ', '_')}"
        dataset_tasks[dataset_type.id] = task_id
        
        dag_code += f'''
# Dataset task: {dataset_type.name}
{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=process_dataset,
    op_kwargs={{'dataset_type_id': '{dataset_type.id}'}},
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    on_failure_callback=handle_task_failure,
    dag=dag,
)


'''
    
    # Create algorithm tasks
    algorithm_tasks = {}
    for algorithm_type in algorithm_types:
        task_id = f"execute_{algorithm_type.id.replace('/', '_').replace(' ', '_')}"
        algorithm_tasks[algorithm_type.id] = task_id
        
        dag_code += f'''
# Algorithm task: {algorithm_type.name}
{task_id} = PythonOperator(
    task_id='{task_id}',
    python_callable=execute_algorithm,
    op_kwargs={{
        'algorithm_type_id': '{algorithm_type.id}',
        'inputs': {algorithm_type.inputs},
        'outputs': {algorithm_type.outputs}
    }},
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    on_failure_callback=handle_task_failure,
    dag=dag,
)


'''
    
    # Set up dependencies
    dag_code += "\n# Set up dependencies\n"
    
    # Connect branch task to all tasks
    for dataset_type in dataset_types:
        dataset_task = dataset_tasks[dataset_type.id]
        dag_code += f"branch_task >> {dataset_task}\n"
    
    for algorithm_type in algorithm_types:
        algorithm_task = algorithm_tasks[algorithm_type.id]
        dag_code += f"branch_task >> {algorithm_task}\n"
    
    # Set up algorithm-to-dataset dependencies
    dag_code += "\n# Algorithm to dataset dependencies\n"
    for algorithm_type in algorithm_types:
        algo_task = algorithm_tasks[algorithm_type.id]
        
        # Add dependencies from input datasets to algorithm
        for input_dataset in algorithm_type.input_datasets:
            if input_dataset in dataset_tasks:
                input_task = dataset_tasks[input_dataset]
                dag_code += f"{input_task} >> {algo_task}\n"
        
        # Add dependencies from algorithm to output datasets
        for output_dataset in algorithm_type.outputs:
            if output_dataset in dataset_tasks:
                output_task = dataset_tasks[output_dataset]
                dag_code += f"{algo_task} >> {output_task}\n"
    
    # Write the DAG script to the output file
    with open(output_path, 'w') as file:
        file.write(dag_code)
 
    try:
        from stacd_db_handler import add_dag_with_nodes, get_dag_nodes
        
        algorithm_node_ids = [algo_type.id for algo_type in algorithm_types]
        dataset_node_ids = [dataset_type.id for dataset_type in dataset_types]
        
        print(f"Storing DAG {dag_definition.id} v{dag_definition.version}")
        print(f"Algorithm nodes to store: {algorithm_node_ids}")
        print(f"Dataset nodes to store: {dataset_node_ids}")
        
        add_dag_with_nodes(
            dag_id=dag_definition.id,
            name=dag_definition.name,
            version=dag_definition.version,
            description=dag_definition.description,
            params=dag_definition.params,
            algorithm_nodes=algorithm_node_ids, 
            dataset_nodes=dataset_node_ids,     
            all_algorithm_types=algorithm_types, 
            all_dataset_types=dataset_types      
        )



        
        # Verify what was actually stored
        stored_nodes = get_dag_nodes(dag_definition.id, dag_definition.version)
        print(f"Verified stored nodes: {stored_nodes}")
        
        print(f"DAG information stored in database: {dag_definition.id} v{dag_definition.version}")
        
    except Exception as e:
        print(f"Warning: Could not store DAG information in database: {e}")
   
    print(f"Enhanced STACD DAG script with database integration generated at {output_path}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python stacd_dag_generator.py <yaml_path> [output_path]")
        sys.exit(1)
    
    yaml_path = sys.argv[1]
    
    if not os.path.exists(yaml_path):
        print(f"Error: YAML file '{yaml_path}' not found")
        sys.exit(1)
    
    with open(yaml_path, 'r') as file:
        config = yaml.load(file, Loader=yaml.Loader)
    
    dag_definition = None
    for item in config:
        if isinstance(item, DAG):
            dag_definition = item
            break
    
    if not dag_definition:
        raise ValueError("No DAG definition found in YAML file")
    
    dag_filename = f"{dag_definition.id}_stacd_dag.py"
    output_path = os.path.join('..', 'dags', dag_filename)
    
    generate_stacd_dag(yaml_path, output_path)
