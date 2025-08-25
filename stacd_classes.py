import yaml
from typing import List, Dict, Any

class DAG(yaml.YAMLObject):
    yaml_tag = u'!DAG'
    
    def __init__(self, id: str, name: str, version: str, description: str, 
                 params: List[str], alg_type_nodes: List[str], 
                 dataset_type_nodes: List[str]):
        self.id = id
        self.name = name
        self.version = version
        self.description = description
        self.params = params
        self.alg_type_nodes = alg_type_nodes
        self.dataset_type_nodes = dataset_type_nodes

class Dataset_Type(yaml.YAMLObject):
    yaml_tag = u'!Dataset_Type'
    
    def __init__(self, id: str, name: str):
        self.id = id
        self.name = name

class Algorithm_Type(yaml.YAMLObject):
    yaml_tag = u'!Algorithm_Type'
    
    def __init__(self, id: str, name: str, inputs: List[str], 
                 params: List[Dict], input_datasets: List[str], 
                 outputs: List[str]):
        self.id = id
        self.name = name
        self.inputs = inputs
        self.params = params
        self.input_datasets = input_datasets
        self.outputs = outputs

# Database-compatible classes (keep existing structure)
class Algorithm_Instance(yaml.YAMLObject):
    yaml_tag = u'!Algorithm_Instance'
    
    def __init__(self, version: str, type: str, assets: Dict[str, str], date=None):
        self.version = version
        self.type = type
        self.assets = assets
        self.date = date

class Dataset_Instance(yaml.YAMLObject):
    yaml_tag = u'!Dataset_Instance'
    
    def __init__(self, version: int, type: str, params: List[Dict],
                 alg_name: str, alg_inputs: Dict, input_datasets: List[str], date=None):
        self.version = version
        self.type = type
        self.params = params
        self.alg_name = alg_name
        self.alg_inputs = alg_inputs
        self.input_datasets = input_datasets
        self.date = date

# Legacy classes for backward compatibility
class AlgorithmVersion(yaml.YAMLObject):
    yaml_tag = u'!AlgorithmVersion'
    
    def __init__(self, name, version, date):
        self.name = name
        self.version = version
        self.date = date

class DatasetVersion(yaml.YAMLObject):
    yaml_tag = u'!DatasetVersion'
    
    def __init__(self, name, did, version, algname, algversion, alginputs, date):
        self.name = name
        self.did = did
        self.version = version
        self.algname = algname
        self.algversion = algversion
        self.alginputs = alginputs
        self.date = date
