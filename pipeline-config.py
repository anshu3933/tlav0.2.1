import os
import yaml
import json
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field
from utils.logging_config import get_module_logger

# Create a logger for this module
logger = get_module_logger("pipeline_config")

@dataclass
class ComponentConfig:
    """Configuration for a pipeline component."""
    type: str
    params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineStageConfig:
    """Configuration for a pipeline stage."""
    components: Dict[str, ComponentConfig] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PipelineStageConfig":
        """Create a PipelineStageConfig from a dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            PipelineStageConfig instance
        """
        components = {}
        for name, comp_config in config_dict.items():
            if isinstance(comp_config, dict):
                comp_type = comp_config.get("type", "default")
                comp_params = comp_config.get("params", {})
                components[name] = ComponentConfig(comp_type, comp_params)
        
        return cls(components)


@dataclass
class PipelineConfig:
    """Configuration for a pipeline."""
    name: str
    version: str
    description: str = ""
    stages: Dict[str, PipelineStageConfig] = field(default_factory=dict)
    environment: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PipelineConfig":
        """Create a PipelineConfig from a dictionary.
        
        Args:
            config_dict: Configuration dictionary
            
        Returns:
            PipelineConfig instance
        """
        name = config_dict.get("name", "unnamed_pipeline")
        version = config_dict.get("version", "0.1.0")
        description = config_dict.get("description", "")
        environment = config_dict.get("environment", {})
        
        stages = {}
        for stage_name, stage_config in config_dict.get("stages", {}).items():
            stages[stage_name] = PipelineStageConfig.from_dict(stage_config)
        
        return cls(
            name=name,
            version=version,
            description=description,
            stages=stages,
            environment=environment
        )
    
    @classmethod
    def from_yaml(cls, yaml_path: str) -> "PipelineConfig":
        """Load configuration from a YAML file.
        
        Args:
            yaml_path: Path to YAML file
            
        Returns:
            PipelineConfig instance
        """
        try:
            with open(yaml_path, "r") as f:
                config_dict = yaml.safe_load(f)
            
            return cls.from_dict(config_dict)
        except Exception as e:
            logger.error(f"Error loading configuration from {yaml_path}: {str(e)}")
            # Return a default configuration
            return cls(name="default", version="0.1.0")
    
    @classmethod
    def from_json(cls, json_path: str) -> "PipelineConfig":
        """Load configuration from a JSON file.
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            PipelineConfig instance
        """
        try:
            with open(json_path, "r") as f:
                config_dict = json.load(f)
            
            return cls.from_dict(config_dict)
        except Exception as e:
            logger.error(f"Error loading configuration from {json_path}: {str(e)}")
            # Return a default configuration
            return cls(name="default", version="0.1.0")
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary.
        
        Returns:
            Configuration dictionary
        """
        stages_dict = {}
        for stage_name, stage_config in self.stages.items():
            components_dict = {}
            for comp_name, comp_config in stage_config.components.items():
                components_dict[comp_name] = {
                    "type": comp_config.type,
                    "params": comp_config.params
                }
            stages_dict[stage_name] = components_dict
        
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "stages": stages_dict,
            "environment": self.environment
        }
    
    def to_yaml(self, yaml_path: str) -> bool:
        """Save configuration to a YAML file.
        
        Args:
            yaml_path: Path to YAML file
            
        Returns:
            Success status
        """
        try:
            config_dict = self.to_dict()
            
            with open(yaml_path, "w") as f:
                yaml.dump(config_dict, f, default_flow_style=False)
            
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to {yaml_path}: {str(e)}")
            return False
    
    def to_json(self, json_path: str) -> bool:
        """Save configuration to a JSON file.
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            Success status
        """
        try:
            config_dict = self.to_dict()
            
            with open(json_path, "w") as f:
                json.dump(config_dict, f, indent=2)
            
            return True
        except Exception as e:
            logger.error(f"Error saving configuration to {json_path}: {str(e)}")
            return False
    
    def get_component_config(self, stage: str, component: str) -> Optional[ComponentConfig]:
        """Get configuration for a specific component.
        
        Args:
            stage: Pipeline stage
            component: Component name
            
        Returns:
            ComponentConfig or None if not found
        """
        stage_config = self.stages.get(stage)
        if not stage_config:
            return None
        
        return stage_config.components.get(component)
    
    def get_component_param(self, stage: str, component: str, param: str, default: Any = None) -> Any:
        """Get a parameter value for a specific component.
        
        Args:
            stage: Pipeline stage
            component: Component name
            param: Parameter name
            default: Default value if not found
            
        Returns:
            Parameter value or default
        """
        comp_config = self.get_component_config(stage, component)
        if not comp_config:
            return default
        
        return comp_config.params.get(param, default)
    
    def set_component_param(self, stage: str, component: str, param: str, value: Any) -> bool:
        """Set a parameter value for a specific component.
        
        Args:
            stage: Pipeline stage
            component: Component name
            param: Parameter name
            value: Parameter value
            
        Returns:
            Success status
        """
        if stage not in self.stages:
            self.stages[stage] = PipelineStageConfig()
        
        stage_config = self.stages[stage]
        
        if component not in stage_config.components:
            stage_config.components[component] = ComponentConfig(type="default")
        
        stage_config.components[component].params[param] = value
        return True


class ConfigManager:
    """Manages pipeline configurations."""
    
    def __init__(self, config_dir: str = "config/pipelines"):
        self.config_dir = config_dir
        self.configs: Dict[str, PipelineConfig] = {}
        
        # Create config directory if it doesn't exist
        os.makedirs(config_dir, exist_ok=True)
        
        logger.debug(f"Initialized config manager with directory: {config_dir}")
    
    def load_config(self, name: str) -> Optional[PipelineConfig]:
        """Load a pipeline configuration.
        
        Args:
            name: Pipeline name
            
        Returns:
            PipelineConfig or None if not found
        """
        # Check if already loaded
        if name in self.configs:
            return self.configs[name]
        
        # Try YAML first
        yaml_path = os.path.join(self.config_dir, f"{name}.yaml")
        if os.path.exists(yaml_path):
            config = PipelineConfig.from_yaml(yaml_path)
            self.configs[name] = config
            return config
        
        # Try JSON next
        json_path = os.path.join(self.config_dir, f"{name}.json")
        if os.path.exists(json_path):
            config = PipelineConfig.from_json(json_path)
            self.configs[name] = config
            return config
        
        logger.warning(f"Configuration '{name}' not found")
        return None
    
    def save_config(self, config: PipelineConfig, format: str = "yaml") -> bool:
        """Save a pipeline configuration.
        
        Args:
            config: Pipeline configuration
            format: File format ("yaml" or "json")
            
        Returns:
            Success status
        """
        # Save to cache
        self.configs[config.name] = config
        
        # Save to file
        if format.lower() == "json":
            path = os.path.join(self.config_dir, f"{config.name}.json")
            return config.to_json(path)
        else:
            path = os.path.join(self.config_dir, f"{config.name}.yaml")
            return config.to_yaml(path)
    
    def list_configs(self) -> List[str]:
        """List all available configurations.
        
        Returns:
            List of configuration names
        """
        configs = []
        
        # Check for YAML files
        for filename in os.listdir(self.config_dir):
            if filename.endswith(".yaml") or filename.endswith(".yml"):
                name = os.path.splitext(filename)[0]
                configs.append(name)
            elif filename.endswith(".json"):
                name = os.path.splitext(filename)[0]
                if name not in configs:  # Avoid duplicates if both YAML and JSON exist
                    configs.append(name)
        
        return configs
    
    def create_config(self, name: str, version: str = "0.1.0", description: str = "") -> PipelineConfig:
        """Create a new pipeline configuration.
        
        Args:
            name: Pipeline name
            version: Pipeline version
            description: Pipeline description
            
        Returns:
            New PipelineConfig
        """
        config = PipelineConfig(
            name=name,
            version=version,
            description=description
        )
        
        # Save to cache
        self.configs[name] = config
        
        return config
