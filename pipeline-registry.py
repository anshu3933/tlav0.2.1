import inspect
from typing import Dict, Any, Type, List, Optional, Callable, TypeVar, Generic
from utils.logging_config import get_module_logger

# Create a logger for this module
logger = get_module_logger("pipeline_registry")

T = TypeVar('T')

class ComponentRegistry(Generic[T]):
    """Registry for pipeline components of a specific type."""
    
    def __init__(self, component_type: str):
        self.component_type = component_type
        self.components: Dict[str, Type[T]] = {}
        logger.debug(f"Initialized {component_type} registry")
    
    def register(self, name: str, component_class: Type[T]) -> Type[T]:
        """Register a component class.
        
        Args:
            name: Component name
            component_class: Component class
            
        Returns:
            The registered component class (for decorator usage)
        """
        if name in self.components:
            logger.warning(f"{self.component_type} component '{name}' already registered, overwriting")
            
        self.components[name] = component_class
        logger.debug(f"Registered {self.component_type} component: {name}")
        return component_class
    
    def register_decorator(self, name: str) -> Callable[[Type[T]], Type[T]]:
        """Create a decorator to register a component.
        
        Args:
            name: Component name
            
        Returns:
            Decorator function
        """
        def decorator(component_class: Type[T]) -> Type[T]:
            return self.register(name, component_class)
        return decorator
    
    def get(self, name: str) -> Optional[Type[T]]:
        """Get a registered component class.
        
        Args:
            name: Component name
            
        Returns:
            Component class or None if not found
        """
        component = self.components.get(name)
        if component is None:
            logger.warning(f"{self.component_type} component '{name}' not found")
        return component
    
    def list(self) -> List[str]:
        """List all registered component names.
        
        Returns:
            List of component names
        """
        return list(self.components.keys())
    
    def create(self, name: str, **kwargs) -> Optional[T]:
        """Create an instance of a registered component.
        
        Args:
            name: Component name
            **kwargs: Arguments to pass to the component constructor
            
        Returns:
            Component instance or None if not found
        """
        component_class = self.get(name)
        if component_class is None:
            return None
            
        try:
            # Get constructor signature
            sig = inspect.signature(component_class.__init__)
            params = sig.parameters
            
            # Filter kwargs to only include valid parameters
            valid_kwargs = {k: v for k, v in kwargs.items() if k in params}
            
            # Create instance
            instance = component_class(**valid_kwargs)
            return instance
        except Exception as e:
            logger.error(f"Error creating {self.component_type} component '{name}': {str(e)}")
            return None


class PipelineRegistry:
    """Global registry for all pipeline components."""
    
    _registries: Dict[str, ComponentRegistry] = {}
    
    @classmethod
    def create_registry(cls, component_type: str) -> ComponentRegistry:
        """Create a registry for a component type.
        
        Args:
            component_type: Type of components in the registry
            
        Returns:
            Component registry
        """
        if component_type in cls._registries:
            logger.warning(f"Registry for '{component_type}' already exists, returning existing one")
            return cls._registries[component_type]
            
        registry = ComponentRegistry(component_type)
        cls._registries[component_type] = registry
        return registry
    
    @classmethod
    def get_registry(cls, component_type: str) -> Optional[ComponentRegistry]:
        """Get a registry for a component type.
        
        Args:
            component_type: Type of components in the registry
            
        Returns:
            Component registry or None if not found
        """
        return cls._registries.get(component_type)
    
    @classmethod
    def register(cls, component_type: str, name: str, component_class: Type) -> Type:
        """Register a component in the specified registry.
        
        Args:
            component_type: Type of component registry
            name: Component name
            component_class: Component class
            
        Returns:
            The registered component class
        """
        registry = cls.get_registry(component_type)
        if registry is None:
            registry = cls.create_registry(component_type)
        
        return registry.register(name, component_class)
    
    @classmethod
    def get(cls, component_type: str, name: str) -> Optional[Type]:
        """Get a component class from a registry.
        
        Args:
            component_type: Type of component registry
            name: Component name
            
        Returns:
            Component class or None if not found
        """
        registry = cls.get_registry(component_type)
        if registry is None:
            logger.warning(f"No registry found for component type '{component_type}'")
            return None
        
        return registry.get(name)
    
    @classmethod
    def create(cls, component_type: str, name: str, **kwargs) -> Any:
        """Create an instance of a registered component.
        
        Args:
            component_type: Type of component registry
            name: Component name
            **kwargs: Arguments to pass to the component constructor
            
        Returns:
            Component instance or None if not found
        """
        registry = cls.get_registry(component_type)
        if registry is None:
            logger.warning(f"No registry found for component type '{component_type}'")
            return None
        
        return registry.create(name, **kwargs)
    
    @classmethod
    def list_registries(cls) -> List[str]:
        """List all registered component types.
        
        Returns:
            List of component types
        """
        return list(cls._registries.keys())
    
    @classmethod
    def list_components(cls, component_type: str) -> List[str]:
        """List all components in a registry.
        
        Args:
            component_type: Type of component registry
            
        Returns:
            List of component names
        """
        registry = cls.get_registry(component_type)
        if registry is None:
            return []
        
        return registry.list()


# Create decorator functions for common component types
def document_processor(name: str):
    """Decorator to register a document processor."""
    registry = PipelineRegistry.get_registry("document_processor")
    if registry is None:
        registry = PipelineRegistry.create_registry("document_processor")
    return registry.register_decorator(name)

def embedding_generator(name: str):
    """Decorator to register an embedding generator."""
    registry = PipelineRegistry.get_registry("embedding_generator")
    if registry is None:
        registry = PipelineRegistry.create_registry("embedding_generator")
    return registry.register_decorator(name)

def vector_store(name: str):
    """Decorator to register a vector store."""
    registry = PipelineRegistry.get_registry("vector_store")
    if registry is None:
        registry = PipelineRegistry.create_registry("vector_store")
    return registry.register_decorator(name)

def llm_provider(name: str):
    """Decorator to register an LLM provider."""
    registry = PipelineRegistry.get_registry("llm_provider")
    if registry is None:
        registry = PipelineRegistry.create_registry("llm_provider")
    return registry.register_decorator(name)

def retriever(name: str):
    """Decorator to register a retriever."""
    registry = PipelineRegistry.get_registry("retriever")
    if registry is None:
        registry = PipelineRegistry.create_registry("retriever")
    return registry.register_decorator(name)

def rag_chain(name: str):
    """Decorator to register a RAG chain."""
    registry = PipelineRegistry.get_registry("rag_chain")
    if registry is None:
        registry = PipelineRegistry.create_registry("rag_chain")
    return registry.register_decorator(name)
