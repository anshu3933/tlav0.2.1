from core.pipeline.config import ConfigManager, PipelineConfig
import os

def demonstrate_config_usage():
    # Initialize config manager
    config_dir = "config/pipelines"
    os.makedirs(config_dir, exist_ok=True)
    config_manager = ConfigManager(config_dir=config_dir)
    
    # List available configurations
    print("Available configurations:")
    configs = config_manager.list_configs()
    for config_name in configs:
        print(f"  - {config_name}")
    
    # Load the RAG pipeline configuration
    rag_config = config_manager.load_config("rag_pipeline")
    if rag_config:
        print(f"\nLoaded configuration: {rag_config.name} v{rag_config.version}")
        print(f"Description: {rag_config.description}")
        
        # Get and print components for each stage
        for stage_name, stage_config in rag_config.stages.items():
            print(f"\nStage: {stage_name}")
            for comp_name, comp_config in stage_config.components.items():
                print(f"  Component: {comp_name} (type: {comp_config.type})")
                for param_name, param_value in comp_config.params.items():
                    print(f"    - {param_name}: {param_value}")
        
        # Get specific parameters
        chunking_size = rag_config.get_component_param(
            "embedding", "chunking", "chunk_size", default=1000
        )
        print(f"\nChunking size: {chunking_size}")
        
        # Modify a parameter
        rag_config.set_component_param(
            "embedding", "chunking", "chunk_size", 1500
        )
        print(f"Updated chunking size: {rag_config.get_component_param('embedding', 'chunking', 'chunk_size')}")
        
        # Save the modified configuration
        config_manager.save_config(rag_config, format="yaml")
        print("Saved modified configuration")
    else:
        print("RAG pipeline configuration not found")
    
    # Create a new configuration
    new_config = config_manager.create_config(
        name="iep_generator",
        version="0.1.0",
        description="IEP generator pipeline"
    )
    
    # Add components to the new configuration
    new_config.set_component_param("document_processing", "pdf_processor", "type", "pdf")
    new_config.set_component_param("document_processing", "pdf_processor", "params", {
        "extraction_timeout": 60,
        "extract_images": False
    })
    
    new_config.set_component_param("llm", "openai", "type", "openai")
    new_config.set_component_param("llm", "openai", "params", {
        "model": "gpt-4o",
        "temperature": 0.5,
        "max_tokens": 4000
    })
    
    # Save the new configuration
    config_manager.save_config(new_config, format="json")
    print(f"\nCreated and saved new configuration: {new_config.name}")

if __name__ == "__main__":
    demonstrate_config_usage()