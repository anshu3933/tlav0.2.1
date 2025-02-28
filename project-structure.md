# Refactored Project Structure

```
educational_assistant/
├── config/
│   ├── __init__.py
│   ├── app_config.py        # Configuration settings
│   └── logging_config.py    # Logging configuration
├── core/
│   ├── __init__.py
│   ├── document_processing/ # Document processing module
│   │   ├── __init__.py
│   │   ├── document_loader.py
│   │   ├── document_validator.py
│   │   └── file_handler.py
│   ├── embeddings/          # Embedding and vector store module
│   │   ├── __init__.py
│   │   ├── embedding_manager.py
│   │   └── vector_store.py
│   ├── llm/                 # LLM integration module
│   │   ├── __init__.py
│   │   ├── llm_client.py
│   │   ├── llm_cache.py
│   │   └── rate_limiter.py
│   ├── rag/                 # RAG implementation
│   │   ├── __init__.py
│   │   ├── retriever.py
│   │   ├── augmentation.py
│   │   └── chain_builder.py
│   └── pipelines/           # Processing pipelines
│       ├── __init__.py
│       ├── iep_pipeline.py
│       └── lesson_plan_pipeline.py
├── api/                     # API layer
│   ├── __init__.py
│   ├── document_api.py
│   ├── iep_api.py
│   ├── lesson_plan_api.py
│   └── chat_api.py
├── ui/                      # UI layer
│   ├── __init__.py
│   ├── app.py               # Main Streamlit app
│   ├── state_manager.py     # State management
│   ├── pages/
│   │   ├── __init__.py
│   │   ├── chat_page.py
│   │   ├── iep_page.py
│   │   └── lesson_plan_page.py
│   └── components/
│       ├── __init__.py
│       ├── document_upload.py
│       ├── chat_interface.py
│       └── plan_viewer.py
├── utils/
│   ├── __init__.py
│   ├── error_handling.py
│   └── validation.py
├── tests/                   # Test suite
│   ├── __init__.py
│   ├── test_document_processing.py
│   ├── test_llm_integration.py
│   └── test_pipelines.py
├── .env.example             # Example environment variables
├── requirements.txt         # Dependencies
├── main.py                  # Application entry point
└── README.md                # Documentation
```

This structure addresses several issues:
1. Separates concerns with clear module boundaries
2. Eliminates circular dependencies
3. Isolates core logic from UI components
4. Provides proper abstraction layers
5. Supports dependency injection patterns
