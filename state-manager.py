# ui/state_manager.py

import streamlit as st
from typing import Dict, Any, List, Optional, TypeVar, Generic, Callable
import threading
import uuid
import json
import time
from datetime import datetime
from config.logging_config import get_module_logger

# Create a logger for this module
logger = get_module_logger("state_manager")

T = TypeVar('T')

class StateValidationError(Exception):
    """Exception raised for state validation errors."""
    pass

class SessionState(Generic[T]):
    """Thread-safe session state management."""
    
    def __init__(self, init_value: T, validator: Optional[Callable[[T], bool]] = None):
        """Initialize with initial value and optional validator.
        
        Args:
            init_value: Initial value
            validator: Optional validation function
        """
        self.value = init_value
        self.validator = validator
        self.lock = threading.RLock()
    
    def get(self) -> T:
        """Get the current value (thread-safe).
        
        Returns:
            Current value
        """
        with self.lock:
            return self.value
    
    def set(self, new_value: T) -> None:
        """Set a new value with validation (thread-safe).
        
        Args:
            new_value: New value
            
        Raises:
            StateValidationError: If validation fails
        """
        with self.lock:
            if self.validator and not self.validator(new_value):
                raise StateValidationError(f"Invalid value: {new_value}")
            self.value = new_value


class AppStateManager:
    """Manages application state with validation and persistence."""
    
    def __init__(self):
        """Initialize state manager."""
        self._initialize_session_state()
        logger.debug("Initialized app state manager")
    
    def _initialize_session_state(self):
        """Initialize Streamlit session state with default values."""
        defaults = {
            "user_id": self._generate_user_id(),
            "session_id": str(uuid.uuid4()),
            "session_start": datetime.now().isoformat(),
            "documents_processed": False,
            "documents": [],
            "iep_results": [],
            "lesson_plans": [],
            "messages": [],
            "current_plan": None,
            "errors": [],
            "warnings": [],
            "system_state": {
                "chain_initialized": False,
                "vector_store_initialized": False,
                "llm_initialized": False
            }
        }
        
        # Initialize each key if not exists
        for key, default_value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = default_value
    
    def _generate_user_id(self) -> str:
        """Generate a stable user ID based on session properties.
        
        Returns:
            User ID
        """
        # For demo purposes, just generate a random ID
        # In production, this could be linked to authentication
        return str(uuid.uuid4())
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from session state.
        
        Args:
            key: State key
            default: Default value if key doesn't exist
            
        Returns:
            Value from session state
        """
        return st.session_state.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in session state.
        
        Args:
            key: State key
            value: Value to set
        """
        st.session_state[key] = value
    
    def update(self, key: str, update_func: Callable[[Any], Any]) -> None:
        """Update a value in session state using a function.
        
        Args:
            key: State key
            update_func: Function that takes old value and returns new value
        """
        if key in st.session_state:
            st.session_state[key] = update_func(st.session_state[key])
    
    def append(self, key: str, value: Any) -> None:
        """Append a value to a list in session state.
        
        Args:
            key: State key (must be a list)
            value: Value to append
            
        Raises:
            TypeError: If the key doesn't exist or isn't a list
        """
        if key not in st.session_state:
            st.session_state[key] = []
            
        if not isinstance(st.session_state[key], list):
            raise TypeError(f"Key '{key}' is not a list")
            
        st.session_state[key].append(value)
    
    def clear(self, key: Optional[str] = None) -> None:
        """Clear a specific key or all session state.
        
        Args:
            key: Optional key to clear (None clears all)
        """
        if key is None:
            # Preserve user ID and session info when clearing
            user_id = st.session_state.get("user_id")
            session_id = st.session_state.get("session_id")
            session_start = st.session_state.get("session_start")
            
            # Clear all state
            for k in list(st.session_state.keys()):
                del st.session_state[k]
            
            # Restore user and session info
            st.session_state["user_id"] = user_id
            st.session_state["session_id"] = session_id
            st.session_state["session_start"] = session_start
            
            # Reinitialize with defaults
            self._initialize_session_state()
        elif key in st.session_state:
            del st.session_state[key]
    
    def add_error(self, error: str) -> None:
        """Add an error message.
        
        Args:
            error: Error message
        """
        timestamp = datetime.now().isoformat()
        self.append("errors", {"message": error, "timestamp": timestamp})
        logger.error(f"UI Error: {error}")
    
    def add_warning(self, warning: str) -> None:
        """Add a warning message.
        
        Args:
            warning: Warning message
        """
        timestamp = datetime.now().isoformat()
        self.append("warnings", {"message": warning, "timestamp": timestamp})
        logger.warning(f"UI Warning: {warning}")
    
    def has_errors(self) -> bool:
        """Check if there are any errors.
        
        Returns:
            True if there are errors, False otherwise
        """
        return len(self.get("errors", [])) > 0
    
    def get_latest_error(self) -> Optional[Dict[str, str]]:
        """Get the latest error message.
        
        Returns:
            Latest error or None if no errors
        """
        errors = self.get("errors", [])
        return errors[-1] if errors else None
    
    def clear_errors(self) -> None:
        """Clear all error messages."""
        self.set("errors", [])
    
    def has_warnings(self) -> bool:
        """Check if there are any warnings.
        
        Returns:
            True if there are warnings, False otherwise
        """
        return len(self.get("warnings", [])) > 0
    
    def get_system_state(self) -> Dict[str, bool]:
        """Get the system state.
        
        Returns:
            System state dictionary
        """
        return self.get("system_state", {
            "chain_initialized": False,
            "vector_store_initialized": False,
            "llm_initialized": False
        })
    
    def update_system_state(self, **kwargs) -> None:
        """Update the system state.
        
        Args:
            **kwargs: System state key-value pairs
        """
        system_state = self.get("system_state", {})
        system_state.update(kwargs)
        self.set("system_state", system_state)
    
    def export_state(self, include_system_state: bool = False) -> Dict[str, Any]:
        """Export session state for persistence.
        
        Args:
            include_system_state: Whether to include system state
            
        Returns:
            Dictionary with session state
        """
        # Get all keys except for large objects and system state
        excluded_keys = {"_lock", "vector_store", "chain", "llm_client"}
        if not include_system_state:
            excluded_keys.add("system_state")
        
        # Copy state
        state_copy = {}
        for key, value in st.session_state.items():
            if key not in excluded_keys:
                state_copy[key] = value
        
        return state_copy
    
    def import_state(self, state_dict: Dict[str, Any]) -> None:
        """Import session state from a dictionary.
        
        Args:
            state_dict: Dictionary with session state
        """
        for key, value in state_dict.items():
            st.session_state[key] = value


# Create a global state manager instance
state_manager = AppStateManager()
