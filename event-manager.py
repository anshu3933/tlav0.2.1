import time
import json
import uuid
import os
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from utils.logging_config import get_module_logger

# Create a logger for this module
logger = get_module_logger("event_manager")

class EventSubscriber:
    """Base class for event subscribers."""
    
    def __init__(self, event_types: List[str] = None):
        self.event_types = event_types or ["*"]
    
    def handle_event(self, event: Dict[str, Any]) -> None:
        """Handle an event.
        
        Args:
            event: Event data
        """
        raise NotImplementedError("Subclasses must implement handle_event")
    
    def should_handle(self, event_type: str) -> bool:
        """Check if this subscriber should handle an event type.
        
        Args:
            event_type: Event type
            
        Returns:
            True if should handle, False otherwise
        """
        return "*" in self.event_types or event_type in self.event_types


class LoggingSubscriber(EventSubscriber):
    """Subscriber that logs events."""
    
    def handle_event(self, event: Dict[str, Any]) -> None:
        """Log an event.
        
        Args:
            event: Event data
        """
        event_type = event.get("event_type", "unknown")
        pipeline_id = event.get("pipeline_id", "unknown")
        step = event.get("step", "unknown")
        
        if event_type == "step_start":
            logger.info(f"Pipeline {pipeline_id}: Step '{step}' started")
        elif event_type == "step_end":
            duration = event.get("duration", 0)
            logger.info(f"Pipeline {pipeline_id}: Step '{step}' completed in {duration:.2f}s")
        elif event_type == "error":
            error = event.get("error", "Unknown error")
            logger.error(f"Pipeline {pipeline_id}: Error in step '{step}': {error}")
        elif event_type == "metric":
            metric_name = event.get("metric_name", "unknown")
            metric_value = event.get("metric_value", 0)
            logger.info(f"Pipeline {pipeline_id}: Metric '{metric_name}' = {metric_value} in step '{step}'")
        else:
            logger.debug(f"Pipeline {pipeline_id}: Event '{event_type}' in step '{step}'")


class FileStorageSubscriber(EventSubscriber):
    """Subscriber that stores events to file."""
    
    def __init__(self, 
                output_dir: str = "logs/pipeline_events", 
                event_types: List[str] = None):
        super().__init__(event_types)
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.file_locks = {}
    
    def handle_event(self, event: Dict[str, Any]) -> None:
        """Store an event to file.
        
        Args:
            event: Event data
        """
        pipeline_id = event.get("pipeline_id", "unknown")
        file_path = os.path.join(self.output_dir, f"{pipeline_id}.jsonl")
        
        # Get or create lock for this file
        if file_path not in self.file_locks:
            self.file_locks[file_path] = threading.Lock()
        
        # Write event to file
        with self.file_locks[file_path]:
            with open(file_path, "a") as f:
                f.write(json.dumps(event) + "\n")


class MetricsCollector(EventSubscriber):
    """Subscriber that collects metrics."""
    
    def __init__(self, event_types: List[str] = None):
        super().__init__(event_types or ["metric", "step_end"])
        self.metrics = {}
        self.lock = threading.Lock()
    
    def handle_event(self, event: Dict[str, Any]) -> None:
        """Collect metrics from an event.
        
        Args:
            event: Event data
        """
        event_type = event.get("event_type", "unknown")
        pipeline_id = event.get("pipeline_id", "unknown")
        step = event.get("step", "unknown")
        
        with self.lock:
            # Ensure pipeline exists in metrics
            if pipeline_id not in self.metrics:
                self.metrics[pipeline_id] = {"steps": {}, "metrics": {}}
            
            if event_type == "metric":
                metric_name = event.get("metric_name", "unknown")
                metric_value = event.get("metric_value", 0)
                
                # Add to metrics
                if metric_name not in self.metrics[pipeline_id]["metrics"]:
                    self.metrics[pipeline_id]["metrics"][metric_name] = []
                
                self.metrics[pipeline_id]["metrics"][metric_name].append({
                    "value": metric_value,
                    "step": step,
                    "timestamp": event.get("timestamp", time.time())
                })
            
            elif event_type == "step_end":
                # Record step duration
                if step not in self.metrics[pipeline_id]["steps"]:
                    self.metrics[pipeline_id]["steps"][step] = []
                
                self.metrics[pipeline_id]["steps"][step].append({
                    "duration": event.get("duration", 0),
                    "timestamp": event.get("timestamp", time.time())
                })
    
    def get_step_durations(self, pipeline_id: str) -> Dict[str, List[float]]:
        """Get step durations for a pipeline.
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Dictionary of step names to lists of durations
        """
        with self.lock:
            if pipeline_id not in self.metrics:
                return {}
            
            return {
                step: [run["duration"] for run in runs]
                for step, runs in self.metrics[pipeline_id]["steps"].items()
            }
    
    def get_metric_values(self, pipeline_id: str, metric_name: str) -> List[float]:
        """Get values for a specific metric.
        
        Args:
            pipeline_id: Pipeline ID
            metric_name: Metric name
            
        Returns:
            List of metric values
        """
        with self.lock:
            if pipeline_id not in self.metrics:
                return []
            
            metrics = self.metrics[pipeline_id]["metrics"]
            if metric_name not in metrics:
                return []
            
            return [metric["value"] for metric in metrics[metric_name]]
    
    def get_average_duration(self, pipeline_id: str, step: str) -> Optional[float]:
        """Get average duration for a step.
        
        Args:
            pipeline_id: Pipeline ID
            step: Step name
            
        Returns:
            Average duration or None if no data
        """
        durations = self.get_step_durations(pipeline_id).get(step, [])
        if not durations:
            return None
        
        return sum(durations) / len(durations)
    
    def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics.
        
        Returns:
            Dictionary of all metrics
        """
        with self.lock:
            return json.loads(json.dumps(self.metrics))  # Deep copy


class PipelineEventManager:
    """Manages pipeline events for observability."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(PipelineEventManager, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.subscribers: List[EventSubscriber] = []
        self.metrics_collector = MetricsCollector()
        self.lock = threading.Lock()
        
        # Register default subscribers
        self.register_subscriber(LoggingSubscriber())
        self.register_subscriber(FileStorageSubscriber())
        self.register_subscriber(self.metrics_collector)
        
        self._initialized = True
        logger.debug("Initialized pipeline event manager")
    
    def register_subscriber(self, subscriber: EventSubscriber) -> None:
        """Register an event subscriber.
        
        Args:
            subscriber: Event subscriber
        """
        with self.lock:
            self.subscribers.append(subscriber)
    
    def publish_event(self, event: Dict[str, Any]) -> None:
        """Publish an event to all subscribers.
        
        Args:
            event: Event data
        """
        # Add timestamp if not present
        if "timestamp" not in event:
            event["timestamp"] = time.time()
        
        event_type = event.get("event_type", "unknown")
        
        # Dispatch to subscribers
        for subscriber in self.subscribers:
            if subscriber.should_handle(event_type):
                try:
                    subscriber.handle_event(event)
                except Exception as e:
                    logger.error(f"Error in subscriber {subscriber.__class__.__name__}: {str(e)}")
    
    def get_metrics_collector(self) -> MetricsCollector:
        """Get the metrics collector.
        
        Returns:
            Metrics collector
        """
        return self.metrics_collector


class PipelineContext:
    """Context for a pipeline run."""
    
    def __init__(self, pipeline_id: str = None, parent_id: str = None):
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self.parent_id = parent_id
        self.event_manager = PipelineEventManager()
        self.start_time = time.time()
        self.step_times = {}
    
    def record_step_start(self, step: str) -> None:
        """Record the start of a pipeline step.
        
        Args:
            step: Step name
        """
        self.step_times[step] = time.time()
        
        self.event_manager.publish_event({
            "event_type": "step_start",
            "pipeline_id": self.pipeline_id,
            "parent_id": self.parent_id,
            "step": step
        })
    
    def record_step_end(self, step: str, outputs: Any = None) -> None:
        """Record the end of a pipeline step.
        
        Args:
            step: Step name
            outputs: Step outputs
        """
        start_time = self.step_times.get(step, self.start_time)
        duration = time.time() - start_time
        
        output_info = None
        if outputs is not None:
            # Try to get some general info about the output
            if isinstance(outputs, (list, tuple)):
                output_info = {
                    "type": type(outputs).__name__,
                    "length": len(outputs)
                }
            elif hasattr(outputs, "__dict__"):
                output_info = {
                    "type": type(outputs).__name__,
                    "attributes": list(outputs.__dict__.keys())
                }
            else:
                output_info = {
                    "type": type(outputs).__name__
                }
        
        self.event_manager.publish_event({
            "event_type": "step_end",
            "pipeline_id": self.pipeline_id,
            "parent_id": self.parent_id,
            "step": step,
            "duration": duration,
            "output_info": output_info
        })
    
    def record_error(self, step: str, error: Exception) -> None:
        """Record an error in a pipeline step.
        
        Args:
            step: Step name
            error: Exception that occurred
        """
        self.event_manager.publish_event({
            "event_type": "error",
            "pipeline_id": self.pipeline_id,
            "parent_id": self.parent_id,
            "step": step,
            "error": str(error),
            "error_type": type(error).__name__
        })
    
    def record_metric(self, step: str, metric_name: str, value: float) -> None:
        """Record a metric for a pipeline step.
        
        Args:
            step: Step name
            metric_name: Metric name
            value: Metric value
        """
        self.event_manager.publish_event({
            "event_type": "metric",
            "pipeline_id": self.pipeline_id,
            "parent_id": self.parent_id,
            "step": step,
            "metric_name": metric_name,
            "metric_value": value
        })
    
    def get_step_timing(self, step: str) -> Optional[float]:
        """Get the duration of a step.
        
        Args:
            step: Step name
            
        Returns:
            Step duration or None if step not completed
        """
        if step in self.step_times:
            return time.time() - self.step_times[step]
        return None


class step_timing:
    """Decorator for timing pipeline steps."""
    
    def __init__(self, step_name: str, context: Optional[PipelineContext] = None):
        self.step_name = step_name
        self.context = context
    
    def __call__(self, func: Callable) -> Callable:
        """Create a decorator for the function.
        
        Args:
            func: Function to decorate
            
        Returns:
            Decorated function
        """
        def wrapper(*args, **kwargs):
            # Get context from args if not provided
            context = self.context
            if context is None and args and isinstance(args[0], PipelineContext):
                context = args[0]
            elif "context" in kwargs and isinstance(kwargs["context"], PipelineContext):
                context = kwargs["context"]
            
            # Create context if none exists
            if context is None:
                context = PipelineContext()
            
            # Record step start
            context.record_step_start(self.step_name)
            
            try:
                # Call the function
                result = func(*args, **kwargs)
                
                # Record step end
                context.record_step_end(self.step_name, result)
                
                return result
            except Exception as e:
                # Record error
                context.record_error(self.step_name, e)
                raise
        
        return wrapper
