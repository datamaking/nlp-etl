from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame

from src.config.pipeline_config import PipelineConfig
from src.utils.timer import Timer


@dataclass
class PipelineContext:
    """Context object that is passed through the pipeline stages."""
    
    config: PipelineConfig
    spark: SparkSession
    
    # Data that is passed between stages
    data: Optional[DataFrame] = None
    
    # Metadata and state information
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Execution metrics
    timers: Dict[str, Timer] = field(default_factory=dict)
    execution_times: Dict[str, float] = field(default_factory=dict)
    
    def start_timer(self, stage_name: str) -> None:
        """Start a timer for a pipeline stage."""
        if self.config.track_execution_time:
            self.timers[stage_name] = Timer()
            self.timers[stage_name].start()
    
    def stop_timer(self, stage_name: str) -> None:
        """Stop a timer for a pipeline stage and record the execution time."""
        if self.config.track_execution_time and stage_name in self.timers:
            self.timers[stage_name].stop()
            self.execution_times[stage_name] = self.timers[stage_name].elapsed_time
    
    def get_execution_report(self) -> Dict[str, Any]:
        """Get a report of execution times for all stages."""
        report = {
            "pipeline_name": self.config.pipeline_name,
            "department": self.config.department,
            "total_time": sum(self.execution_times.values()),
            "stages": self.execution_times
        }
        return report
    
    def set_data(self, data: DataFrame) -> None:
        """Set the current data in the context."""
        self.data = data
    
    def get_data(self) -> DataFrame:
        """Get the current data from the context."""
        if self.data is None:
            raise ValueError("No data available in the pipeline context")
        return self.data
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to the context."""
        self.metadata[key] = value
    
    def get_metadata(self, key: str) -> Any:
        """Get metadata from the context."""
        if key not in self.metadata:
            raise KeyError(f"Metadata key '{key}' not found in pipeline context")
        return self.metadata[key]