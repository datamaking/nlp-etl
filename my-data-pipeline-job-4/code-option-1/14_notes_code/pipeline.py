from typing import List, Optional, Dict, Any, Callable
from abc import ABC, abstractmethod

from pyspark.sql import SparkSession, DataFrame

from src.config.pipeline_config import PipelineConfig
from src.core.context import PipelineContext
from src.utils.logger import get_logger
from src.utils.exception_handler import handle_exceptions
from src.utils.circuit_breaker import CircuitBreaker

logger = get_logger(__name__)


class PipelineStage(ABC):
    """Abstract base class for pipeline stages."""
    
    def __init__(self, name: str):
        self.name = name
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=60,
            name=f"{name}_circuit_breaker"
        )
    
    @abstractmethod
    def process(self, context: PipelineContext) -> PipelineContext:
        """Process the data in the context and return the updated context."""
        pass
    
    @handle_exceptions
    def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the stage with timing and circuit breaker."""
        logger.info(f"Executing pipeline stage: {self.name}")
        
        # Start timer for this stage
        context.start_timer(self.name)
        
        # Execute with circuit breaker
        try:
            result = self.circuit_breaker.execute(lambda: self.process(context))
        except Exception as e:
            logger.error(f"Error in pipeline stage {self.name}: {str(e)}")
            raise
        finally:
            # Stop timer for this stage
            context.stop_timer(self.name)
        
        return result


class Pipeline:
    """Main pipeline class that orchestrates the execution of stages."""
    
    def __init__(self, config: PipelineConfig, spark: Optional[SparkSession] = None):
        self.config = config
        self.spark = spark or self._create_spark_session()
        self.stages: List[PipelineStage] = []
        self.observers: List[Callable[[PipelineContext], None]] = []
        logger.info(f"Initialized pipeline: {config.pipeline_name}")
    
    def _create_spark_session(self) -> SparkSession:
        """Create a Spark session if one is not provided."""
        logger.info("Creating Spark session")
        return (
            SparkSession.builder
            .appName(self.config.pipeline_name)
            .getOrCreate()
        )
    
    def add_stage(self, stage: PipelineStage) -> 'Pipeline':
        """Add a stage to the pipeline."""
        self.stages.append(stage)
        logger.info(f"Added stage to pipeline: {stage.name}")
        return self
    
    def add_observer(self, observer: Callable[[PipelineContext], None]) -> 'Pipeline':
        """Add an observer that will be notified after each stage."""
        self.observers.append(observer)
        return self
    
    def _notify_observers(self, context: PipelineContext) -> None:
        """Notify all observers with the current context."""
        for observer in self.observers:
            try:
                observer(context)
            except Exception as e:
                logger.warning(f"Error in pipeline observer: {str(e)}")
    
    @handle_exceptions
    def run(self) -> PipelineContext:
        """Run the pipeline with all stages."""
        logger.info(f"Starting pipeline execution: {self.config.pipeline_name}")
        
        # Create initial context
        context = PipelineContext(
            config=self.config,
            spark=self.spark
        )
        
        # Execute each stage
        for stage in self.stages:
            context = stage.execute(context)
            self._notify_observers(context)
        
        # Log execution report
        if self.config.track_execution_time:
            report = context.get_execution_report()
            logger.info(f"Pipeline execution completed. Total time: {report['total_time']:.2f} seconds")
            for stage_name, time in report['stages'].items():
                logger.info(f"  {stage_name}: {time:.2f} seconds")
        
        return context


class TemplateStage(PipelineStage, ABC):
    """Template method pattern implementation for pipeline stages."""
    
    def process(self, context: PipelineContext) -> PipelineContext:
        """Template method that defines the algorithm structure."""
        # 1. Validate input
        self.validate_input(context)
        
        # 2. Load data if needed
        if self.should_load_data(context):
            self.load_data(context)
        
        # 3. Process data
        self.process_data(context)
        
        # 4. Save data if needed
        if self.should_save_data(context):
            self.save_data(context)
        
        # 5. Update metadata
        self.update_metadata(context)
        
        return context
    
    def validate_input(self, context: PipelineContext) -> None:
        """Validate the input data and configuration."""
        pass
    
    def should_load_data(self, context: PipelineContext) -> bool:
        """Determine if data should be loaded from an intermediate source."""
        return False
    
    def load_data(self, context: PipelineContext) -> None:
        """Load data from an intermediate source."""
        pass
    
    @abstractmethod
    def process_data(self, context: PipelineContext) -> None:
        """Process the data in the context."""
        pass
    
    def should_save_data(self, context: PipelineContext) -> bool:
        """Determine if data should be saved to an intermediate target."""
        return False
    
    def save_data(self, context: PipelineContext) -> None:
        """Save data to an intermediate target."""
        pass
    
    def update_metadata(self, context: PipelineContext) -> None:
        """Update metadata in the context."""
        pass