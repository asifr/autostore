from autostore.handlers.base import DataHandler
from autostore.handlers.registry import HandlerRegistry
from autostore.handlers.core import JSONHandler, TextHandler, PickleHandler, YAMLHandler, JSONLHandler
from autostore.handlers.data import ParquetHandler, CSVHandler, NumpyHandler, SparseHandler, ImageHandler
from autostore.handlers.ml import TorchHandler, PydanticHandler, DataclassHandler

# Create default registry with all handlers
def create_default_registry() -> HandlerRegistry:
    """Create a HandlerRegistry with all built-in handlers."""
    registry = HandlerRegistry()
    
    # Core handlers
    registry.register(JSONHandler())
    registry.register(JSONLHandler())
    registry.register(TextHandler())
    registry.register(PickleHandler())
    registry.register(YAMLHandler())
    
    # Data science handlers
    registry.register(ParquetHandler())
    registry.register(CSVHandler())
    registry.register(NumpyHandler())
    registry.register(SparseHandler())
    registry.register(ImageHandler())
    
    # ML handlers
    registry.register(TorchHandler())
    registry.register(PydanticHandler())
    registry.register(DataclassHandler())
    
    return registry