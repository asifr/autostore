from autostore.handlers.base import DataHandler
from typing import List, Dict, Optional, Type, Any


class HandlerRegistry:
    """Registry for managing data handlers."""

    def __init__(self):
        self._handlers: List[DataHandler] = []
        self._extension_map: Dict[str, List[DataHandler]] = {}

    def register(self, handler: DataHandler) -> None:
        """Register a new handler."""
        self._handlers.append(handler)

        # Update extension mapping
        for ext in handler.extensions:
            ext_lower = ext.lower()
            if ext_lower not in self._extension_map:
                self._extension_map[ext_lower] = []
            self._extension_map[ext_lower].append(handler)
            # Sort by priority (higher priority first)
            self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def unregister(self, handler_class: Type[DataHandler]) -> None:
        """Unregister a handler by class type."""
        # Remove from main list
        self._handlers = [h for h in self._handlers if not isinstance(h, handler_class)]

        # Rebuild extension mapping
        self._extension_map.clear()
        for handler in self._handlers:
            for ext in handler.extensions:
                ext_lower = ext.lower()
                if ext_lower not in self._extension_map:
                    self._extension_map[ext_lower] = []
                self._extension_map[ext_lower].append(handler)
                self._extension_map[ext_lower].sort(key=lambda h: h.priority, reverse=True)

    def get_handler_for_extension(self, extension: str) -> Optional[DataHandler]:
        """Get the best handler for a given file extension."""
        ext_lower = extension.lower()
        handlers = self._extension_map.get(ext_lower, [])
        return handlers[0] if handlers else None

    def get_handler_for_file(self, file_path: str, format_override: Optional[str] = None) -> Optional[DataHandler]:
        """Get handler with format override support."""
        if format_override:
            ext = f".{format_override.lstrip('.')}"
            return self.get_handler_for_extension(ext)

        # Use file extension
        from pathlib import Path

        ext = Path(file_path).suffix.lower()
        return self.get_handler_for_extension(ext)

    def get_handler_for_data(self, data: Any) -> Optional[DataHandler]:
        """Get the best handler for a given data instance."""
        compatible_handlers = []
        for handler in self._handlers:
            if handler.can_handle_data(data):
                compatible_handlers.append(handler)

        # Sort by priority and return the best match
        compatible_handlers.sort(key=lambda h: h.priority, reverse=True)
        return compatible_handlers[0] if compatible_handlers else None

    def get_supported_extensions(self) -> List[str]:
        """Get all supported file extensions."""
        return list(self._extension_map.keys())
