from abc import ABC, abstractmethod
from typing import Any, Dict, List

class BaseDataSource(ABC):
    """
    Abstract base class for a FinOps data source.
    Defines the common interface for all data sources.
    """

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the data source."""
        pass

    @abstractmethod
    async def get_cost_summary(self) -> Dict[str, Any]:
        """Get overall cost summary."""
        pass

    @abstractmethod
    async def get_expensive_queries(self) -> Dict[str, Any]:
        """Get list of expensive queries."""
        pass

    @abstractmethod
    async def get_project_costs(self) -> Dict[str, Any]:
        """Get cost breakdown by project."""
        pass

    @abstractmethod
    async def get_cost_trends(self) -> Dict[str, Any]:
        """Get historical cost trends."""
        pass

    @abstractmethod
    async def analyze_query_cost(self, query: str, dry_run: bool) -> Dict[str, Any]:
        """Analyze the cost of a query."""
        pass

    @abstractmethod
    async def get_cost_by_user(self, days: int) -> Dict[str, Any]:
        """Get cost breakdown by user."""
        pass