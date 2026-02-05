from typing import TypedDict, Optional

class AgentState(TypedDict):
    query: str
    ticker: Optional[str]
    metric: Optional[str]
    answer: Optional[str]
