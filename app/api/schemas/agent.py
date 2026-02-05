from pydantic import BaseModel

class AgentAskRequest(BaseModel):
    query: str


class AgentAskResponse(BaseModel):
    answer: str
