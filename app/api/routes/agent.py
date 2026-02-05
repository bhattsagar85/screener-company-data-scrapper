from fastapi import APIRouter

from app.api.schemas.agent import AgentAskRequest, AgentAskResponse
from app.agents.graph import build_screener_graph

router = APIRouter(prefix="/agent", tags=["Agent"])

graph = build_screener_graph()

@router.post("/ask", response_model=AgentAskResponse)
def ask_agent(payload: AgentAskRequest):
    result = graph.invoke({
        "query": payload.query
    })

    return {
        "answer": result.get("answer", "")
    }
