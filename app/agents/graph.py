from langgraph.graph import StateGraph
from app.agents.state import AgentState
from app.agents.nodes import (
    parse_query,
    ingest_data,
    answer,
)

def build_screener_graph():
    """
    Screener Agent LangGraph
    Flow:
    parse_query -> ingest_data -> generate_answer
    """

    graph = StateGraph(AgentState)

    # ---- Nodes ----
    graph.add_node("parse", parse_query)
    graph.add_node("ingest", ingest_data)
    graph.add_node("generate_answer", answer)  # ✅ renamed

    # ---- Flow ----
    graph.set_entry_point("parse")
    graph.add_edge("parse", "ingest")
    graph.add_edge("ingest", "generate_answer")

    return graph.compile()
