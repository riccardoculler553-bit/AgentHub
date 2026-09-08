"""V1.2 agent graph assembly (PDF §14/§105-§106).

    START -> understand -> load_context -> plan -> llm_decide
          -> (execute_tool -> observe -> evaluate -> llm_decide)*
          -> ask_user | build_reply -> END

The graph is compiled per run with that run's ToolPolicy closed over, so
concurrent runs never share a call ledger.
"""

from typing import Any

from langgraph.graph import END, START, StateGraph

from app.agent.core.state import AgentState
from app.agent.graph.nodes import build_nodes
from app.agent.graph.routing import (
    route_after_decide,
    route_after_evaluate,
    route_after_execute,
    route_after_plan,
)


def build_graph(runner: Any, policy: Any):
    nodes = build_nodes(runner, policy)
    graph = StateGraph(AgentState)

    for name, fn in nodes.items():
        graph.add_node(name, fn)

    graph.add_edge(START, "understand")
    graph.add_edge("understand", "load_context")
    graph.add_edge("load_context", "plan")
    graph.add_conditional_edges(
        "plan",
        route_after_plan,
        {"llm_decide": "llm_decide", "execute_tool": "execute_tool"},
    )
    graph.add_conditional_edges(
        "llm_decide",
        route_after_decide,
        {
            "execute_tool": "execute_tool",
            "ask_user": "ask_user",
            "build_reply": "build_reply",
        },
    )
    graph.add_conditional_edges(
        "execute_tool",
        route_after_execute,
        {"observe": "observe", "ask_user": "ask_user"},
    )
    graph.add_edge("observe", "evaluate")
    graph.add_conditional_edges(
        "evaluate",
        route_after_evaluate,
        {"llm_decide": "llm_decide", "build_reply": "build_reply"},
    )
    graph.add_edge("ask_user", END)
    graph.add_edge("build_reply", END)
    return graph.compile()
