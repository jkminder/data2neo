from rel2graph.core.factories.matcher import Matcher

def update_matcher(graph):
    """Hack to use mock matcher"""
    Matcher.graph_matcher = graph.matcher

def eq_node(rnode, gnode):
    # same labels
    if set(rnode[0]) != gnode.labels:
        return False
    # same number of properties
    if len(rnode[1]) != len(gnode):
        return False
    # same properties
    for (key, value) in rnode[1].items():
        if str(gnode[key]) != str(value):
            return False
    return True

def eq_relation(rrel, grel):
    # same type
    if rrel[1] != type(grel).__name__:
        return False
    # same from and to
    if not eq_node(rrel[0], grel.start_node) or not eq_node(rrel[2], grel.end_node):
        return False
    # same number of properties
    if len(rrel[3]) != len(grel):
        return False
    # same properties
    for (key, value) in rrel[3].items():
        if str(grel[key]) != str(value):
            return False
    return True
    
def compare_nodes(graph, result):
    print("Graph Nodes: ", graph.nodes)
    print("Result Nodes: ", result["nodes"])
    assert len(graph.nodes) == len(result["nodes"]), "Same number of nodes"
    for rnode in result["nodes"]:
        found = False
        for gnode in graph.nodes:
            found = found or eq_node(rnode, gnode)
            if found:
                break
        assert found, f"The following node was not found: {rnode}"

def compare_relations(graph, result):
    print("Graph Relations: ", graph.relations)
    print("Result Relations: ", result["relations"])
    assert len(graph.relations) == len(result["relations"]), "Same number of relations"
    for rrel in result["relations"]:
        found = False
        for grel in graph.relations:
            found = found or eq_relation(rrel, grel)
            if found:
                break
        assert found, f"The following relation was not found: {rrel}"

def compare(graph, result):
    compare_nodes(graph, result)
    compare_relations(graph, result)
