from rel2graph.core.factories.matcher import Matcher

def update_matcher(graph):
    """Hack to use mock matcher"""
    Matcher.graph_matcher = graph.matcher

def compare_node(rnode, gnode):
    # same labels
    if set(rnode[0]) != gnode.labels:
        return False
    # same number of properties
    if len(rnode[1]) != len(gnode):
        return False
    # same properties
    for (key, value) in rnode[1].items():
        if gnode[key] != value:
            return False
    return True
        
def compare_nodes(graph, result):
    assert len(graph.nodes) == len(result["nodes"]), "Same number of nodes"
    for rnode in result["nodes"]:
        found = False
        for gnode in graph.nodes:
            found = found or compare_node(rnode, gnode)
            if found:
                break
        assert rnode and found, f"The following node was not found: {rnode}"

def compare_relations(graph, result):
    pass

def compare(graph, result):
    compare_nodes(graph, result)
    compare_relations(graph, result)
