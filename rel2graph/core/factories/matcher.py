import logging
from typing import List, Tuple

from .resource import Resource
from .registrar import register_factory
from ..graph_elements import Node, NodeMatcher

logger = logging.getLogger(__name__)

@register_factory
class Matcher:
    """A Matcher dynamically matches nodes based on provided labels and conditions and a resource.
    
    It is used for RelationFactories that need to dynamically match nodes based on a resource

    Static Attributes:
        graph_matcher: The graph matcher that is used to query the graph, must be set before querying
    """
    graph_matcher: NodeMatcher = None


    def __init__(self, node_id: str = None, *conditions: 'AttributeFactory') -> None:
        """Inits a Matcher with either a node_id XOR a list of conditions
        
        The object musst be initiated by either a node_id (exclusive) or a list conditions. 
        Based on if an attribute_key is set, a condition is interpreted as 
        label or as attribute condition. If conditions are set, the matcher will query the graph
        for nodes matching all labels and all attribute conditions.
    
        Args:
            node_id: String that represents a unique node. The matcher will assume that the
                     searched node can be found in resources.supplies[node_id] upon calling
                     match(resource).

            conditions: List of Conditions (AttributeFactories). If the attribute_key of the AttributeFactory
                        is not set its interpreted as an label condition, else an attribute condition.
        Raises:
            ValueError: Neither node_id nor conditions are provided or no node_id and no label in conditions
        """
        if node_id is None and len(conditions) == 0:
            raise ValueError("Matcher: Either node_id and labels or conditions must be provided")
        self._node_id = node_id
        self._labels = []
        self._conditions = []
        for attr in conditions:
            if attr.attribute_key is not None:
                self._conditions.append(attr)
            else:
                self._labels.append(attr)
        if len(self._labels) == 0 and self._node_id is None:
            raise ValueError("At least one label must be provided")


    def match(self, resource: Resource) -> List[Node]:
        """Matches Nodes based on the settings (from init) and the resource
        
        Args:
            resource: A Resource containing any information needed for the matching
        Raises:
            KeyError: node_id key does not exist in resource.supplies
        """

        if self._node_id is not None:
            try:
                node = resource.supplies[self._node_id]
            except KeyError:
                raise KeyError(f"Matcher: The provided resource does not contain the supply {self._node_id}")
            return [node]
        else:
            constructed_conditions = [attr.construct(resource) for attr in self._conditions if attr]
            parsed_conditions = dict([(attr.key, attr.value) for attr in constructed_conditions if attr is not None])
            if len(parsed_conditions) == 0 and len(constructed_conditions) > 0:
                # the attribute factories removed attributes that are normaly existing, there must be a wrapper in action
                # we do not match anymore (no conditions means that no match should be made)
                return []
            constructed_labels = [label_factory.construct(resource) for label_factory in self._labels]
            parsed_labels = [attr.value for attr in constructed_labels if attr is not None]

            logger.debug(f"Matching based on labels: '{parsed_labels}' and conditions: {parsed_conditions}")
            match_list = Matcher.graph_matcher.match(*parsed_labels, 
                                                   **parsed_conditions).all()
            return match_list

