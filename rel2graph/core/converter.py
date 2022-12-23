#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main module that converts Relational Database entities into graphs.

authors: Julian Minder
"""
from typing import Dict, Tuple, Iterable, List, Any
import sys
import logging
import threading
from enum import IntEnum
import time
import os
import multiprocessing as mp
from py2neo import Graph, Node, Relationship
from itertools import chain

from .resource_iterator import ResourceIterator
from .graph_elements import Graph, Subgraph, NodeMatcher, Attribute, Relation
from .schema_compiler import compile_schema
from .factories import Matcher, Resource
from .global_state import GlobalSharedState

logger = logging.getLogger(__name__)

class WorkType(IntEnum):
    NODE = 0
    RELATION = 1

__process_config = None

def _get_graph():
    return __process_config.graph

class WorkerConfig:
    """Config container including information, data and configuration of the convertion that has to be done.
    
    Attributes:
        schema: The schema that is used to convert the resources
        factories: The factories that are used to convert the resources
        graph_lock: Lock to ensure that only one thread is writing to the graph at a time
        graph: The graph that is used to store the converted resources
        exit_flag: Flag that is set when the worker should exit
        nodes_flag: Flag that is set when the worker should process nodes
        processed_resources: Counter for the number of processed resources
        processed_nodes: Counter for the number of processed nodes
        processed_relations: Counter for the number of processed relations
        processed_lock: Lock to ensure that only one process is writing to the counters at a time
    """

    def __init__(self) -> None:
        """Initialises a Worker config with the required data. Part of the data is set when the worker is started.
        
        Args:
            schema: The schema that is used to convert the resources
        """
        self.factories, self.node_mask, self.relation_mask = None, None, None
        self.graph_lock = mp.Lock()
        self.graph = None

        self.exit_flag = mp.Event()
        self.exit_flag.clear()
        self.nodes_flag = mp.Event()
        self.processed_resources = mp.Value('i', 0)
        self.processed_nodes = mp.Value('i', 0)
        self.processed_relations = mp.Value('i', 0)
        self.processed_lock = mp.Lock()

    def set_work_type(self, work_type: WorkType):
        if work_type == WorkType.NODE:
            self.nodes_flag.set()
        else:
            self.nodes_flag.clear()

def commit_wrap(function):
    """Wraps the graph commit function into try except block with retry. """
    try:
        function()
    except Exception as e:
        logger.error(f"Py2neo Exception '{type(e).__name__}': " + str(e))
        logger.error("Sleeping for 10 second and retrying commit")
        time.sleep(10)
        function()

def commit_batch(to_create: Subgraph, to_merge: Subgraph) -> None:
        """"Commits processed batch to graph."""
        nodes_committed = 0 
        relations_committed = 0
        
        # Creating does not rely on synchronous executions
        if len(to_create.nodes) + len(to_create.relationships) > 0:
            commit_wrap(lambda: __process_config.graph.create(to_create))
            nodes_committed += len(to_create.nodes)
            relations_committed += len(to_create.relationships)
            
        # Merging nodes requires serialization (synchronous executions) between processes
        # Using locks to enforce this
        if len(to_merge.nodes) + len(to_merge.relationships) > 0:
            with __process_config.graph_lock:
                commit_wrap(lambda: __process_config.graph.merge(to_merge))
            nodes_committed += len(to_merge.nodes)
            relations_committed += len(to_merge.relationships)

        # Update the processed nodes and relations
        if nodes_committed > 0 or relations_committed > 0:
            with __process_config.processed_lock:
                __process_config.processed_nodes.value += nodes_committed
                __process_config.processed_relations.value += relations_committed



def picklize_supplies(resource) -> Resource:
    """
    Removes the graph from the nodes and relations of the resource. Further we parse the relations to a tuple because
    of their dynamic type.
    """
    for key in resource.supplies:
        supply = resource.supplies[key]
        for node in supply.nodes:
            if node.graph is not None:
                node.graph = 0
        if not isinstance(supply, Node) and supply.relationships is not None:
            relations = []
            if resource.supplies[key].relationships is not None:
                for rel in supply.relationships:
                    if rel.start_node.graph is not None:
                        rel.start_node.graph = 0
                    if rel.end_node.graph is not None:
                        rel.end_node.graph = 0
                    relations.append((rel.start_node, rel.__class__.__name__, rel.end_node, rel.identity))
            resource.supplies[key] = (set(supply.nodes), relations)

def rebuild_supplies(resource: Resource) -> Resource:
    """
    Adds the graph to the nodes and relations of the resource
    """
    for key in resource.supplies: 
        if isinstance(resource.supplies[key], tuple):
            relations = []
            for (start, typename, end, identity) in resource.supplies[key][1]:
                rel = Relationship(start, typename, end)
                rel.identity = identity
                if identity is not None:
                    rel.graph = __process_config.graph
                    rel.start_node.graph = __process_config.graph
                    rel.end_node.graph = __process_config.graph
                relations.append(rel)
            resource.supplies[key] = Subgraph(resource.supplies[key][0], relations)
        for node in resource.supplies[key].nodes:
            if node.graph == 0:
                node.graph = __process_config.graph

def process_batch(batch) -> None:
    """
    Main conversion processing. While the worker is not notified to stop (with the exit flag), 
    it repeatedly askes the iterator for a next resource to process. If the iterator reports that the
    data is traversed fully, the method returns.
    """
    # __process_config is a global variable that contains the configuration for the current process
    try:
        work_type = WorkType.NODE if __process_config.nodes_flag.is_set() else WorkType.RELATION
        mask = __process_config.node_mask if work_type == WorkType.NODE else __process_config.relation_mask
        to_merge = [[], []] # List of resources to merge (nodes, rels)
        to_create = [[], []] # List of resources to create (nodes, rels)
        processed_resources = []
        for i, resource in enumerate(batch) :
            if __process_config.exit_flag.is_set():
                # If the exit flag is set, we stop processing
                return []
            if resource.type in mask:
                # If the resource is not in the mask (meaning we don't need to convert it), we skip it
                # This is mainly done for performance reasons, as the conversion is not needed

                logger.debug(f"Processing {resource}")

                # add graph to resource (only necessary for relations)
                if work_type == WorkType.RELATION:
                    rebuild_supplies(resource)
                
                factory = __process_config.factories[resource.type][int(work_type)]

                try:
                    subgraph = factory.construct(resource)
                except Exception as err:
                    err.args += (f"Encountered error when processing {'nodes' if __process_config.nodes_flag.is_set() else 'relations'} of {resource}.",)
                    raise err
                
                
                # We sort the subgraph based on if its parts should be 
                # merged or just created. This is selected based on if the
                # __primarykey__ property is set. Note that for relations 
                # this only matters if you use the GraphWithParallelRelations from
                # rel2graph.py2neo_extensions (otherwise relations are always merged)

                for node in subgraph.nodes:
                    if node.__primarykey__ is not None:
                        # If a primary key is existing we merge the node to the graph
                        to_merge[0].append(node)
                    else:
                        to_create[0].append(node)
                for relation in subgraph.relationships:
                    if getattr(relation, "__primarykey__", None) is not None:
                        # If a primary key is existing we merge the relation to the graph
                        to_merge[1].append(relation)
                        pass
                    else:
                        # If no primary key is existing we create the relation to the graph
                        relation.__primarykey__ = -1
                        to_create[1].append(relation)
            if i % 1000 == 0 and i > 0:
                # Update counter
                with __process_config.processed_lock:
                    __process_config.processed_resources.value += 1000

            processed_resources.append(resource)

        # Update counter
        with __process_config.processed_lock:
            __process_config.processed_resources.value += len(processed_resources) % 1000
        
        to_create = Subgraph(*to_create)
        to_merge = Subgraph(*to_merge)
        commit_batch(to_create, to_merge)

            
    except Exception as err:
        logger.error(str(err))
        logger.debug(f"Exiting Worker " + str(mp.current_process().pid))
        raise err
    
    if work_type == WorkType.NODE:
        # Prepare resources for pickling
        for resource in processed_resources:
            picklize_supplies(resource)
        return processed_resources
    else:
        # No need to return anything for relations as no synchronization is needed
        return []

class Batcher:
    def __init__(self, batch_size: int, iterator: Iterable):
        self._batch_size = batch_size
        self._iterator = iter(iterator)

    def __iter__(self):
        return self

    def __next__(self):
        batch = []
        for _ in range(self._batch_size):
            try:
                batch.append(next(self._iterator))
            except StopIteration:
                if len(batch) == 0:
                    raise StopIteration
                else:
                    return batch
        return batch

def update_progress_bar(progress_bar, num, exit_flag) -> None:
    while(True):
        if exit_flag.is_set():
            return
        n = num.value
        # Set value of progress bar
        progress_bar.n = n
        progress_bar.refresh()
        time.sleep(0.1)

def init_process_state(proc_config: WorkerConfig, graph_class: type, graph_profile: "py2neo.ServiceProfile", conversion_objects: Tuple, global_shared_state: Dict[str, Any]):
    '''Initialize each process with a global config.
    '''
    global __process_config
    __process_config = proc_config
    __process_config.graph = graph_class(profile=graph_profile)

    # Load the conversion objects
    factories,node_mask,relation_mask = conversion_objects
    __process_config.factories = factories
    __process_config.node_mask = node_mask
    __process_config.relation_mask = relation_mask
    
    # Set the matcher to the graph
    # TODO: This is a hacky way to set the matcher to the graph. 
    Matcher.graph_matcher = NodeMatcher(__process_config.graph)

    # Set the global shared state
    GlobalSharedState.set_state(global_shared_state)
    GlobalSharedState.graph = __process_config.graph

def cleanup_process_state():
    '''Cleanup the process state. Only used in serial processing.
    '''
    global __process_config
    del __process_config.graph
    del __process_config
    __process_config = None
    del GlobalSharedState.graph


class Converter:
    """The converter handles the whole conversion pipeline.  """

    def __init__(self, schema: str, iterator: ResourceIterator, graph: Graph, num_workers: int = 1, serialize: bool = False, batch_size: int = 5000, global_vars: List[Any] = []) -> None:
        """Initialises a converter. Note that this is a singleton and only the most recent instantiation is valid.
        
        Args:
            schema: The schema to convert.
            iterator: The resource iterator.
            graph: The neo4j graph (from py2neo)
            num_workers: The number of parallel workers. Please make sure that your usage supports parallelism. To use serial processing set this to 1. (default: 1)
            serialize: If true, the converter will make sure that all resources are processed serially and does not use any buffering. This is useful if you want to make sure that all resources are processed 
                and committed to the graph in the same order as they are returned by the iterator. Note that you can't set both serialize to true and set num_workers > 1. (default: False)
            batch_size: The batch size for the parallel processing. (default: 5000)
            global_vars: A list of global variables that should be available in the workers. Make sure they are picklizable and synchronized. (default: [])
        """
        if serialize and num_workers > 1:
            raise ValueError("You can't use serialization and parallel processing (num_workers > 1) at the same time.")

        # Compile the schema 
        self._factories, self._node_mask, self._relation_mask =  compile_schema(schema) 


        self.iterator = iterator
        self._graph = graph
        self._num_workers = num_workers

        # Parse the schema and compile it into factories
        self.schema = schema

        # Serialization
        self._serialize = serialize
        
        # Batch size
        self._batch_size = batch_size

        # Global variables
        self._global_vars = global_vars
        
    @property
    def iterator(self) -> ResourceIterator:
        """Gets the resource iterator"""
        return self._iterator

    @iterator.setter
    def iterator(self, iterator: ResourceIterator) -> None:
        """Sets the resource iterator"""
        self._iterator = iterator

    def _process_iteration(self, pool: mp.Pool, iterator: Iterable, config: WorkerConfig) -> None:
        """Runs one iteration of the conversion pipeline.
        """
        processed_resources = []
        try:
            result = pool.imap_unordered(process_batch, Batcher(self._batch_size, iterator))
            for batch in result:
                processed_resources.extend(batch)
        except KeyboardInterrupt as e:
            # KeyboardInterrupt is raised on the main exec thread
            # -> Cleanup all workers
            config.exit_flag.set()
            pool.terminate()
            raise e
        return processed_resources

    def __call__(self, progress_bar: "tdqm.tqdm" = None, skip_nodes = False, skip_relations = False) -> None:
        """Runs the convertion and commits the produced nodes and relations to the graph.
        
        Args:
            progress_bar: An optional tqdm like instance for a progress bar. 
            skip_nodes: (default: False) If true creation of nodes will be skiped. ATTENTION: this might lead to problems if you use identifiers.
            skip_relation: If true creation of relations will be skiped (default: False)
        """
        config = WorkerConfig()

        conversion_objects = (self._factories, self._node_mask, self._relation_mask)

        # Handle progress bar (create new or update it)
        pb = None
        if progress_bar is not None:
            pb = progress_bar(total=2*len(self._iterator))
            if skip_nodes:
                pb.update(len(self._iterator))
            
            pb_updater = threading.Thread(target=update_progress_bar, args=(pb, config.processed_resources, config.exit_flag), daemon=True)
            pb_updater.start()
            

        start = time.time()
        # Initiate the main process
        # init_process_state(config, self._graph.__class__, self._graph.service.profile, conversion_objects, self._global_vars)

        if not self._serialize:
            logger.info(f"Running convertion with {self._num_workers} parallel workers.")
            with mp.Pool(processes=self._num_workers, initializer=init_process_state, 
                        initargs=(config, self._graph.__class__, self._graph.service.profile, conversion_objects, GlobalSharedState.get_state())) as pool:
                if not skip_nodes:
                    config.set_work_type(WorkType.NODE)
                    logger.info("Starting creation of nodes.")
                    
                    processed_resources = self._process_iteration(pool, self._iterator, config)
                else:
                    logger.info("Skipping creation of nodes.")

                if not skip_relations:  
                    config.set_work_type(WorkType.RELATION)
                    logger.info("Starting creation of relations.")

                    self._process_iteration(pool, processed_resources, config)
                else:
                    logger.info("Skipping creation of relations.")
        else:

            # Serialize the processing
            try:
                # Initialize the process state
                init_process_state(config, self._graph.__class__, self._graph.service.profile, conversion_objects, GlobalSharedState.get_state())
                logger.info("Starting serial processing.")
                if not skip_nodes:
                    config.set_work_type(WorkType.NODE)
                    logger.info("Starting creation of nodes.")
                    pmap = map(process_batch, Batcher(self._batch_size, self._iterator))
                    processed_resources = []
                    for batch in pmap:
                        processed_resources.extend(batch)
                else:
                    logger.info("Skipping creation of nodes.")

                if not skip_relations:
                    config.set_work_type(WorkType.RELATION)
                    logger.info("Starting creation of relations.")
                    list(map(process_batch, Batcher(self._batch_size, processed_resources)))
                else:
                    logger.info("Skipping creation of relations.")
            finally:
                # Cleanup the process state
                cleanup_process_state()
        
        # make sure that the progress bar is updated one last time
        if pb is not None:
            config.exit_flag.set()
            pb_updater.join()
            pb.n = config.processed_resources.value
            pb.refresh()
            pb.close()        
        logger.info(f"Processed in total {config.processed_nodes.value} nodes and {config.processed_relations.value} relations (this run took {int(time.time()-start)}s)")