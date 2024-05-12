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
from itertools import chain
import pickle
from neo4j import GraphDatabase, Auth, Driver

from .resource_iterator import ResourceIterator
from ..neo4j import Subgraph, Relationship, Node, create, merge
from .schema_compiler import compile_schema
from .factories import Matcher, Resource
from .global_state import GlobalSharedState

logger = logging.getLogger(__name__)

class WorkType(IntEnum):
    NODE = 0
    RELATIONSHIP = 1

__process_config = None


class WorkerConfig:
    """Config container including information, data and configuration of the convertion that has to be done.
    
    Attributes:
        schema: The schema that is used to convert the resources
        factories: The factories that are used to convert the resources
        graph_lock: Lock to ensure that only one process is writing to the graph at a time
        neo4j_uri: The uri of the neo4j database
        neo4j_auth: The authentication for the neo4j database 
        exit_flag: Flag that is set when the worker should exit
        nodes_flag: Flag that is set when the worker should process nodes
        processed_resources: Counter for the number of processed resources
        processed_nodes: Counter for the number of processed nodes
        processed_relationships: Counter for the number of processed relationships
        processed_lock: Lock to ensure that only one process is writing to the counters at a time
    """

    def __init__(self, neo4j_uri: str, neo4j_auth: Auth) -> None:
        """Initialises a Worker config with the required data. Part of the data is set when the worker is started.
        
        Args:
            neo4j_uri: The uri of the neo4j database
            neo4j_auth: The authentication for the neo4j database
        """
        self.factories, self.node_mask, self.relationship_mask = None, None, None
        self.graph_lock = mp.Lock()
        self.neo4j_uri = neo4j_uri
        self.neo4j_auth = neo4j_auth
        self.exit_flag = mp.Event()
        self.exit_flag.clear()
        self.nodes_flag = mp.Event()
        self.processed_resources = mp.Value('i', 0)
        self.processed_nodes = mp.Value('i', 0)
        self.processed_relationships = mp.Value('i', 0)
        self.processed_lock = mp.Lock()

        self._graph_driver = None
    
    @property
    def graph_driver(self) -> Driver:
        """Gets the graph driver."""
        if self._graph_driver is None:
            raise ValueError("Graph driver is not set. Please call setup() first.")
        return self._graph_driver
    
    def setup(self) -> None:
        """Sets up the worker config. This is called when the worker is started."""
        self._graph_driver = GraphDatabase.driver(self.neo4j_uri, auth=self.neo4j_auth)

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
        logger.error(f"Neo4j Exception '{type(e).__name__}': " + str(e))
        logger.error("Sleeping for 10 second and retrying commit")
        time.sleep(10)
        function()

def commit_batch(to_create: Subgraph, to_merge: Subgraph) -> None:
        """"Commits processed batch to graph."""
        nodes_committed = 0 
        relationships_committed = 0
        
        # Creating nodes does not rely on serialized executions
        # If there are relationships to create, we need to serialize the creation
        # TODO: We could split the creation of nodes and relationships into two separate branches, might be more efficient
        #       but considering that in almost all cases no relationships are created in the first loop, it's not worth it
        if len(to_create.relationships) > 0:
            with __process_config.graph_lock:
                with __process_config.graph_driver.session() as session:
                    commit_wrap(lambda: create(to_create, session))
            relationships_committed += len(to_create.relationships)
        elif len(to_create.nodes) > 0:
            with __process_config.graph_driver.session() as session:
                commit_wrap(lambda: create(to_create, session))
            nodes_committed += len(to_create.nodes)

        # Merging nodes requires serialization (synchronous executions) between processes
        # Using locks to enforce this
        if len(to_merge.nodes) + len(to_merge.relationships) > 0:
            with __process_config.graph_lock:
                with __process_config.graph_driver.session() as session:
                    commit_wrap(lambda: merge(to_merge, session))
            nodes_committed += len(to_merge.nodes)
            relationships_committed += len(to_merge.relationships)

        # Update the processed nodes and relations
        if nodes_committed > 0 or relationships_committed > 0:
            with __process_config.processed_lock:
                __process_config.processed_nodes.value += nodes_committed
                __process_config.processed_relationships.value += relationships_committed

def process_batch(batch) -> None:
    """
    Main conversion processing. While the worker is not notified to stop (with the exit flag), 
    it repeatedly askes the iterator for a next resource to process. If the iterator reports that the
    data is traversed fully, the method returns.
    """
    # __process_config is a global variable that contains the configuration for the current process
    batch = pickle.loads(batch)
    try:
        work_type = WorkType.NODE if __process_config.nodes_flag.is_set() else WorkType.RELATIONSHIP
        mask = __process_config.node_mask if work_type == WorkType.NODE else __process_config.relationship_mask
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
                
                factory = __process_config.factories[resource.type][int(work_type)]

                try:
                    subgraph = factory.construct(resource)
                except Exception as err:
                    err.args += (f"Encountered error when processing {'nodes' if __process_config.nodes_flag.is_set() else 'relationships'} of {resource}.",)
                    raise err
                
                
                # We sort the subgraph based on if its parts should be 
                # merged or just created. This is selected based on if the
                # __primarykey__ property is set. 

                for node in subgraph.nodes:
                    if node.__primarykey__ is not None:
                        # If a primary key is existing we merge the node to the graph
                        to_merge[0].append(node)
                    else:
                        to_create[0].append(node)
                for relationship in subgraph.relationships:
                    if getattr(relationship, "__primarykey__", None) is not None:
                        # If a primary key is existing we merge the relationship to the graph
                        to_merge[1].append(relationship)
                        pass
                    else:
                        # If no primary key is existing we create the relationship to the graph
                        relationship.__primarykey__ = -1
                        to_create[1].append(relationship)
            processed_resources.append(resource)

        
        to_create = Subgraph(*to_create)
        to_merge = Subgraph(*to_merge)
        commit_batch(to_create, to_merge)

        # Update counter
        with __process_config.processed_lock:
            __process_config.processed_resources.value += len(processed_resources)

            
    except Exception as err:
        logger.error(str(err))
        logger.debug(f"Exiting Worker " + str(mp.current_process().pid))
        raise err
    
    if work_type == WorkType.NODE:
        # For memory reasons we return the processed resources as a binary string
        bin_resources = pickle.dumps(processed_resources)
        return bin_resources
    else:
        # No need to return anything for relationship as no synchronization is needed
        return []

class Batcher:
    def __init__(self, batch_size: int, iterator: Iterable, binarize: bool = True):
        self._batch_size = batch_size
        self._iterator = iter(iterator)
        self._binarize = binarize

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
                break
        if self._binarize:
            batch = pickle.dumps(batch)
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

def init_process_state(proc_config: WorkerConfig, conversion_objects: Tuple, global_shared_state: Dict[str, Any]):
    '''Initialize each process with a global config.
    '''
    global __process_config
    __process_config = proc_config
    # Setup the worker config in the local process (this sets up the graph driver)
    __process_config.setup()

    # Load the conversion objects
    factories,node_mask,relationship_mask = conversion_objects
    __process_config.factories = factories
    __process_config.node_mask = node_mask
    __process_config.relationship_mask = relationship_mask
    
    # Set driver for matcher
    # TODO: This is a hacky way to set the matcher to the graph. 
    Matcher.graph_driver = __process_config.graph_driver

    # Set the global shared state
    GlobalSharedState.set_state(global_shared_state)
    GlobalSharedState._set_graph_driver(__process_config.graph_driver)

def cleanup_process_state():
    '''Cleanup the process state. Only used in serial processing.
    '''
    global __process_config
    del __process_config
    __process_config = None
    GlobalSharedState._del_graph_driver()


class Converter:
    """The converter handles the whole conversion pipeline.  """

    def __init__(self, schema: str, iterator: ResourceIterator, neo4j_uri: str, neo4j_auth: Auth, num_workers: int = None, serialize: bool = False, batch_size: int = 5000) -> None:
        """Initialises a converter. Note that this is a singleton and only the most recent instantiation is valid.
        
        Args:
            schema: The schema to convert.
            iterator: The resource iterator.
            neo4j_uri: The uri of the neo4j database.
            neo4j_auth: The authentication for the neo4j database.
            num_workers: The number of parallel workers. Please make sure that your usage supports parallelism. To use serial processing set this to 1. (default: cpu_count-2)
            serialize: If true, the converter will make sure that all resources are processed serially and does not use any buffering. This is useful if you want to make sure that all resources are processed 
                and committed to the graph in the same order as they are returned by the iterator. Note that you can't set both serialize to true and set num_workers > 1. (default: False)
            batch_size: The batch size for the parallel processing. (default: 5000)
        """
        if serialize:
            if num_workers is not None and num_workers > 1:
                raise ValueError("You can't use serialization and parallel processing (num_workers > 1) at the same time.")
            else:
                num_workers = 1
        elif num_workers is None:
            num_workers = mp.cpu_count() - 2
        
        # Verify connection to neo4j
        self._neo4j_uri = neo4j_uri
        self._neo4j_auth = neo4j_auth
        driver = GraphDatabase.driver(self._neo4j_uri, auth=self._neo4j_auth)
        driver.verify_connectivity()
        driver.close()

        # Compile the schema 
        if "RELATION(" in schema:
            raise DeprecationWarning("The RELATION keyword is deprecated. Please use RELATIONSHIP instead.")
        self._factories, self._node_mask, self._relationship_mask =  compile_schema(schema) 
        self.iterator = iterator
        self._num_workers = num_workers

        # Parse the schema and compile it into factories
        self.schema = schema

        # Serialization
        self._serialize = serialize
        
        # Batch size
        self._batch_size = batch_size

        
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
            result = pool.imap_unordered(process_batch, iterator)
            for i, batch in enumerate(result):
                processed_resources.append(batch)
        except KeyboardInterrupt as e:
            # KeyboardInterrupt is raised on the main exec thread
            # -> Cleanup all workers
            config.exit_flag.set()
            pool.terminate()
            raise e
        return processed_resources

    def __call__(self, progress_bar: "tdqm.tqdm" = None, skip_nodes = False, skip_relationships = False) -> None:
        """Runs the convertion and commits the produced nodes and relationships to the graph.
        
        Args:
            progress_bar: An optional tqdm like instance for a progress bar. 
            skip_nodes: (default: False) If true creation of nodes will be skiped. ATTENTION: this might lead to problems if you use identifiers.
            skip_relationships: If true creation of relationships will be skiped (default: False)
        """
        config = WorkerConfig(self._neo4j_uri, self._neo4j_auth)

        conversion_objects = (self._factories, self._node_mask, self._relationship_mask)

        # Handle progress bar (create new or update it)
        pb = None
        if progress_bar is not None:
            pb = progress_bar(total=2*len(self._iterator))
            if skip_nodes:
                pb.update(len(self._iterator))
            
            pb_updater = threading.Thread(target=update_progress_bar, args=(pb, config.processed_resources, config.exit_flag), daemon=True)
            pb_updater.start()
            

        start = time.time()

        if not self._serialize:
            logger.info(f"Running convertion with {self._num_workers} parallel workers.")
            with mp.Pool(processes=self._num_workers, initializer=init_process_state, 
                        initargs=(config, conversion_objects, GlobalSharedState.get_state()), 
                        maxtasksperchild=50) as pool:
                if not skip_nodes:
                    config.set_work_type(WorkType.NODE)
                    logger.info("Starting creation of nodes.")
                    
                    processed_batches = self._process_iteration(pool, Batcher(self._batch_size, self._iterator), config)
                else:
                    logger.info("Skipping creation of nodes.")

                if not skip_relationships:  
                    config.set_work_type(WorkType.RELATIONSHIP)
                    logger.info("Starting creation of relationships.")

                    self._process_iteration(pool, processed_batches, config)
                else:
                    logger.info("Skipping creation of relations.")
        else:

            # Serialize the processing
            try:
                # Initialize the process state
                init_process_state(config, conversion_objects, GlobalSharedState.get_state())
                logger.info("Starting serial processing.")
                if not skip_nodes:
                    config.set_work_type(WorkType.NODE)
                    logger.info("Starting creation of nodes.")
                    pmap = map(process_batch, Batcher(self._batch_size, self._iterator))
                    processed_batches = []
                    for batch in pmap:
                        processed_batches.append(batch)
                else:
                    logger.info("Skipping creation of nodes.")

                if not skip_relationships:
                    config.set_work_type(WorkType.RELATIONSHIP)
                    logger.info("Starting creation of relations.")
                    list(map(process_batch, processed_batches))
                else:
                    logger.info("Skipping creation of relationships.")
            finally:
                # Cleanup the process state
                cleanup_process_state()
        
        # make sure that the progress bar is updated one last time
        if pb is not None:
            config.exit_flag.set()
            pb_updater.join()
            pb.n = config.processed_resources.value
            pb.refresh()
            time.sleep(0.1)
            pb.close()
        logger.info(f"Processed in total {config.processed_nodes.value} nodes and {config.processed_relationships.value} relationships (this run took {int(time.time()-start)}s)")