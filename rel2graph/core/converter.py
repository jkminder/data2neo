#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main module that converts Relational Database entities into graphs.

authors: Julian Minder
"""
from sre_constants import SRE_FLAG_UNICODE
from typing import Dict, Tuple
from queue import Queue, Empty
import sys
import logging
import threading
from enum import IntEnum
import time

from .factories import resource

from .resource_iterator import ResourceIterator
from .factories.registrar import register_factory
from .graph_elements import NodeMatcher, Graph, Subgraph
from .factories.matcher import Matcher
from .config_parser import parse
import threading
import time

from rel2graph.core import graph_elements

logger = logging.getLogger(__name__)

class WorkType(IntEnum):
    NODE = 0
    RELATION = 1

class WorkerConfig:
    """Config container including information, data and configuration of the convertion that has to be done.
    
    Attributes:
        iterator:  A resource iterator containing the resource that need to be converted
        iterator_lock: A lock for access to the iterator
        done: A bool signifying where the conversion is done
        factories: The factories that are used to convert the resources. A dictionary of <resource type, tuple<node factories, relation factoires>>
        factories_lock: A lock for access to the factories
        work_type: An enum specifying whether nodes or relations should be processed
        graph: The graph object
        graph_lock: The lock for access to the graph
        counter: Amount of processed objects
        counter_lock: A lock for access to the counter
        progress_bar: An optional progress bar
    """

    def __init__(self, iterator: ResourceIterator, 
                factories: Dict[str, Tuple["SupplyChain", "SupplyChain"]],
                work_type: WorkType, 
                graph: Graph,
                progress_bar: "tqdm.tqdm" = None) -> None:
        """Initialises a Worker config with the required data.
        
        Args:
            iterator:  A resource iterator containing the resource that need to be converted
            factories: The factories that are used to convert the resources. A dictionary of <resource type, tuple<node factories, relation factoires>>
            work_type: An enum specifying whether nodes or relations should be processed
            graph: The graph object, where the converted data is commited to
            progress_bar: An optional progress bar object, that is updated for each completed conversion of a resource with progress_bar.update(1).
                Suggested usage is with the tqdm module.
            n_resources: A counter for all processed resources
        """
        self.iterator = iterator
        self.done = False
        self.iterator_lock = threading.Lock()
        
        self.factories = factories
        self.factories_lock = threading.Lock()
        self.work_type = work_type

        self.graph = graph
        self.graph_lock = threading.Lock()

        self.counter = 0
        self.counter_lock = threading.Lock()

        self.progress_bar = progress_bar
        self.n_resources = 0

class Worker(threading.Thread):
    """The Worker does the main conversion. It is build to be parallelised."""

    def __init__(self, id: int, config: WorkerConfig, bucket: Queue) -> None:
        """Initialises a Worker.
        
        Args:
            id: The worker id. Is only used for logging.
            config: The worker config, specifying all details of the work
            bucket: An exception queue. The worker puts any exception happening during execution in this queue.
        """
        threading.Thread.__init__(self, name=f"Worker-{id}")
        self._worker_id = id
        self._config = config
        self._bucket = bucket
        self._exit_flag = False

        # The work item is used to remember on what resource the worker is working on. 
        # If an exception is raised during its processing and the cause of the exception is external (network or similar)
        # the user can restart the converter and the worker will redo the work on work_item -> no element is lost
        self._work_item = None

    def process(self) -> None:
        """
        Main conversion processing. While the worker is not notified to stop (with the exit flag), 
        it repeatedly askes the iterator for a next resource to process. If the iterator reports that the
        data is traversed fully, the method returns.
        """
        while not self._exit_flag:
            if self._work_item is not None:
                # We first test if the worker still has a work item (happens if exception gets raised during execution)
                next_resource = self._work_item
            else:
                with self._config.iterator_lock:
                    next_resource = self._config.iterator.next()
                    self._work_item = next_resource

            if next_resource is None:
                self._config.done = True
                return # Processed all data -> worker is done

            logger.debug(f"Processing {next_resource}")

            with self._config.factories_lock:
                factory = self._config.factories[next_resource.type][int(self._config.work_type)]
            
            try:
                subgraph = factory.construct(next_resource)
            except Exception as err:
                raise type(err)(f"Encountered error when processing {'nodes' if self._config.work_type == WorkType.NODE else 'relations'} of {next_resource}.\n{type(err)}: {err}\n")

            # We sort the subgraph based on if its parts should be 
            # merged or just created. This is selected based on if the
            # __primarykey__ property is set. Note that for relations 
            # this only matters if you use the GraphWithParallelRelations from
            # rel2graph.py2neo_extensions (otherwise relations are always merged)
            to_merge = Subgraph()
            to_create = Subgraph()
            for node in subgraph.nodes:
                if node.__primarykey__ is not None:
                    # If a primary key is existing we merge the node to the graph
                    to_merge |= node
                else:
                    to_create |= node
            for relation in subgraph.relationships:
                if getattr(relation, "__primarykey__", None) is not None:
                    # If a primary key is existing we merge the relation to the graph
                    to_merge |= relation
                else:
                    to_create |= relation

            # Creating does not rely on synchronous executions
            self._config.graph.create(to_create)
                
            # Merging nodes requires serialization (synchronous executions)
            # Using locks to enforce this
            with self._config.graph_lock:
                self._config.graph.merge(to_merge)

            self._work_item = None # Remove work item

            with self._config.counter_lock:
                self._config.n_resources += 1
                if self._config.work_type == WorkType.NODE:
                    self._config.counter += len(subgraph.nodes)
                else:
                    self._config.counter += len(subgraph.relationships)

            # Update the progress bar if enabled
            if self._config.progress_bar is not None:
                self._config.progress_bar.update(1)

    def run(self) -> None:
        """
        Logs the start and end of the worker process. If an exception happens during processing
        it is catched and put in the exception bucket.
        """
        logger.debug(f"Starting Worker " + str(self._worker_id))
        try:
            self.process()
        except Exception as err:
            logger.error(str(err))
            self._bucket.put(sys.exc_info())
        logger.debug(f"Exiting Worker " + str(self._worker_id))
    
    def interrupt(self) -> None:
        """Stops currently running worker (Worker will finish currently running convertion)"""
        self._exit_flag = True

    def reinstantiate(self, bucket: Queue) -> None:
        """Makes the worker ready to restart work.
        
        Args:
            bucket: An exception queue. The worker puts any exception happening during execution in this queue.
        """
        threading.Thread.__init__(self, name=self.name)
        self._exit_flag = False
        self._bucket = bucket
    
class WorkerPool:
    """The worker pool manages a pool of workers and abstracts any interaction with them."""

    def __init__(self, num_workers: int, config: WorkerConfig) -> None:
        """Initialises a worker pool. 
        
        Args:
            num_workers: Number of workers in the pool.
            config: The worker config. Is passed to all workers.
        """
        self._n = num_workers
        self._config = config
        self._workers = []
        self._bucket = Queue()

    @property
    def config(self) -> WorkerConfig:
        """Returns the pool config"""
        return self._config

    def run(self) -> None:
        """
        Creates the workers, starts them and waits for all of them to finish. If one
        of the workers reports an exception, it interrupts all other workers and once all have
        returns it propagates the exception further up.
        """
        if len(self._workers) == 0:
            for i in range(self._n):
                self._workers.append(Worker(i, self._config, self._bucket))

        for worker in self._workers:
            worker.start()

        # main loop 
        while True:
            if self._config.done:
                # wait for all workers to finish
                self.join()
                # We still need to check if any of the workers reported an exception
            # Check if an exception has been raised
            try:
                exc = self._bucket.get(block=False)
            except Empty:
                if self._config.done:
                    return # all workers done and no exception reported
                time.sleep(0.00001)
                continue
            else:
                # Exception has been raised -> clean up workers
                _, exc_obj, _ = exc
                logger.error(f"Cleaning up...")
                self.interrupt()
                self.join()
                raise exc_obj


    def interrupt(self) -> None:
        """Sends iterruption signal to all workers."""
        for worker in self._workers:
            worker.interrupt()
    
    def reinstantiate(self) -> None:
        """Makes the workers ready to restart work"""
        # Make sure the exception bucket is emtpy
        self._bucket = Queue()
        for worker in self._workers:
            worker.reinstantiate(bucket=self._bucket)

    def join(self) -> None:
        """Waits for all workers to finish."""
        # wait for join
        for worker in self._workers:
            worker.join()
        
class Converter:
    """The converter handles the whole conversion pipeline.
    
    Args:
        no_reinstantiation_warnings: Turns of singleton reinstantiation warnings (Default: False)
    """
    _is_instantiated = False
    _instantiation_time = None
    no_instantiation_warnings = False

    def __init__(self, config_filename: str, iterator: ResourceIterator, graph: Graph, num_workers: int = 1) -> None:
        """Initialises a converter. Note that this is a singleton and only the most recent instantiation is valid.
        
        Args:
            config_filename: Path of the schema config file.
            iterator: The resource iterator.
            graph: The neo4j graph (from py2neo)
            num_workers: The number of parallel workers. Please make sure that your usage supports parallelism. To use serial processing set this to 1. (default: 20)
        """
        if Converter._is_instantiated and not Converter.no_instantiation_warnings:
            logger.warn(f"Reinstantiating Converter, only one valid instance possible. Reinstantiation invalidates the old instance.")
        self._iterator = iterator
        self._graph = graph
        self._num_workers = num_workers

        # Parse the schema and compile it into factories
        self._factories = parse(config_filename)

        # register the node matcher -> TODO: maybe find a better solution than global variable. Problem is inclusion in compiler. Would also fix the singleton issue.
        Matcher.graph_matcher = NodeMatcher(graph)

        # Only one converter can exist, since we use global properties for the node matcher
        # TODO: fix this? Is it necessary though? Could just stay a singleton.
        Converter._is_instantiated = True
        Converter._instantiation_time = time.time()
        self._instantiation_time = Converter._instantiation_time

        # State information
        self._processed_nodes = False
        self._processed_relations = False

        # Progress counters
        self._n_nodes = 0
        self._n_relations = 0

        # Worker pool
        self._worker_pool = None

    @property
    def iterator(self) -> ResourceIterator:
        """Gets the resource iterator"""
        return self._iterator

    @iterator.setter
    def iterator(self, iterator: ResourceIterator) -> None:
        """Sets the resource iterator"""
        self._iterator = iterator

        # Update state
        self._processed_nodes = False
        self._processed_relations = False

    def reload_config(self, config_filename: str) -> None:
        """Can be used to reload the schema configuration, without changing the state
        of the Converter.
        
        Args:
            config_filename: Path of the schema config file.
        """
        self._factories = parse(config_filename)
        if self._worker_pool is not None:
            self._worker_pool.config.factories = self._factories
        
    def _is_valid_instance(self) -> None:
        """Tests if a newer instance of the converter exists.
        
        Raises:
            Exception: If a newer converter exists.
        """
        if self._instantiation_time < Converter._instantiation_time:
            raise Exception("This converter is no longer a valid instance. Only 1 valid instance per process exists and all old instances are invalidated.")
    
    def _setup_worker_pool(self, type, pb = None) -> None:
        if self._worker_pool is None:
                config = WorkerConfig(self.iterator, self._factories, type,  self._graph, pb)  
                self._worker_pool = WorkerPool(self._num_workers, config)
        else:
            logger.info("Continuing previous work...")
            self._worker_pool.reinstantiate()
    
    def __call__(self, progress_bar: "tdqm.tqdm" = None, skip_nodes = False, skip_relations = False) -> None:
        """Runs the convertion and commits the produced nodes and relations to the graph.
        
        Args:
            progress_bar: An optional tqdm like instance for a progress bar. 
            skip_nodes: (default: False) If true creation of nodes will be skiped. ATTENTION: this might lead to problems if you use identifiers.
            skip_relation: If true creation of relations will be skiped (default: False)
        """
        # Test whether converter instance is still valid
        self._is_valid_instance()

        # Handle progress bar (create new or update it)
        pb = None
        if progress_bar is not None:
            pb = progress_bar(total=2*len(self._iterator))
            if self._processed_nodes:
                pb.update(len(self._iterator))
            if self._worker_pool is not None:
                self._worker_pool.config.progress_bar = pb
                self._worker_pool.config.progress_bar.update(self._worker_pool.config.n_resources)
        else:
            if self._worker_pool is not None:
                self._worker_pool.config.progress_bar = None
            
        logger.info(f"Running convertion with {self._num_workers} parallel workers.")

        start = time.time()

        if not skip_nodes and not self._processed_nodes:
            self._setup_worker_pool(WorkType.NODE, pb)
                
            logger.info("Starting creation of nodes.")

            try:
                self._worker_pool.run()
            except KeyboardInterrupt as e:
                # KeyboardInterrupt is raised on the main exec thread
                # -> Cleanup all workers
                self._worker_pool.interrupt()
                self._worker_pool.join()
                raise e
            self._n_nodes += self._worker_pool.config.counter

            # Clean up after nodes creation
            self.iterator.reset_to_first()
            self._processed_nodes = True # update state
            self._worker_pool = None 
        else:
            logger.info("Skipping creation of nodes.")

        # Update total of progress bar
        if pb is not None:
            pb.total = 2*len(self._iterator)
            
        if not self._processed_relations and not skip_relations:    
            # Create relations
            logger.info("Starting creation of relations.")

            self._setup_worker_pool(WorkType.RELATION, pb)

            try:
                self._worker_pool.run()
            except KeyboardInterrupt as e:
                # KeyboardInterrupt is raised on the main exec thread
                # -> Cleanup all workers
                self._worker_pool.interrupt()
                self._worker_pool.join()
                raise e
            self._n_relations += self._worker_pool.config.counter

            # Update state
            self._processed_relations = True
        else:
            logger.info("Skipping creation of relations.")

        logger.info(f"Processed in total {self._n_nodes} nodes and {self._n_relations} relations (this run took {int(time.time()-start)}s)")
