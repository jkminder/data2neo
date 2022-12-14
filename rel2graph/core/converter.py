#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main module that converts Relational Database entities into graphs.

authors: Julian Minder
"""
from re import T
from sre_constants import SRE_FLAG_UNICODE
from typing import Dict, Tuple
from queue import Queue, Empty
import sys
import logging
import threading
import ctypes
from enum import IntEnum
import time
import os

from .factories import Resource
from .resource_iterator import ResourceIterator
from .factories.registrar import register_factory
from .graph_elements import NodeMatcher, Graph, Subgraph
from .factories.matcher import Matcher
from .schema_compiler import compile_schema
import threading
import time
import random

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
        mask: A set of all entities that are required to be shipped to the factories.
        factories_lock: A lock for access to the factories
        work_type: An enum specifying whether nodes or relations should be processed
        buffer_size: Number of resources that are processed until the result is committed to the graph
        graph: The graph object
        graph_lock: The lock for access to the graph
        counter: Amount of processed objects
        counter_lock: A lock for access to the counter
        progress_bar: An optional progress bar
        n_resources: Number of resources that have been processed
    """

    def __init__(self, iterator: ResourceIterator, 
                factories: Dict[str, Tuple["SupplyChain", "SupplyChain"]],
                node_mask: set, relation_mask: set,
                work_type: WorkType, 
                graph: Graph,
                buffer_size: int,
                progress_bar: "tqdm.tqdm" = None) -> None:
        """Initialises a Worker config with the required data.
        
        Args:
            iterator:  A resource iterator containing the resource that need to be converted
            factories: The factories that are used to convert the resources. A dictionary of <resource type, tuple<node factories, relation factoires>>
            node_mask: A set of all entities that produce a node.
            relation_mask: A set of all entities that produce a relation.
            work_type: An enum specifying whether nodes or relations should be processed
            graph: The graph object, where the converted data is commited to
            buffer_size: Number of resources that are processed until the result is committed to the graph
            progress_bar: An optional progress bar object, that is updated for each completed conversion of a resource with progress_bar.update(1).
                Suggested usage is with the tqdm module.
        """
        self.iterator = iterator
        self.done = False
        self.iterator_lock = threading.Lock()
        
        self.factories = factories
        self.factories_lock = threading.Lock()
        self.work_type = work_type
        if work_type == WorkType.NODE:
            self.mask = node_mask
        else:
            self.mask = relation_mask

        self.graph = graph
        self.graph_lock = threading.Lock()

        self.counter = 0
        self.counter_lock = threading.Lock()

        self.progress_bar = progress_bar
        self.n_resources = 0

        self.buffer_size = buffer_size 

class DynamicBufferMonitor(threading.Thread):
    """Adapts the buffer size based on average time per resource."""
    def __init__(self, config: WorkerConfig, update_interval: int = 60) -> None:
        """Initialises the buffer with the config.
        
        Args:
            config: The config of the worker
            update_interval: The interval in seconds between two updates of the buffer size.
        """
        super().__init__()

        self._config = config
        self._update_interval = update_interval
        self._stop_event = threading.Event()

    def terminate(self) -> None:
        """Terminate the thread."""
        self._stop_event.set()
    
    def take_measurement(self):
        """Takes a time measurement of the current buffer size performance."""
        diff = 0
        while (diff == 0):
            count_start = self._config.n_resources
            start_t = time.time()
            self._stop_event.wait(self._update_interval/3) # we need to take 3 measurements
            if self._stop_event.is_set():
                raise Exception("Stopped")
            count_end = self._config.n_resources
            diff = count_end - count_start
        return (time.time() - start_t) / float(diff)

    def run(self) -> None:
        """Dynamically adapts the buffer size based on the performance of the conversion."""
        initial = True
        def bound(x):
            x = round(x/10)*10 # round to nearest 10
            res = 1 if x < 10 else min(x, 1000)
            return res
        def get_adaptive_step_size(current_buffer_size, initial):
            # If its the initial step, we first try large steps
            # The step size is inversely proportional to the buffer size and bounded by 1 and 1000
            # To escape local minima with small bs it is randomly set to 10 for probability 0.2
            if initial:
                return 90
            base = 10
            if current_buffer_size > 100:
                base = 20
            elif current_buffer_size > 200:
                base = 50
            elif current_buffer_size > 500:
                base = 100
            
            # for 20% of the time we set the step size to a random number to escape local minima
            # if the buffer is very small, we set the step size to 100 to test large buffers 
            # if the buffer is large we set the step size such that the buffer size is 10
            return current_buffer_size - 10 if random.random() > 0.2 and current_buffer_size > 30 else base 
        
        while(not self._stop_event.is_set()):
            higher_avg, lower_avg = 99999, 99999 # init with high values
            current_size = self._config.buffer_size
            step_size = get_adaptive_step_size(self._config.buffer_size, initial)
            initial = False
            try:
                current_avg = self.take_measurement()
                if current_size < 1000:
                    self._config.buffer_size = bound(current_size + step_size)
                    higher_avg = self.take_measurement()
                if current_size > 1:
                    self._config.buffer_size = bound(current_size - step_size)
                    lower_avg = self.take_measurement()
                self._config.buffer_size = current_size # back to original value
            except Exception:
                break # if the thread is terminated
            time_list = [current_avg, higher_avg, lower_avg]
            imin = time_list.index(min(time_list))
            if imin == 1:
                self._config.buffer_size = bound(self._config.buffer_size + step_size)
                logger.debug(f"Buffer size increased from {current_size} to {self._config.buffer_size}")
            elif imin == 2:
                self._config.buffer_size = bound(self._config.buffer_size - step_size)
                logger.debug(f"Buffer size decreased from {current_size} to {self._config.buffer_size}")
            self._stop_event.wait(10) # run 10s in current configuration
        
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

        # Init the subgraph buffers
        self.to_merge = Subgraph()
        self.to_create = Subgraph()
        self._r_count = 0 # counter for the not reported processed resources


    @staticmethod
    def _commit_wrap(function):
        """Wraps the graph commit function into try except block with retry. """
        try:
            function()
        except Exception as e:
            logger.error(f"Py2neo Exception '{type(e).__name__}': " + str(e))
            logger.error("Sleeping for 10 second and retrying commit")
            time.sleep(10)
            function()
        
    def commit_batch(self):
        """"Commits processed batch to graph."""
        nodes_committed = 0 
        relations_committed = 0
        
        # Creating does not rely on synchronous executions
        if len(self.to_create.nodes) + len(self.to_create.relationships) > 0:
            with self._config.graph_lock:
                Worker._commit_wrap(lambda: self._config.graph.create(self.to_create))
            nodes_committed += len(self.to_create.nodes)
            relations_committed += len(self.to_create.relationships)
            
        # Merging nodes requires serialization (synchronous executions) between threads
        # Using locks to enforce this
        if len(self.to_merge.nodes) + len(self.to_merge.relationships) > 0:
            with self._config.graph_lock:
                Worker._commit_wrap(lambda: self._config.graph.merge(self.to_merge))
            nodes_committed += len(self.to_merge.nodes)
            relations_committed += len(self.to_merge.relationships)

        if nodes_committed > 0 or relations_committed > 0:
            with self._config.counter_lock:
                self._config.n_resources += self._r_count
                self._r_count = 0
                if self._config.work_type == WorkType.NODE:
                    self._config.counter += nodes_committed
                else:
                    self._config.counter += relations_committed

        self.to_merge = Subgraph()
        self.to_create = Subgraph()

    def process(self) -> None:
        """
        Main conversion processing. While the worker is not notified to stop (with the exit flag), 
        it repeatedly askes the iterator for a next resource to process. If the iterator reports that the
        data is traversed fully, the method returns.
        """
        buffer_size = 0
        while not self._exit_flag:
            if buffer_size % self._config.buffer_size == 0:
                self.commit_batch()
                buffer_size = 0

            if self._work_item is not None:
                # We first test if the worker still has a work item (happens if exception gets raised during execution)
                next_resource = self._work_item
            else:
                with self._config.iterator_lock:
                    try:
                        next_resource = next(self._config.iterator)
                    except StopIteration:
                        # If the iterator is exhausted, we return
                        self._config.done = True
                        break # Processed all data -> worker is done
                    self._work_item = next_resource

            if next_resource.type in self._config.mask:
                # If the resource is not in the mask (meaning we don't need to convert it), we skip it
                # This is mainly done for performance reasons, as the conversion is not needed
                
                logger.debug(f"Processing {next_resource}")

                with self._config.factories_lock:
                    factory = self._config.factories[next_resource.type][int(self._config.work_type)]
                
                try:
                    subgraph = factory.construct(next_resource)
                except Exception as err:
                    err.args += (f"Encountered error when processing {'nodes' if self._config.work_type == WorkType.NODE else 'relations'} of {next_resource}.",)
                    raise err

                # We sort the subgraph based on if its parts should be 
                # merged or just created. This is selected based on if the
                # __primarykey__ property is set. Note that for relations 
                # this only matters if you use the GraphWithParallelRelations from
                # rel2graph.py2neo_extensions (otherwise relations are always merged)

                for node in subgraph.nodes:
                    if node.__primarykey__ is not None:
                        # If a primary key is existing we merge the node to the graph
                        self.to_merge |= node
                    else:
                        self.to_create |= node
                for relation in subgraph.relationships:
                    if getattr(relation, "__primarykey__", None) is not None:
                        # If a primary key is existing we merge the relation to the graph
                        self.to_merge |= relation
                    else:
                        relation.__primarykey__ = -1
                        self.to_create |= relation

                buffer_size += 1
            
            self._work_item = None # Remove work item

            # Update the progress bar if enabled
            if self._config.progress_bar is not None:
                self._config.progress_bar.update(1)
            self._r_count += 1 # Increment resource count -> needed for buffer size adjustment

        # Make sure all remaining data is committed
        self.commit_batch()

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

    def __init__(self, num_workers: int, config: WorkerConfig, use_dynamic_buffer: bool) -> None:
        """Initialises a worker pool. 
        
        Args:
            num_workers: Number of workers in the pool.
            config: The worker config. Is passed to all workers.
            use_dynamic_buffer: If true, the buffer size is dynamically adjusted based on the resource throughput.
        """
        self._n = num_workers
        self._config = config
        self._workers = []
        self._bucket = Queue()
        self._use_dynamic_buffer = use_dynamic_buffer

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

        if self._use_dynamic_buffer:
            self._dynamic_buffer_monitor = DynamicBufferMonitor(self._config, update_interval=30)
            self._dynamic_buffer_monitor.start()

        # main loop 
        while True:
            if self._config.done:
                # wait for all workers to finish
                if self._use_dynamic_buffer:
                    self._dynamic_buffer_monitor.terminate()
                self.join()

                # We still need to check if any of the workers reported an exception
            # Check if an exception has been raised
            try:
                exc = self._bucket.get(block=False)
            except Empty:
                if self._config.done:
                    if self._use_dynamic_buffer:
                        self._dynamic_buffer_monitor.terminate()
                    return # all workers done and no exception reported
                time.sleep(0.5) # Check again in 0.5 seconds
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
        if self._use_dynamic_buffer:
            self._dynamic_buffer_monitor.terminate()
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
        if self._use_dynamic_buffer:
            self._dynamic_buffer_monitor.join()

        
class Converter:
    """The converter handles the whole conversion pipeline.

    Attributes:
        no_reinstantiation_warnings: Turns of singleton reinstantiation warnings (Default: False)
    """
    _is_instantiated = False
    _instantiation_time = None
    no_instantiation_warnings = False

    def __init__(self, schema: str, iterator: ResourceIterator, graph: Graph, num_workers: int = 1, serialize: bool = False) -> None:
        """Initialises a converter. Note that this is a singleton and only the most recent instantiation is valid.
        
        Args:
            schema: The schema to convert.
            iterator: The resource iterator.
            graph: The neo4j graph (from py2neo)
            num_workers: The number of parallel workers. Please make sure that your usage supports parallelism. To use serial processing set this to 1. (default: 1)
            serialize: If true, the converter will make sure that all resources are processed serially and does not use any buffering. This is useful if you want to make sure that all resources are processed 
                and committed to the graph in the same order as they are returned by the iterator. Note that you can't set both serialize to true and set num_workers > 1. (default: False)
        """

        if serialize and num_workers > 1:
            raise ValueError("You can't use serialization and parallel processing (num_workers > 1) at the same time.")


        if Converter._is_instantiated and not Converter.no_instantiation_warnings:
            logger.warn(f"Reinstantiating Converter, only one valid instance possible. Reinstantiation invalidates the old instance.")
        self.iterator = iterator
        self._graph = graph
        self._num_workers = num_workers

        # Parse the schema and compile it into factories
        self.reload_schema(schema)

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

        # Serialization
        self._serialize = serialize

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

    def reload_schema(self, schema: str) -> None:
        """Can be used to reload the schema configuration, without changing the state
        of the Converter.
        
        Args:
            schema: The schema to convert.
        """
        if os.path.isfile(schema):
            raise DeprecationWarning("Please supply the schema as a string instead of a file. Use 'Converter(schema=load_file(filename)...' instead. \
                The 'load_file' function is provided under rel2graph.utils. This warning will be removed in a future version.")
        self._factories, self._node_mask, self._relation_mask = compile_schema(schema)
        if "_worker_pool" in dir(self) and self._worker_pool is not None:
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
                instantiated_iterator = iter(self.iterator)
                config = WorkerConfig(instantiated_iterator, self._factories, self._node_mask, self._relation_mask, type, self._graph, 1 if self._serialize else 100, pb)
                self._worker_pool = WorkerPool(self._num_workers, config, use_dynamic_buffer=not self._serialize)
        else:
            logger.info("Continuing previous work...")
            self._worker_pool.reinstantiate()
    
    def _set_relation_wait_function(self, relations_wait_function):
        self._relations_wait_function = relations_wait_function

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
            self._processed_nodes = True # update state
            self._worker_pool = None 
        else:
            logger.info("Skipping creation of nodes.")
    
        # Update total of progress bar
        if pb is not None:
            pb.total = 2*len(self._iterator)

        if self._relations_wait_function is not None:
            self._relations_wait_function()

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
        logger.info(f"Processed in total {self._n_nodes} nodes and {self._n_relations} relations (this run took {int(time.time()-start)}s)")