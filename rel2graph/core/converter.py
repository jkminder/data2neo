#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main module that converts Relational Database entities into graphs

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
from .graph_elements import NodeMatcher, Graph
from .factories.matcher import Matcher
from .config_parser import _index_of_closing_bracket, parse
import threading
import time

from rel2graph.core import graph_elements

logger = logging.getLogger(__name__)

class WorkType(IntEnum):
    NODE = 0
    RELATION = 1

class WorkerConfig:
    def __init__(self,iterator: ResourceIterator, factories: Dict[str, Tuple["SupplyChain", "SupplyChain"]],
                work_type: WorkType, 
                graph: Graph,
                progress_bar: "tqdm.tqdm" = None) -> None:
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

class Worker(threading.Thread):
    def __init__(self, id: int, config: WorkerConfig, bucket: Queue) -> None:
        threading.Thread.__init__(self)
        self._worker_id = id
        self._config = config
        self._bucket = bucket
        self._exit_flag = False

    def process(self) -> None:
        while not self._exit_flag:
            with self._config.iterator_lock:
                next_resource = self._config.iterator.next()

            if next_resource is None:
                self._config.done = True
                return None # Processed all data -> worker is done

            logger.debug(f"Worker {self._worker_id}: Processing resource type: '{next_resource.type}'")

            with self._config.factories_lock:
                factory = self._config.factories[next_resource.type][int(self._config.work_type)]
            
            try:
                subgraph = factory.construct(next_resource)
            except Exception as err:
                logger.error(f"Encountered error when processing {'nodes' if self._config.work_type == WorkType.NODE else 'relations'} of resource type {next_resource.type}.\n{type(err)}: {err}\n")
                raise err

            with self._config.graph_lock:
                if self._config.work_type == WorkType.NODE:
                # We need to loop through all the nodes and test if they need to be 
                # merged or simply created (merging only possible if __primarykey__ is set)
                    for node in subgraph.nodes:
                        if node.__primarykey__ is not None:
                            # If a primary key is existing we merge the node to the graph
                            self._config.graph.merge(node)
                        else:
                            self._config.graph.create(node)
                else:
                    # Relationships are always merged
                    self._config.graph.merge(subgraph)
            
            with self._config.counter_lock:
                if self._config.work_type == WorkType.NODE:
                    self._config.counter += len(subgraph.nodes)
                else:
                    self._config.counter += len(subgraph.relationships)

            # Update the progress bar if enabled
            if self._config.progress_bar is not None:
                self._config.progress_bar.update(1)

    def run(self) -> None:
        logger.debug("Starting Worker " + str(self._worker_id))
        try:
            self.process()
        except Exception:
            self._bucket.put(sys.exc_info())
        logger.debug("Exiting Worker " + str(self._worker_id))
    
    def interrupt(self) -> None:
        """Stops currently running worker (Worker will finish currently running convertion)"""
        self._exit_flag = True

class WorkerPool:
    def __init__(self, num_workers: int, config: WorkerConfig) -> None:
        self._n = num_workers
        self._config = config
        self._workers = []
        self._bucket = Queue()


    def run(self):
        for i in range(self._n):
            self._workers.append(Worker(i, self._config, self._bucket))

        for worker in self._workers:
            worker.start()

        while not self._config.done:
            try:
                exc = self._bucket.get(block=False)
            except Empty:
                time.sleep(0.1)
            else:
                _, exc_obj, _ = exc
                logger.error(f"Cleaning up...")
                self.interrupt()
                self.join()
                raise exc_obj
        self.join()

    def interrupt(self):
        # send sigint
        for worker in self._workers:
            worker.interrupt()
    
    def join(self) -> None:
        # wait for join
        for worker in self._workers:
            worker.join()
        
class Converter:
    _is_instantiated = False
    _instantiation_time = None

    def __init__(self, config_filename: str, iterator: ResourceIterator, graph: Graph, num_workers: int = 20) -> None:
        if Converter._is_instantiated:
            logger.warn(f"Reinstantiating Converter, only one valid instance possible. Reinstantiation invalidates the old instance.")
        self._iterator = iterator
        self._graph = graph
        self._factories = parse(config_filename)
        self._num_workers = num_workers

        # register the node matcher
        Matcher.graph_matcher = NodeMatcher(graph)

        # Only one converter can exist, since we use global properties for the node matcher
        # TODO: fix this? Is it necessary though? Could just stay a singleton.
        Converter._is_instantiated = True
        Converter._instantiation_time = time.time()
        self._instantiation_time = Converter._instantiation_time


    @property
    def iterator(self) -> ResourceIterator:
        """Gets the resource iterator"""
        return self._iterator

    @iterator.setter
    def iterator(self, iterator: ResourceIterator) -> None:
        """Sets the resource iterator"""
        self._iterator = iterator

    def _is_valid_instance(self):
        if self._instantiation_time < Converter._instantiation_time:
            raise Exception("This converter is no longer a valid instance. Only 1 valid instance per process exists and all old instances are invalidated.")
    
    def __call__(self, progress_bar: "tdqd.tqdm" = None) -> None:
        """Runs the convertion and commits the produced nodes and relations to the graph
        
        Args:
            progress_bar: An optional tqdm instance for a progress bar
        """
        self._is_valid_instance()

        # Handle progress bar
        pb = None
        if progress_bar is not None:
            pb = progress_bar(total=2*len(self._iterator))
        
        logger.info(f"Running convertion with {self._num_workers} parallel workers.")

        start = time.time()

        # Make sure iterator is reset
        self.iterator.reset_to_first()

        n_nodes = 0
        n_relations = 0

        logger.info("Starting creation of nodes.")
        
        worker_config = WorkerConfig(self.iterator, self._factories, WorkType.NODE, self._graph, pb)  
        pool = WorkerPool(self._num_workers, worker_config)

        try:
            pool.run()
        except KeyboardInterrupt as e:
            # Cleanup
            pool.interrupt()
            pool.join()
            raise e
        n_nodes += worker_config.counter

        # Create relations
        logger.info("Starting creation of relations.")
        self.iterator.reset_to_first()

        worker_config = WorkerConfig(self.iterator, self._factories, WorkType.RELATION, self._graph, pb)
        pool = WorkerPool(self._num_workers, worker_config)

        try:
            pool.run()
        except KeyboardInterrupt as e:
            # Cleanup
            pool.interrupt()
            pool.join()
            raise e        
        n_relations += worker_config.counter

        logger.info(f"Processed {n_nodes} nodes and {n_relations} relations (took {int(time.time()-start)}s)")
