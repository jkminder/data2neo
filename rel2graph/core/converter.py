#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Main module that converts Relational Database entities into graphs

authors: Julian Minder
"""
from typing import Dict, Tuple
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
                graph: Graph) -> None:
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

class Worker(threading.Thread):
    def __init__(self, id: int, config: WorkerConfig) -> None:
        threading.Thread.__init__(self)
        self._worker_id = id
        self._config = config
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
                subgraph = factory.construct(next_resource)   # 0 for node factories
            except Exception as err:
                logger.error(f"Encountered error when processing {'nodes' if self._config.work_type == WorkType.NODE else 'relations'} of resource type {next_resource.type}.\n{type(err)}: {err}\n")
                raise err

            with self._config.graph_lock:
                if self._config.work_type == WorkType.NODE:
                    self._config.graph.create(subgraph)
                else:
                    self._config.graph.merge(subgraph)
            
            with self._config.counter_lock:
                if self._config.work_type == WorkType.NODE:
                    self._config.counter += len(subgraph.nodes)
                else:
                    self._config.counter += len(subgraph.relationships)

    def run(self) -> None:
        logger.debug("Starting Worker " + str(self._worker_id))
        self.process()
        logger.debug("Exiting Worker " + str(self._worker_id))
    
    def interrupt(self) -> None:
        """Stops currently running worker"""
        self._exit_flag = True

class WorkerPool:
    def __init__(self, num_workers: int, config: WorkerConfig) -> None:
        self._n = num_workers
        self._config = config
        self._workers = []

    def start_workers(self):
        for i in range(self._n):
            self._workers.append(Worker(i, self._config))

        for worker in self._workers:
            worker.start()
        
    def join(self) -> None:
        # send sigint
        for worker in self._workers:
            worker.interrupt()

        # wait for join
        for worker in self._workers:
            worker.join()
        
class Converter:
    is_instantiated = False

    def __init__(self, config_filename: str, iterator: ResourceIterator, graph: Graph, num_workers: int = 20) -> None:
        if Converter.is_instantiated:
            logger.warn(f"Reinstantiating Converter, only one valid instance possible. Reinstantiation invalidates the old instance.")
        self._iterator = iterator
        self._graph = graph
        self._factories = parse(config_filename)
        self._num_workers = num_workers
        self._processed_nodes = False # This is used to persist progress
        # register the node matcher
        Matcher.graph_matcher = NodeMatcher(graph)

        Converter.is_instantiated = True

    @property
    def iterator(self) -> ResourceIterator:
        """Gets the resource iterator"""
        return self._iterator

    @iterator.setter
    def iterator(self, iterator: ResourceIterator) -> None:
        """Sets the resource iterator"""
        self._iterator = iterator

    def __call__(self) -> None:
        """Runs the convertion and commits the produced nodes and relations to the graph"""
        start = time.time()

        n_nodes = 0
        n_relations = 0

        if not self._processed_nodes:
            worker_config = WorkerConfig(self.iterator, self._factories, WorkType.NODE, self._graph)
            pool = WorkerPool(self._num_workers, worker_config)

            logger.info("Starting creation of nodes.")

            pool.start_workers()
            while not worker_config.done:
                time.sleep(0.5) # wait for workers to exit
            pool.join()
            n_nodes += worker_config.counter

        # Create relations
        logger.info("Starting creation of relations.")
        self.iterator.reset_to_first()

        worker_config = WorkerConfig(self.iterator, self._factories, WorkType.RELATION, self._graph)
        pool = WorkerPool(self._num_workers, worker_config)

        pool.start_workers()
        while not worker_config.done:
            time.sleep(0.5) # wait for workers to exit
        pool.join()

        n_relations += worker_config.counter

        logger.info(f"Processed {n_nodes} nodes and {n_relations} relations (took {int(time.time()-start)}s)")
