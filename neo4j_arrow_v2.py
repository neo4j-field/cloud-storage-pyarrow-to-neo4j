from typing import Any, Dict, Iterable, Union, Tuple
from enum import Enum
import json, os, sys

import pyarrow as pa
import pyarrow.flight as flight


class ClientState(Enum):
    READY = "ready"
    FEEDING_NODES = "feeding_nodes"
    FEEDING_EDGES = "feeding_edges"
    AWAITING_GRAPH = "awaiting_graph"
    GRAPH_READY = "done"


class Neo4jArrowClient():
    def __init__(self, host: str, *, port: int=8491, user: str = "neo4j",
                 password: str = "!twittertest", tls: bool = False,
                 concurrency: int = 4, database: str = "users200m"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.tls = tls
        self.client: flight.FlightClient = None
        self.call_opts = None
        self.database = database
        self.concurrency = concurrency
        self.state = ClientState.READY

    def __str__(self):
        return f"Neo4jArrowClient{{{self.user}@{self.host}:{self.port}/{self.database}}}"

    def __getstate__(self):
        state = self.__dict__.copy()
        # Remove the FlightClient and CallOpts as they're not serializable
        if "client" in state:
            del state["client"]
        if "call_opts" in state:
            del state["call_opts"]
        return state

    def copy(self):
        client = Neo4jArrowClient(self.host, port=self.port, user=self.user,
                                  password=self.password,
                                  tls=self.tls, concurrency=self.concurrency,
                                  database=self.database)
        client.state = self.state
        return client

    def _client(self):
        """Lazy client construction to help pickle this class."""
        if not hasattr(self, "client") or not self.client:
            self.call_opts = None
            if self.tls:
                location = flight.Location.for_grpc_tls(self.host, self.port)
            else:
                location = flight.Location.for_grpc_tcp(self.host, self.port)
            client = flight.FlightClient(location)
            if self.user and self.password:
                (header, token) = client.authenticate_basic_token(self.user, self.password)
                if header:
                    self.call_opts = flight.FlightCallOptions(timeout=None, headers=[(header, token)])
            self.client = client
        return self.client
        
    def _send_action(self, action: str, body: Dict[str, Any]) -> dict:
        """
        Communicates an Arrow Action message to the GDS Arrow Service.
        """
        client = self._client()
        try:
            payload = json.dumps(body).encode("utf-8")
            result = client.do_action(
                flight.Action(action, payload),
                options=self.call_opts
            )
            return json.loads(next(result).body.to_pybytes().decode())
        except Exception as e:
            print(f"send_action error: {e}")
            #return None
            raise e


    def _write_table(self, desc: bytes, table: pa.Table, mappingfn = None) -> Tuple[int, int]:
        """
        Write a PyArrow Table to the GDS Flight service.
        """
        client = self._client()
        fn = mappingfn or self._nop
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        
        writer, _ = client.do_put(upload_descriptor, table.schema, options=self.call_opts)
        with writer:
            try:
                writer.write_table(table)
                return table.num_rows, table.get_total_buffer_size()
            except Exception as e:
                print(f"_write_table error: {e}")
        return 0, 0

    @classmethod
    def _nop(*args, **kwargs):
        pass

    def _write_batches(self, desc: bytes, batches, mappingfn = None) -> Tuple[int, int]:
        """
        Write PyArrow RecordBatches to the GDS Flight service.
        """
        batches = iter(batches)
        fn = mappingfn or self._nop

        first = fn(next(batches, None))
        if not first:
            raise Exception("empty iterable of record batches provided")
        
        client = self._client()
        upload_descriptor = flight.FlightDescriptor.for_command(
            json.dumps(desc).encode("utf-8")
        )
        rows, nbytes = 0, 0
        writer, reader = client.do_put(upload_descriptor, first.schema, options=self.call_opts)
        with writer:
            try:
                writer.write_batch(first)
                rows += first.num_rows
                nbytes += first.get_total_buffer_size()
                for remaining in batches:
                    writer.write_batch(fn(remaining))
                    rows += remaining.num_rows
                    nbytes += remaining.get_total_buffer_size()
            except Exception as e:
                print(f"_write_batches error: {e}")
        return rows, nbytes

    def create_database(self, action: str = "CREATE_DATABASE", config: Dict[str, Any] = {}) -> Dict[str, Any]:
        assert self.state == ClientState.READY
        if not config:
            config = {
                "name": self.database, 
                "concurrency": self.concurrency,
                "high_io": True,
                "force": True,
                "record_format": "aligned",
                "id_property": "id",
                "id_type": "STRING"
            }
        result = self._send_action(action, config)
        if result:
            self.state = ClientState.FEEDING_NODES
        return result

    def write_nodes(self, nodes: Union[pa.Table, Iterable[pa.RecordBatch]], mappingfn = None) -> Tuple[int, int]:
        assert self.state == ClientState.FEEDING_NODES
        desc = { "name": self.database, "entity_type": "node" }
        
        if isinstance(nodes, pa.Table):
            return self._write_table(desc, nodes, mappingfn)
        return self._write_batches(desc, nodes, mappingfn)

    def nodes_done(self) -> Dict[str, Any]:
        assert self.state == ClientState.FEEDING_NODES
        result = self._send_action("NODE_LOAD_DONE", { "name": self.database })
        if result:
            self.state = ClientState.FEEDING_EDGES
        return result

    def write_edges(self, edges: Union[pa.Table, Iterable[pa.RecordBatch]], mappingfn = None) -> Tuple[int, int]:
        assert self.state == ClientState.FEEDING_EDGES
        
        desc = { "name": self.database, "entity_type": "relationship" }

        
        if isinstance(edges, pa.Table):
            return self._write_table(desc, edges, mappingfn)
        
        return self._write_batches(desc, edges, mappingfn)

    def edges_done(self) -> Dict[str, Any]:
        assert self.state == ClientState.FEEDING_EDGES
        result = self._send_action("RELATIONSHIP_LOAD_DONE",
                                   { "name": self.database })
        if result:
            self.state = ClientState.AWAITING_GRAPH
        return result

    def wait(timeout: int = 0):
        """wait for completion"""
        assert self.state == ClientState.AWAITING_GRAPH
        self.state = ClientState.AWAITING_GRAPH
        pass
