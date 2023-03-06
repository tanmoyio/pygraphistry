import os
import logging
import requests
import typing
import pandas as pd
import cudf
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings()
requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

import mrc
import mrc.core.operators as ops

from morpheus.config import Config
from morpheus.pipeline import LinearPipeline
from morpheus.utils.logger import configure_logging

### stages
from morpheus.stages.input.file_source_stage import FileSourceStage

from morpheus.pipeline.single_port_stage import SinglePortStage
from morpheus.pipeline.single_output_source import SingleOutputSource
from morpheus.messages.message_meta import MessageMeta


class CsvSourceStage(SingleOutputSource):
    def __init__(self, c: Config, filename):
        super().__init__(c)
        self.filename = filename

    @property
    def name(self) -> str:
        return "from-csv"

    def supports_cpp_node(self):
        return False

    def _build_source(self, builder: mrc.Builder):
        out_stream = builder.make_source(self.unique_name, self._generate_frames())
        out_type = MessageMeta
        return out_stream, out_type

    def _generate_frames(self):
        yield MessageMeta(pd.read_csv(self.filename))


class TransformStage(SinglePortStage):
    def __init__(self, c, f):
        super().__init__(c)
        self.f = f
    
    @property
    def name(self):
        return "transform"
    
    def supports_cpp_node(self):
        return False

    def accepted_types(self):
        return (typing.Any, )

    def on_data(self, message):
        try:
            message = message.df.to_pandas()
        except:
            message = message.df

        message = self.f(message)
        return MessageMeta(message)

    def _build_single(self, builder, input_stream):
        node = builder.make_node(self.unique_name, self.on_data)
        builder.make_edge(input_stream[0], node)
        return node, input_stream[1]


class WriteToCsvStage(SinglePortStage):
    def __init__(self, c, filename):
        super().__init__(c)
        self.filename = filename
    
    @property
    def name(self):
        return "write-csv"
    
    def supports_cpp_node(self):
        return False

    def accepted_types(self):
        return (typing.Any, )

    def on_data(self, message):
        message.df.to_csv(self.filename, index=False)
        return message

    def _build_single(self, builder, input_stream):
        node = builder.make_node(self.unique_name, self.on_data)
        builder.make_edge(input_stream[0], node)
        return node, input_stream[1]


class WriteToParquetStage(SinglePortStage):
    def __init__(self, c, filename):
        super().__init__(c)
        self.filename = filename
    
    @property
    def name(self):
        return "write-parquet"
    
    def supports_cpp_node(self):
        return False

    def accepted_types(self):
        return (typing.Any, )

    def on_data(self, message):
        message.df.to_parquet(self.filename)
        return message

    def _build_single(self, builder, input_stream):
        node = builder.make_node(self.unique_name, self.on_data)
        builder.make_edge(input_stream[0], node)
        return node, input_stream[1]


class WriteToSplunkStage(SinglePortStage):
    def __init__(self, c, host, port, auth_token):
        super().__init__(c)
        if 'http' not in host:
            host = 'https://' + host

        self.host = host
        self.host = self.host + ':' + str(port) + "/services/collector"

        self.auth_token = "Splunk " + auth_token

        self.headers = {
            "Authorization":self.auth_token,
        }

    @property
    def name(self) -> str:
        return "write-splunk"

    def accepted_types(self) -> typing.Tuple:
        return (MessageMeta, )

    def supports_cpp_node(self):
        return False

    def _build_single(self, builder: mrc.Builder, input_stream):
        stream = input_stream[0]

        def node_fn(obs: mrc.Observable, sub: mrc.Subscriber):

            def write_to_splunk(x: MessageMeta) -> MessageMeta:
                df = x.df
                df = df.to_dict('records')
                data = []

                if 'event' not in df[0].keys():
                    for r in df:
                        data.append({'event':r})

                data = ''.join([str(r) for r in data]).replace("'", '"')
                requests.post(self.host, headers=self.headers, data=data, verify=False)
                return x

            obs.pipe(ops.map(write_to_splunk)).subscribe(sub)
        
        to_splunk = builder.make_node_full(self.unique_name, node_fn)
        builder.make_edge(stream, to_splunk)
        stream = to_splunk
        return stream, input_stream[1]
                       

class Pipeline:
    STAGES = {
        'source-json': FileSourceStage,
        'source-csv': CsvSourceStage,
        'transform': TransformStage,
        'write-splunk': WriteToSplunkStage,
        'write-csv': WriteToCsvStage,
        'write-parquet': WriteToParquetStage,
    }
    def __init__(self, debug=False):

        self.config = Config()
        self.config.num_threads = os.cpu_count()

        if debug:
            configure_logging(log_level=logging.DEBUG)

        self.pipeline = LinearPipeline(self.config)

        self.flag_source_stage = False


    def _add(self, stage, **kwargs):
        self.pipeline.add_stage(
            self.STAGES[stage](self.config, **kwargs)
        )


    def add_stage(self, stage, **kwargs):
        stage = stage.strip()

        if stage not in self.STAGES.keys():
            raise KeyError(f"{stage} stage doesn't exist, the available options are {list(self.STAGES.keys())}")
        
        is_source = True if stage[:6] == 'source' else False

        if is_source and not self.flag_source_stage:
            self.pipeline.set_source(self.STAGES[stage](self.config, **kwargs))

        elif is_source and self.flag_source_stage:
            # TODO
            print('raise error: cannot add multiple source stages into the pipeline')
            return
        
        if not is_source:
            self._add(stage, **kwargs)

    def run(self):
        self.pipeline.run()
