import argparse
import logging
import sys

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
import re
from pyflink.datastream.connectors import (FileSource, StreamFormat)
from pyflink.common.time import Duration


def word_count():
    input_path = "/files/streaming"
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    # write all the data to one file
    env.set_parallelism(1)

    fs = FileSource.\
        for_record_stream_format(StreamFormat.text_line_format(), input_path).\
        monitor_continuously(Duration.of_seconds(1)).\
        build()
            
    ds = env.from_source(
        source=fs,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )


    def split(line):
        yield from line.lower().strip().split()

    # compute word count
    def starts_with_letter(word):
        return re.match(r'^[a-zščřžýáéíěťóďůň]', word, re.IGNORECASE) is not None

    ds = ds.flat_map(split) \
        .filter(starts_with_letter) \
        .map(lambda i: (i[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
        .key_by(lambda i: i[0]) \
        .reduce(lambda i, j: (i[0], i[1] + j[1]))


    ds.print()

    # submit for execution
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    word_count()