import argparse
import logging
import sys
import re

from pyflink.common import WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors import FileSource, StreamFormat
import json

def load_idnes_data():
    json_path = "/files/articles_fixed.json"

    # Initialize the environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)

    # Define the file source
    fs = FileSource.for_record_stream_format(StreamFormat.text_line_format(), json_path).process_static_file_set().build()
    ds = env.from_source(
        source=fs,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="file_source"
    )
    i = 0
    def parse_json(line):
        nonlocal i
        i += 1
        if i % 1000 == 0:
            print(f"Processed {i} lines so far")
        try:
            return json.loads(line)  # Parse each line as JSON
        except json.JSONDecodeError as e:
            print(f"Failed to parse line: {line}. Error: {e}")
            logging.error(f"Failed to parse line: {line}. Error: {e}")
            return None


    def split(text):
        return re.findall(r'\w+', text.lower())  # Split text into words

    # Count total articles
    ds_count = (
        ds.map(parse_json)  # Removed `output_type`
          .filter(lambda i: i is not None)  # Filter out invalid JSON lines
          .map(lambda i: ("count", 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda i: i[0])
          .reduce(lambda i, j: (i[0], i[1] + j[1]))
    )

    # Print total article count
    ds_count.print()
    
        # compute word count
    def starts_with_letter(word):
        return re.match(r'^[a-zščřžýáéíěťóďůň]', word, re.IGNORECASE) is not None


    # Analyze word frequencies in titles and content
    ds1 = (
        ds.map(parse_json) \
          .filter(lambda i: i is not None)  # Filter out invalid JSON lines
          .flat_map(lambda i: split(i.get("title", "") + " " + i.get("content", "")),
                    output_type=Types.STRING())  # Extract words from title and content
        .filter(starts_with_letter)
          .map(lambda word: (word[0], 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()]))
          .key_by(lambda i: i[0])
          .reduce(lambda i, j: (i[0], i[1] + j[1]))
    )

    # Print word frequencies
    ds1.print()

    # Execute the Flink job
    env.execute("IDNES Data Analysis")


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    load_idnes_data()


"""

Processed 337330 lines so far
(count,337338)

(a,6530907)
(b,4262844)
(c,2258154)
(d,6543803)
(e,812517)
(f,968215)
(g,376533)
(h,2322683)
(i,1847120)
(j,6150486)
(k,6482782)
(l,2405596)
(m,6405004)
(n,11079894)
(o,5846777)
(p,16031595)
(q,11473)
(r,3134715)
(s,12621308)
(t,6721363)
(u,2544035)
(v,11066535)
(w,208060)
(x,14381)
(y,44029)
(z,7612755)
(á,4469)
(é,6292)
(í,28776)
(ó,1865)
(ý,30)
(č,2119920)
(ď,3449)
(ě,44)
(ı,100)
(ň,447)
(ř,1009346)
(š,857304)
(ť,7252)
(ů,28)
(ž,2136215)



"""