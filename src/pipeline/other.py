import os

import pandas as pd
from pipeline.datas import Data
from pipeline.staging import Stage
from pyarrow import parquet as pq


class ParquetWriter(Stage):
    def __init__(self, filename: str = "data.parquet", **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.filename = filename

    def generate(self, inp: Data) -> Data:
        return inp

    def dump(self, data: list[Data], location: str):
        table = pd.DataFrame([item.model_dump() for item in data])
        pq.write_table(table, os.path.join(location, self.filename))
