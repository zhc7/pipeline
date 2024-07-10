import uuid
from typing import Type, Optional

from pipeline.utils import get_time, is_union_subset
from pydantic import BaseModel


class StageMeta(BaseModel):
    name: str
    signature: str


class Data(BaseModel):
    class Config:
        arbitrary_types_allowed = True

    uid_: int
    timestamp_: int
    producers_: list[tuple[StageMeta, int]]

    def __init__(self, /, **data):
        if "uid_" not in data:
            data["uid_"] = uuid.uuid4().int
            data["timestamp_"] = get_time()
            data["producers_"] = []
        super().__init__(**data)

    def __getitem__(self, item):
        return getattr(self, item)

    @classmethod
    def not_fit(cls, data: Type["Data"]) -> Optional[str]:
        """check if all the field in data is covered by this instance"""
        cls_fields = cls.model_fields
        for field, info in data.model_fields.items():
            if not info.is_required():
                continue
            ok = field in cls_fields and is_union_subset(
                cls_fields[field].annotation, info.annotation
            )
            if not ok:
                return field

    def trace(self, stage_meta: StageMeta, timestamp: int):
        self.producers_.append((stage_meta, timestamp))


class EmptyData(Data):
    pass


class AnyData(Data):
    class Config:
        extra = "allow"


class FailedDataType(Data):
    failed_: bool = True


FailedData = FailedDataType()
