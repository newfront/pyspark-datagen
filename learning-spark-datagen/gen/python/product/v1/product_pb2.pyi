import datetime

from buf.validate import validate_pb2 as _validate_pb2
from commerce.v1 import amount_pb2 as _amount_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Product(_message.Message):
    __slots__ = ("product_id", "sku", "name", "brand", "category", "category2", "variant", "size", "price", "percent_discount", "created", "in_stock", "inventory_count")
    PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    BRAND_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    CATEGORY2_FIELD_NUMBER: _ClassVar[int]
    VARIANT_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    PERCENT_DISCOUNT_FIELD_NUMBER: _ClassVar[int]
    CREATED_FIELD_NUMBER: _ClassVar[int]
    IN_STOCK_FIELD_NUMBER: _ClassVar[int]
    INVENTORY_COUNT_FIELD_NUMBER: _ClassVar[int]
    product_id: str
    sku: str
    name: str
    brand: str
    category: str
    category2: str
    variant: str
    size: str
    price: _amount_pb2.Amount
    percent_discount: int
    created: _timestamp_pb2.Timestamp
    in_stock: bool
    inventory_count: int
    def __init__(self, product_id: _Optional[str] = ..., sku: _Optional[str] = ..., name: _Optional[str] = ..., brand: _Optional[str] = ..., category: _Optional[str] = ..., category2: _Optional[str] = ..., variant: _Optional[str] = ..., size: _Optional[str] = ..., price: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., percent_discount: _Optional[int] = ..., created: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., in_stock: _Optional[bool] = ..., inventory_count: _Optional[int] = ...) -> None: ...
