from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Order(_message.Message):
    __slots__ = ("created_at", "updated_at", "user_id", "products", "total", "coupon_code_used")
    class Amount(_message.Message):
        __slots__ = ("currency", "units", "nanos")
        CURRENCY_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        NANOS_FIELD_NUMBER: _ClassVar[int]
        currency: str
        units: int
        nanos: int
        def __init__(self, currency: _Optional[str] = ..., units: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...
    class Product(_message.Message):
        __slots__ = ("product_id", "num_items", "unit_cost", "percent_discount")
        PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
        NUM_ITEMS_FIELD_NUMBER: _ClassVar[int]
        UNIT_COST_FIELD_NUMBER: _ClassVar[int]
        PERCENT_DISCOUNT_FIELD_NUMBER: _ClassVar[int]
        product_id: str
        num_items: int
        unit_cost: Order.Amount
        percent_discount: int
        def __init__(self, product_id: _Optional[str] = ..., num_items: _Optional[int] = ..., unit_cost: _Optional[_Union[Order.Amount, _Mapping]] = ..., percent_discount: _Optional[int] = ...) -> None: ...
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    PRODUCTS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    COUPON_CODE_USED_FIELD_NUMBER: _ClassVar[int]
    created_at: int
    updated_at: int
    user_id: str
    products: _containers.RepeatedCompositeFieldContainer[Order.Product]
    total: Order.Amount
    coupon_code_used: bool
    def __init__(self, created_at: _Optional[int] = ..., updated_at: _Optional[int] = ..., user_id: _Optional[str] = ..., products: _Optional[_Iterable[_Union[Order.Product, _Mapping]]] = ..., total: _Optional[_Union[Order.Amount, _Mapping]] = ..., coupon_code_used: _Optional[bool] = ...) -> None: ...
