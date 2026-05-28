from buf.validate import validate_pb2 as _validate_pb2
from commerce.v1 import amount_pb2 as _amount_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Item(_message.Message):
    __slots__ = ("item_id", "sku", "item_name", "item_brand", "item_category", "item_category2", "item_variant", "size", "price", "quantity", "percent_discount")
    ITEM_ID_FIELD_NUMBER: _ClassVar[int]
    SKU_FIELD_NUMBER: _ClassVar[int]
    ITEM_NAME_FIELD_NUMBER: _ClassVar[int]
    ITEM_BRAND_FIELD_NUMBER: _ClassVar[int]
    ITEM_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    ITEM_CATEGORY2_FIELD_NUMBER: _ClassVar[int]
    ITEM_VARIANT_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    PRICE_FIELD_NUMBER: _ClassVar[int]
    QUANTITY_FIELD_NUMBER: _ClassVar[int]
    PERCENT_DISCOUNT_FIELD_NUMBER: _ClassVar[int]
    item_id: str
    sku: str
    item_name: str
    item_brand: str
    item_category: str
    item_category2: str
    item_variant: str
    size: str
    price: _amount_pb2.Amount
    quantity: int
    percent_discount: int
    def __init__(self, item_id: _Optional[str] = ..., sku: _Optional[str] = ..., item_name: _Optional[str] = ..., item_brand: _Optional[str] = ..., item_category: _Optional[str] = ..., item_category2: _Optional[str] = ..., item_variant: _Optional[str] = ..., size: _Optional[str] = ..., price: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., quantity: _Optional[int] = ..., percent_discount: _Optional[int] = ...) -> None: ...

class SessionStart(_message.Message):
    __slots__ = ("is_first_session", "landing_page")
    IS_FIRST_SESSION_FIELD_NUMBER: _ClassVar[int]
    LANDING_PAGE_FIELD_NUMBER: _ClassVar[int]
    is_first_session: bool
    landing_page: str
    def __init__(self, is_first_session: _Optional[bool] = ..., landing_page: _Optional[str] = ...) -> None: ...

class ViewItemList(_message.Message):
    __slots__ = ("item_list_id", "item_list_name")
    ITEM_LIST_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_LIST_NAME_FIELD_NUMBER: _ClassVar[int]
    item_list_id: str
    item_list_name: str
    def __init__(self, item_list_id: _Optional[str] = ..., item_list_name: _Optional[str] = ...) -> None: ...

class ViewItem(_message.Message):
    __slots__ = ("item_list_id", "item_list_name")
    ITEM_LIST_ID_FIELD_NUMBER: _ClassVar[int]
    ITEM_LIST_NAME_FIELD_NUMBER: _ClassVar[int]
    item_list_id: str
    item_list_name: str
    def __init__(self, item_list_id: _Optional[str] = ..., item_list_name: _Optional[str] = ...) -> None: ...

class AddToCart(_message.Message):
    __slots__ = ("cart_id",)
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    def __init__(self, cart_id: _Optional[str] = ...) -> None: ...

class ViewCart(_message.Message):
    __slots__ = ("cart_id", "num_items")
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_ITEMS_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    num_items: int
    def __init__(self, cart_id: _Optional[str] = ..., num_items: _Optional[int] = ...) -> None: ...

class RemoveFromCart(_message.Message):
    __slots__ = ("cart_id", "reason")
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    reason: str
    def __init__(self, cart_id: _Optional[str] = ..., reason: _Optional[str] = ...) -> None: ...

class BeginCheckout(_message.Message):
    __slots__ = ("cart_id", "coupon")
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    COUPON_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    coupon: str
    def __init__(self, cart_id: _Optional[str] = ..., coupon: _Optional[str] = ...) -> None: ...

class AddShippingInfo(_message.Message):
    __slots__ = ("cart_id", "shipping_tier", "shipping_cost")
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_TIER_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_COST_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    shipping_tier: str
    shipping_cost: _amount_pb2.Amount
    def __init__(self, cart_id: _Optional[str] = ..., shipping_tier: _Optional[str] = ..., shipping_cost: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ...) -> None: ...

class AddPaymentInfo(_message.Message):
    __slots__ = ("cart_id", "payment_method")
    CART_ID_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_METHOD_FIELD_NUMBER: _ClassVar[int]
    cart_id: str
    payment_method: str
    def __init__(self, cart_id: _Optional[str] = ..., payment_method: _Optional[str] = ...) -> None: ...

class Purchase(_message.Message):
    __slots__ = ("transaction_id", "coupon", "tax", "shipping", "payment_method", "shipping_tier")
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    COUPON_FIELD_NUMBER: _ClassVar[int]
    TAX_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_METHOD_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_TIER_FIELD_NUMBER: _ClassVar[int]
    transaction_id: str
    coupon: str
    tax: _amount_pb2.Amount
    shipping: _amount_pb2.Amount
    payment_method: str
    shipping_tier: str
    def __init__(self, transaction_id: _Optional[str] = ..., coupon: _Optional[str] = ..., tax: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., shipping: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., payment_method: _Optional[str] = ..., shipping_tier: _Optional[str] = ...) -> None: ...

class Refund(_message.Message):
    __slots__ = ("transaction_id", "refund_amount", "reason")
    TRANSACTION_ID_FIELD_NUMBER: _ClassVar[int]
    REFUND_AMOUNT_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    transaction_id: str
    refund_amount: _amount_pb2.Amount
    reason: str
    def __init__(self, transaction_id: _Optional[str] = ..., refund_amount: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class Event(_message.Message):
    __slots__ = ("event_id", "event_name", "user_id", "session_id", "event_timestamp_ms", "device_category", "operating_system", "country", "region", "city", "source", "medium", "campaign", "page_location", "page_referrer", "items", "currency", "value", "session_start", "view_item_list", "view_item", "add_to_cart", "view_cart", "remove_from_cart", "begin_checkout", "add_shipping_info", "add_payment_info", "purchase", "refund")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_NAME_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIMESTAMP_MS_FIELD_NUMBER: _ClassVar[int]
    DEVICE_CATEGORY_FIELD_NUMBER: _ClassVar[int]
    OPERATING_SYSTEM_FIELD_NUMBER: _ClassVar[int]
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    REGION_FIELD_NUMBER: _ClassVar[int]
    CITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    MEDIUM_FIELD_NUMBER: _ClassVar[int]
    CAMPAIGN_FIELD_NUMBER: _ClassVar[int]
    PAGE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    PAGE_REFERRER_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    SESSION_START_FIELD_NUMBER: _ClassVar[int]
    VIEW_ITEM_LIST_FIELD_NUMBER: _ClassVar[int]
    VIEW_ITEM_FIELD_NUMBER: _ClassVar[int]
    ADD_TO_CART_FIELD_NUMBER: _ClassVar[int]
    VIEW_CART_FIELD_NUMBER: _ClassVar[int]
    REMOVE_FROM_CART_FIELD_NUMBER: _ClassVar[int]
    BEGIN_CHECKOUT_FIELD_NUMBER: _ClassVar[int]
    ADD_SHIPPING_INFO_FIELD_NUMBER: _ClassVar[int]
    ADD_PAYMENT_INFO_FIELD_NUMBER: _ClassVar[int]
    PURCHASE_FIELD_NUMBER: _ClassVar[int]
    REFUND_FIELD_NUMBER: _ClassVar[int]
    event_id: str
    event_name: str
    user_id: str
    session_id: str
    event_timestamp_ms: int
    device_category: str
    operating_system: str
    country: str
    region: str
    city: str
    source: str
    medium: str
    campaign: str
    page_location: str
    page_referrer: str
    items: _containers.RepeatedCompositeFieldContainer[Item]
    currency: str
    value: _amount_pb2.Amount
    session_start: SessionStart
    view_item_list: ViewItemList
    view_item: ViewItem
    add_to_cart: AddToCart
    view_cart: ViewCart
    remove_from_cart: RemoveFromCart
    begin_checkout: BeginCheckout
    add_shipping_info: AddShippingInfo
    add_payment_info: AddPaymentInfo
    purchase: Purchase
    refund: Refund
    def __init__(self, event_id: _Optional[str] = ..., event_name: _Optional[str] = ..., user_id: _Optional[str] = ..., session_id: _Optional[str] = ..., event_timestamp_ms: _Optional[int] = ..., device_category: _Optional[str] = ..., operating_system: _Optional[str] = ..., country: _Optional[str] = ..., region: _Optional[str] = ..., city: _Optional[str] = ..., source: _Optional[str] = ..., medium: _Optional[str] = ..., campaign: _Optional[str] = ..., page_location: _Optional[str] = ..., page_referrer: _Optional[str] = ..., items: _Optional[_Iterable[_Union[Item, _Mapping]]] = ..., currency: _Optional[str] = ..., value: _Optional[_Union[_amount_pb2.Amount, _Mapping]] = ..., session_start: _Optional[_Union[SessionStart, _Mapping]] = ..., view_item_list: _Optional[_Union[ViewItemList, _Mapping]] = ..., view_item: _Optional[_Union[ViewItem, _Mapping]] = ..., add_to_cart: _Optional[_Union[AddToCart, _Mapping]] = ..., view_cart: _Optional[_Union[ViewCart, _Mapping]] = ..., remove_from_cart: _Optional[_Union[RemoveFromCart, _Mapping]] = ..., begin_checkout: _Optional[_Union[BeginCheckout, _Mapping]] = ..., add_shipping_info: _Optional[_Union[AddShippingInfo, _Mapping]] = ..., add_payment_info: _Optional[_Union[AddPaymentInfo, _Mapping]] = ..., purchase: _Optional[_Union[Purchase, _Mapping]] = ..., refund: _Optional[_Union[Refund, _Mapping]] = ...) -> None: ...
