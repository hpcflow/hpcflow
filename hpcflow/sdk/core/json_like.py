"""
Serialization and deserialization mechanism intended to map between a complex
graph of objects and either JSON or YAML.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence, Mapping
import copy
from dataclasses import dataclass
import enum
from types import SimpleNamespace
from typing import overload, Protocol, cast, runtime_checkable, TYPE_CHECKING
from typing_extensions import final, override

from hpcflow.sdk.core.app_aware import AppAware
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk import app, get_SDK_logger
from .utils import get_md5_hash
from .validation import get_schema
from .errors import ToJSONLikeChildReferenceError

if TYPE_CHECKING:
    from typing import Any, ClassVar, Literal
    from typing_extensions import Self, TypeAlias, TypeGuard
    from ..app import BaseApp
    from .object_list import ObjectList

_BasicJsonTypes: TypeAlias = "int | float | str | None"
_WriteStructure: TypeAlias = (
    "list[JSONable] | tuple[JSONable, ...] | set[JSONable] | dict[str, JSONable]"
)
JSONDocument: TypeAlias = "Sequence[JSONed] | Mapping[str, JSONed]"
JSONable: TypeAlias = "_WriteStructure | enum.Enum | BaseJSONLike | _BasicJsonTypes"
JSONed: TypeAlias = "JSONDocument | _BasicJsonTypes"

if TYPE_CHECKING:
    _JSONDeserState: TypeAlias = "dict[str, dict[str, JSONed]] | None"


#: Primitive types supported by the serialization mechanism.
PRIMITIVES = (
    int,
    float,
    str,
    type(None),
)

_SDK_logger = get_SDK_logger(__name__)


@runtime_checkable
class _AltConstructFromJson(Protocol):
    @classmethod
    def _json_like_constructor(cls, json_like: Mapping[str, JSONed]) -> Self:
        pass


def _is_base_json_like(value: JSONable) -> TypeGuard[BaseJSONLike]:
    return value is not None and hasattr(value, "to_json_like")


_MAX_DEPTH = 50


@overload
def to_json_like(
    obj: int,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[int, _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: float,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[float, _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: str,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[str, _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: None,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[None, _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: enum.Enum,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[str, _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: list[JSONable],
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[Sequence[JSONed], _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: tuple[JSONable, ...],
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[Sequence[JSONed], _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: set[JSONable],
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[Sequence[JSONed], _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: dict[str, JSONable],
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[Mapping[str, JSONed], _JSONDeserState]:
    ...


@overload
def to_json_like(
    obj: BaseJSONLike,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
) -> tuple[Mapping[str, JSONed], _JSONDeserState]:
    ...


def to_json_like(
    obj: JSONable,
    shared_data: _JSONDeserState = None,
    parent_refs: dict | None = None,
    path: list | None = None,
):
    """
    Convert the object to a JSON-like basic value tree.
    Such trees are trivial to serialize as JSON or YAML.
    """
    path = path or []

    if len(path) > _MAX_DEPTH:
        raise RuntimeError(f"I'm in too deep! Path is: {path}")

    if isinstance(obj, (list, tuple, set)):
        out_list: list[JSONed] = []
        for idx, item in enumerate(obj):
            if _is_base_json_like(item):
                new_item, shared_data = item.to_json_like(
                    shared_data=shared_data,
                    exclude=set((parent_refs or {}).values()),
                    path=[*path, idx],
                )
                out_list.append(new_item)
            else:
                new_std_item, shared_data = to_json_like(
                    item, shared_data=shared_data, path=[*path, idx]
                )
                out_list.append(new_std_item)
        if isinstance(obj, tuple):
            return tuple(out_list), shared_data
        elif isinstance(obj, set):
            return set(out_list), shared_data
        else:
            return out_list, shared_data

    elif isinstance(obj, dict):
        out_map: dict[str, JSONed] = {}
        for dct_key, dct_val in obj.items():
            if _is_base_json_like(dct_val):
                try:
                    out_map[dct_key], shared_data = dct_val.to_json_like(
                        shared_data=shared_data,
                        exclude={(parent_refs or {}).get(dct_key)},
                        path=[*path, dct_key],
                    )
                except ToJSONLikeChildReferenceError:
                    continue
            else:
                out_map[dct_key], shared_data = to_json_like(
                    dct_val,
                    shared_data=shared_data,
                    parent_refs=parent_refs,
                    path=[*path, dct_key],
                )
        return out_map, shared_data

    elif isinstance(obj, PRIMITIVES):
        return obj, shared_data

    elif isinstance(obj, enum.Enum):
        return obj.name, shared_data

    else:
        return obj.to_json_like(shared_data=shared_data, path=path)


@dataclass
class ChildObjectSpec:
    """
    Used to describe what the child structure of an class is so that the generic
    deserializer can build the structure.
    """

    #: The name of the attribute.
    name: str
    #: The name of the class (or class of members of a list) used to deserialize the
    #: attribute.
    class_name: str | None = None
    #: The class (or class of members of a list) used to deserialize the
    #: attribute.
    class_obj: type[enum.Enum | BaseJSONLike] | None = None
    # TODO: no need for class_obj/class_name if shared data?
    #: The name of the key used in the JSON document, if different from the attribute
    #: name.
    json_like_name: str | None = None
    #: If true, the attribute is really a list of instances,
    #: or a dictionary if :attr:`dict_key_attr` is set.
    is_multiple: bool | None = False
    #: If set, the name of an attribute of the object to use as a dictionary key.
    #: Requires that :attr:`is_multiple` be set as well.
    dict_key_attr: str | None = None
    #: If set, the name of an attribute of the object to use as a dictionary value.
    #: If not set but :attr:`dict_key_attr` is set, the whole object is the value.
    #: Requires that :attr:`dict_key_attr` be set as well.
    dict_val_attr: str | None = None
    #: If set, the attribute of the child object that contains a reference to its parent.
    parent_ref: str | None = None
    # TODO: do parent refs make sense when from shared? Prob not.
    #: If true, enables special handling where there can be only one child descriptor
    #: for a containing class.
    is_single_attribute: bool | None = False
    #: If true, the object is an enum member and should use special serialization rules.
    is_enum: bool | None = False
    #: If true, the child object is a dict, whose values are of the specified class.
    #: The dict structure will remain.
    is_dict_values: bool | None = False
    #: If true, values that are not lists are cast to lists and multiple child objects
    #: are instantiated for each dict value.
    is_dict_values_ensure_list: bool | None = False
    #: What key to look values up under in the shared data cache.
    #: If unspecified, the shared data cache is ignored.
    shared_data_name: str | None = None
    #: What attribute provides the value of the key into the shared data cache.
    #: If unspecified, a hash of the object dictionary is used.
    #: Ignored if :py:attr:`~.shared_data_name` is unspecified.
    shared_data_primary_key: str | None = None
    # shared_data_secondary_keys: tuple[str, ...] | None = None # TODO: what's the point?

    def __post_init__(self):
        if self.class_name is not None and self.class_obj is not None:
            raise ValueError("Specify at most one of `class_name` and `class_obj`.")

        if self.dict_key_attr:
            if not isinstance(self.dict_key_attr, str):
                raise TypeError(
                    "`dict_key_attr` must be of type `str`, but has type "
                    f"{type(self.dict_key_attr)} with value {self.dict_key_attr}."
                )  # TODO: test raise
        if self.dict_val_attr:
            if not self.dict_key_attr:
                raise ValueError(
                    "If `dict_val_attr` is specified, `dict_key_attr` must be specified."
                )  # TODO: test raise
            if not isinstance(self.dict_val_attr, str):
                raise TypeError(
                    "`dict_val_attr` must be of type `str`, but has type "
                    f"{type(self.dict_val_attr)} with value {self.dict_val_attr}."
                )  # TODO: test raise
        if not self.is_multiple and self.dict_key_attr:
            raise ValueError(
                "If `dict_key_attr` is specified, `is_multiple` must be set to True."
            )
        if not self.is_multiple and self.is_dict_values:
            raise ValueError(
                "If `is_dict_values` is specified, `is_multiple` must be set to True."
            )
        if self.is_dict_values_ensure_list and not self.is_dict_values:
            raise ValueError(
                "If `is_dict_values_ensure_list` is specified, `is_dict_values` must be "
                "set to True."
            )
        if self.parent_ref:
            if not isinstance(self.parent_ref, str):
                raise TypeError(
                    "`parent_ref` must be of type `str`, but has type "
                    f"{type(self.parent_ref)} with value {self.parent_ref}."
                )  # TODO: test raise

        self.json_like_name = self.json_like_name or self.name


_ChildType: TypeAlias = "type[enum.Enum | JSONLike]"


@hydrate
class BaseJSONLike:
    """
    An object that has a serialization as JSON or YAML.

    Parameters
    ----------
    _class_namespace : namespace
        Namespace whose attributes include the class definitions that might be
        referenced (and so require instantiation) in child objects.
    _shared_data_namespace : namespace
        Namespace whose attributes include the shared data that might be referenced
        in child objects.
    """

    _child_objects: ClassVar[Sequence[ChildObjectSpec] | None] = None
    _validation_schema: ClassVar[str | None] = None

    __class_namespace: ClassVar[dict[str, Any] | SimpleNamespace | BaseApp | None] = None
    _hash_value: str | None

    @overload
    @classmethod
    def _set_class_namespace(
        cls, value: SimpleNamespace, is_dict: Literal[False] = False
    ) -> None:
        ...

    @overload
    @classmethod
    def _set_class_namespace(cls, value: dict[str, Any], is_dict: Literal[True]) -> None:
        ...

    @classmethod
    def _set_class_namespace(
        cls, value: dict[str, Any] | SimpleNamespace, is_dict=False
    ) -> None:
        cls.__class_namespace = value

    @classmethod
    def _class_namespace(cls) -> dict[str, Any] | SimpleNamespace | BaseApp:
        if (ns := cls.__class_namespace) is None:
            raise ValueError(f"`{cls.__name__}` `class_namespace` must be set!")
        return ns

    @classmethod
    def _get_child_class(cls, child_obj_spec: ChildObjectSpec) -> _ChildType | None:
        if child_obj_spec.class_obj:
            return cast(_ChildType, child_obj_spec.class_obj)
        elif child_obj_spec.class_name:
            ns = cls._class_namespace()
            if isinstance(ns, dict):
                return ns[child_obj_spec.class_name]
            else:
                return getattr(ns, child_obj_spec.class_name)
        else:
            return None

    @classmethod
    def _get_default_shared_data(cls) -> Mapping[str, ObjectList[JSONable]]:
        return {}

    @overload
    @classmethod
    def from_json_like(
        cls,
        json_like: str,
        shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
    ) -> Self | None:
        ...

    @overload
    @classmethod
    def from_json_like(
        cls,
        json_like: Sequence[Mapping[str, JSONed]] | Mapping[str, JSONed],
        shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
    ) -> Self:
        ...

    @overload
    @classmethod
    def from_json_like(
        cls,
        json_like: None,
        shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
    ) -> None:
        ...

    @classmethod
    def from_json_like(
        cls,
        json_like: str | Mapping[str, JSONed] | Sequence[Mapping[str, JSONed]] | None,
        shared_data: Mapping[str, ObjectList[JSONable]] | None = None,
    ) -> Self | None:
        """
        Make an instance of this class from JSON (or YAML) data.

        Parameters
        ----------
        json_like:
            The data to deserialise.
        shared_data:
            Shared context data.

        Returns
        -------
            The deserialised object.
        """
        shared_data = shared_data or cls._get_default_shared_data()
        if isinstance(json_like, str):
            json_like = cls._parse_from_string(json_like)
        if json_like is None:
            # e.g. optional attributes # TODO: is this still needed?
            return None
        return cls._from_json_like(copy.deepcopy(json_like), shared_data)

    @classmethod
    def _parse_from_string(cls, string: str) -> dict[str, str] | None:
        raise TypeError(f"unparseable {cls}: '{string}'")

    @classmethod
    def __remap_child_seq(
        cls, spec: ChildObjectSpec, json_like: JSONed
    ) -> tuple[list[JSONed], dict[str, list[int]]]:
        if not spec.is_multiple:
            return [json_like], {}
        elif isinstance(json_like, list):
            return json_like, {}
        elif not isinstance(json_like, dict):
            raise TypeError(
                f"Child object {spec.name} of {cls.__name__!r} must be a list or "
                f"dict, but is of type {type(json_like)} with value {json_like!r}."
            )

        multi_chd_objs: list[JSONed] = []

        if spec.is_dict_values:
            # (if is_dict_values) indices into multi_chd_objs that enable reconstruction
            # of the source dict:
            is_dict_values_idx: dict[str, list[int]] = defaultdict(list)

            # keep as a dict
            for k, v in json_like.items():
                if spec.is_dict_values_ensure_list:
                    if not isinstance(v, list):
                        v = [v]
                else:
                    v = [v]

                for i in v:
                    is_dict_values_idx[k].append(len(multi_chd_objs))
                    multi_chd_objs.append(i)
            return multi_chd_objs, is_dict_values_idx

        # want to cast to a list
        if not spec.dict_key_attr:
            raise ValueError(
                f"{cls.__name__!r}: must specify a `dict_key_attr` for child "
                f"object spec {spec.name!r}."
            )

        for k, v in json_like.items():
            all_attrs: dict[str, JSONed] = {spec.dict_key_attr: k}
            if spec.dict_val_attr:
                all_attrs[spec.dict_val_attr] = v
            elif isinstance(v, dict):
                all_attrs.update(v)
            else:
                raise TypeError(
                    f"Value for key {k!r} must be a dict representing "
                    f"attributes of the {spec.name!r} child object "
                    f"(parent: {cls.__name__!r}). If it instead "
                    f"represents a single attribute, set the "
                    f"`dict_val_attr` of the child object spec."
                )
            multi_chd_objs.append(all_attrs)

        return multi_chd_objs, {}

    @classmethod
    def __inflate_enum(cls, chd_cls: type[enum.Enum], multi_chd_objs: list[JSONed]):
        out: list[JSONable] = []
        for i in multi_chd_objs:
            if i is None:
                out.append(None)
            elif not isinstance(i, str):
                raise ValueError(
                    f"Enumeration {chd_cls!r} has no name {i!r}. Available"
                    f" names are: {chd_cls._member_names_!r}."
                )
            else:
                try:
                    out.append(getattr(chd_cls, i.upper()))
                except AttributeError:
                    raise ValueError(
                        f"Enumeration {chd_cls!r} has no name {i!r}. Available"
                        f" names are: {chd_cls._member_names_!r}."
                    )
        return out

    @classmethod
    def _from_json_like(
        cls,
        json_like: Mapping[str, JSONed] | Sequence[Mapping[str, JSONed]],
        shared_data: Mapping[str, ObjectList[JSONable]],
    ) -> Self:
        def from_json_like_item(
            child_obj_spec: ChildObjectSpec, json_like_i: JSONed
        ) -> JSONable:
            if not (
                child_obj_spec.class_name
                or child_obj_spec.class_obj
                or child_obj_spec.is_multiple
                or child_obj_spec.shared_data_name
            ):
                # Nothing to process:
                return cast("JSONable", json_like_i)

            # (if is_dict_values) indices into multi_chd_objs that enable reconstruction
            # of the source dict:
            multi_chd_objs, is_dict_values_idx = cls.__remap_child_seq(
                child_obj_spec, json_like_i
            )

            out: list[JSONable] = []
            if chd.shared_data_name:
                for i in multi_chd_objs:
                    if i is None:
                        out.append(i)
                        continue

                    sd_lookup_kwargs: dict[str, JSONable]
                    if isinstance(i, str):
                        if i.startswith("hash:"):
                            sd_lookup_kwargs = {"_hash_value": i.split("hash:")[1]}
                        else:
                            assert chd.shared_data_primary_key
                            sd_lookup_kwargs = {chd.shared_data_primary_key: i}
                    elif isinstance(i, dict):
                        sd_lookup_kwargs = i
                    else:
                        raise TypeError(
                            "Shared data reference must be a str or a dict."
                        )  # TODO: test raise
                    out.append(shared_data[chd.shared_data_name].get(**sd_lookup_kwargs))
            else:
                chd_cls = cls._get_child_class(child_obj_spec)
                assert chd_cls is not None
                if issubclass(chd_cls, enum.Enum):
                    out = cls.__inflate_enum(chd_cls, multi_chd_objs)
                else:
                    out.extend(
                        (
                            None
                            if i is None
                            else chd_cls.from_json_like(
                                cast("Any", i),  # FIXME: This is "Trust me, bro!" hack
                                shared_data,
                            )
                        )
                        for i in multi_chd_objs
                    )

            if child_obj_spec.is_dict_values:
                if child_obj_spec.is_dict_values_ensure_list:
                    return {
                        k: [out[i] for i in v2] for k, v2 in is_dict_values_idx.items()
                    }
                else:
                    return {
                        k: next(out[i] for i in v2)
                        for k, v2 in is_dict_values_idx.items()
                    }

            elif not child_obj_spec.is_multiple:
                return out[0]

            return out

        if cls._validation_schema:
            validation_schema = get_schema(cls._validation_schema)
            validated = validation_schema.validate(json_like)
            if not validated.is_valid:
                raise ValueError(validated.get_failures_string())

        json_like_copy = copy.deepcopy(json_like)

        if cls._child_objects:
            for chd in cls._child_objects:
                if chd.is_single_attribute:
                    if len(cls._child_objects) > 1:
                        raise TypeError(
                            f"If ChildObjectSpec has `is_single_attribute=True`, only one "
                            f"ChildObjectSpec may be specified on the class. Specified child "
                            f"objects specs are: {cls._child_objects!r}."
                        )
                    json_like_copy = {chd.name: json_like_copy}

                assert isinstance(json_like_copy, Mapping)
                if chd.json_like_name and chd.json_like_name in json_like_copy:
                    jlc = dict(json_like_copy)
                    json_like_i = jlc.pop(chd.json_like_name)
                    jlc[chd.name] = cast("JSONed", from_json_like_item(chd, json_like_i))
                    json_like_copy = jlc

        assert isinstance(json_like_copy, Mapping)

        need_hash = hasattr(cls, "_hash_value") and "_hash_value" not in json_like_copy

        try:
            if issubclass(cls, _AltConstructFromJson):
                obj = cls._json_like_constructor(json_like_copy)
            else:
                obj = cls(**json_like_copy)
        except TypeError as err:
            raise TypeError(
                f"Failed initialisation of class {cls.__name__!r}. Check the signature. "
                f"Caught TypeError: {err}"
            ) from err

        if need_hash:
            obj._set_hash()

        return obj

    def _set_parent_refs(self, child_name_attrs: Mapping[str, str] | None = None):
        """Assign references to self on child objects that declare a parent ref
        attribute."""

        for chd in self._child_objects or ():
            if chd.parent_ref:
                chd_name = (child_name_attrs or {}).get(chd.name, chd.name)
                if chd.is_multiple:
                    for chd_obj in getattr(self, chd_name):
                        if chd_obj:
                            setattr(chd_obj, chd.parent_ref, self)
                else:
                    chd_obj = getattr(self, chd_name)
                    if chd_obj:
                        setattr(chd_obj, chd.parent_ref, self)

    def _get_hash(self) -> str:
        json_like = self.to_json_like()[0]
        hash_val = self._get_hash_from_json_like(json_like)
        return hash_val

    def _set_hash(self) -> None:
        self._hash_value = self._get_hash()

    @staticmethod
    def _get_hash_from_json_like(json_like) -> str:
        json_like = copy.deepcopy(json_like)
        json_like.pop("_hash_value", None)
        return get_md5_hash(json_like)

    @final
    def to_dict(self) -> dict[str, Any]:
        """
        Serialize this object as a dictionary.
        """
        if hasattr(self, "__dict__"):
            return self._postprocess_to_dict(dict(self.__dict__))
        elif hasattr(self, "__slots__"):
            return self._postprocess_to_dict(
                {k: getattr(self, k) for k in self.__slots__}
            )
        else:
            return self._postprocess_to_dict({})

    def _postprocess_to_dict(self, dct: dict[str, Any]) -> dict[str, Any]:
        """
        Apply any desired postprocessing to the results of :meth:`to_dict`.
        """
        return dct

    def to_json_like(
        self,
        dct: dict[str, JSONable] | None = None,
        shared_data: _JSONDeserState = None,
        exclude: set[str | None] | None = None,
        path: list | None = None,
    ) -> tuple[JSONDocument, _JSONDeserState]:
        """
        Serialize this object as an object structure that can be trivially converted
        to JSON. Note that YAML can also be produced from the result of this method;
        it just requires a different final serialization step.
        """
        if dct is None:
            dct_value = {
                k: v for k, v in self.to_dict().items() if k not in (exclude or ())
            }
        else:
            dct_value = dct

        parent_refs: dict[str, str] = {}
        if self._child_objects:
            for chd in self._child_objects:
                if chd.is_single_attribute:
                    if len(self._child_objects) > 1:
                        raise TypeError(
                            "If ChildObjectSpec has `is_single_attribute=True`, only one "
                            "ChildObjectSpec may be specified on the class."
                        )
                    assert chd.json_like_name is not None
                    dct_value = dct_value[chd.json_like_name]

                if chd.parent_ref:
                    parent_refs[chd.name] = chd.parent_ref

        json_like_, shared_data = to_json_like(
            dct_value, shared_data=shared_data, parent_refs=parent_refs, path=path
        )
        json_like: dict[str, JSONed] | list[JSONed] = cast("Any", json_like_)
        shared_data = shared_data or {}

        for chd in self._child_objects or ():
            assert chd.json_like_name is not None
            if chd.name in json_like:
                assert isinstance(json_like, dict)
                json_like[chd.json_like_name] = json_like.pop(chd.name)

            if chd.shared_data_name:
                assert isinstance(json_like, dict)
                if chd.shared_data_name not in shared_data:
                    shared_data[chd.shared_data_name] = {}

                chd_obj_js = json_like.pop(chd.json_like_name)

                if not chd.is_multiple:
                    chd_obj_js = [chd_obj_js]

                shared_keys: list[JSONed] = []
                assert isinstance(chd_obj_js, (list, tuple, set))
                for i in chd_obj_js:
                    if i is None:
                        continue
                    i.pop("_hash_value", None)
                    hash_i = self._get_hash_from_json_like(i)
                    shared_keys.append(f"hash:{hash_i}")
                    shared_data[chd.shared_data_name].setdefault(hash_i, i)

                if not chd.is_multiple:
                    try:
                        json_like[chd.json_like_name] = shared_keys[0]
                    except IndexError:
                        json_like[chd.json_like_name] = None
                else:
                    json_like[chd.json_like_name] = shared_keys

        return json_like, shared_data


@hydrate
class JSONLike(BaseJSONLike, AppAware):
    """BaseJSONLike, where the class namespace is the App instance."""

    __sdk_classes: ClassVar[list[type[BaseJSONLike]]] = []

    @classmethod
    def _class_namespace(cls) -> BaseApp:
        return getattr(cls, cls._app_attr)

    @classmethod
    def __get_classes(cls) -> list[type[BaseJSONLike]]:
        """
        Get the collection of actual SDK classes that conform to BaseJSONLike.
        """
        if not cls.__sdk_classes:
            for cls_name in app.sdk_classes:
                cls2 = getattr(app, cls_name)
                if isinstance(cls2, type) and issubclass(cls2, BaseJSONLike):
                    cls.__sdk_classes.append(cls2)
        return cls.__sdk_classes

    @override
    def _postprocess_to_dict(self, d: dict[str, Any]) -> dict[str, Any]:
        out = super()._postprocess_to_dict(d)

        # remove parent references:
        for cls in self.__get_classes():
            for chd in cls._child_objects or ():
                if chd.parent_ref:
                    # _SDK_logger.debug(
                    #     f"removing parent reference {chd.parent_ref!r} from child "
                    #     f"object {chd!r}."
                    # )
                    if (
                        self.__class__.__name__ == chd.class_name
                        or self.__class__ is chd.class_obj
                    ):
                        out.pop(chd.parent_ref, None)
        return out
