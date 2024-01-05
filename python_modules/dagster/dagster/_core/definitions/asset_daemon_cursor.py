import base64
import binascii
import functools
import gzip
import json
from typing import (
    TYPE_CHECKING,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
)

from dagster._core.definitions.asset_graph_subset import AssetGraphSubset
from dagster._core.definitions.asset_subset import AssetSubset
from dagster._core.definitions.events import AssetKey
from dagster._serdes.serdes import (
    DeserializationError,
    FieldSerializer,
    JsonSerializableValue,
    PackableValue,
    SerializableNonScalarKeyMapping,
    UnpackContext,
    WhitelistMap,
    deserialize_value,
    pack_value,
    serialize_value,
    unpack_value,
    whitelist_for_serdes,
)

from .asset_graph import AssetGraph

if TYPE_CHECKING:
    from .asset_condition import (
        AssetConditionEvaluationInfo,
        AssetConditionEvaluationResult,
        AssetConditionSnapshot,
    )


@whitelist_for_serdes
class AssetConditionCursorExtras(NamedTuple):
    """Represents additional state that may be optionally saved by an AssetCondition between
    evaluations.
    """

    condition_snapshot: "AssetConditionSnapshot"
    extras: Mapping[str, PackableValue]


class ObserveRequestTimestampSerializer(FieldSerializer):
    def pack(
        self,
        mapping: Mapping[str, float],
        whitelist_map: WhitelistMap,
        descent_path: str,
    ) -> JsonSerializableValue:
        return pack_value(SerializableNonScalarKeyMapping(mapping), whitelist_map, descent_path)

    def unpack(
        self,
        unpacked_value: JsonSerializableValue,
        whitelist_map: WhitelistMap,
        context: UnpackContext,
    ) -> PackableValue:
        return unpack_value(unpacked_value, dict, whitelist_map, context)


@whitelist_for_serdes(
    field_serializers={
        "last_observe_request_timestamp_by_asset_key": ObserveRequestTimestampSerializer
    }
)
class AssetDaemonCursor(NamedTuple):
    """State that's stored between daemon evaluations.

    Attributes:
        evaluation_id (int): The ID of the evaluation that produced this cursor.
        previous_evaluation_info (Sequence[AssetConditionEvaluationInfo]): The evaluation info
            recorded for each asset on the previous tick.
    """

    evaluation_id: int
    previous_evaluation_info: Sequence["AssetConditionEvaluationInfo"]

    last_observe_request_timestamp_by_asset_key: Mapping[AssetKey, float]

    @staticmethod
    def empty(evaluation_id: int = 0) -> "AssetDaemonCursor":
        return AssetDaemonCursor(
            evaluation_id=evaluation_id,
            previous_evaluation_info=[],
            last_observe_request_timestamp_by_asset_key={},
        )

    @staticmethod
    def from_serialized(
        raw_cursor: Optional[str], asset_graph: Optional[AssetGraph], default_evaluation_id: int = 0
    ) -> "AssetDaemonCursor":
        """Deserializes an AssetDaemonCursor from a string. Provides a backcompat layer for the old
        manually-serialized cursor format.
        """
        if raw_cursor is None:
            return AssetDaemonCursor.empty(default_evaluation_id)
        try:
            decoded = base64.b64decode(raw_cursor)
            unzipped = gzip.decompress(decoded).decode("utf-8")
            return deserialize_value(unzipped, AssetDaemonCursor)
        except (binascii.Error, gzip.BadGzipFile, json.JSONDecodeError, DeserializationError):
            # this cursor was serialized with the old format
            return backcompat_deserialize_asset_daemon_cursor_str(
                raw_cursor, asset_graph, default_evaluation_id
            )

    def serialize(self) -> str:
        """Serializes the cursor into a string. To do so, the cursor is first serialized using the
        traditional Dagster serialization process, then gzipped, then base64 encoded, and finally
        converted to a string.
        """
        return base64.b64encode(gzip.compress(serialize_value(self).encode("utf-8"))).decode(
            "utf-8"
        )

    @property
    @functools.lru_cache(maxsize=1)
    def previous_evaluation_info_by_key(self) -> Mapping[AssetKey, "AssetConditionEvaluationInfo"]:
        """Efficient lookup of previous evaluation info by asset key."""
        return {
            evaluation_info.asset_key: evaluation_info
            for evaluation_info in self.previous_evaluation_info
        }

    def get_previous_evaluation_info(
        self, asset_key: AssetKey
    ) -> Optional["AssetConditionEvaluationInfo"]:
        """Returns the AssetConditionCursor associated with the given asset key. If no stored
        cursor exists, returns an empty cursor.
        """
        return self.previous_evaluation_info_by_key.get(asset_key)

    def get_previous_evaluation_result(
        self, asset_key: AssetKey
    ) -> Optional["AssetConditionEvaluationResult"]:
        """Returns the previous AssetConditionEvaluation for a given asset key, if it exists."""
        previous_evaluation_info = self.get_previous_evaluation_info(asset_key)
        return previous_evaluation_info.evaluation_result if previous_evaluation_info else None

    def with_updates(
        self,
        evaluation_id: int,
        evaluation_timestamp: float,
        newly_observe_requested_asset_keys: Sequence[AssetKey],
        evaluation_info: Sequence["AssetConditionEvaluationInfo"],
    ) -> "AssetDaemonCursor":
        return self._replace(
            evaluation_id=evaluation_id,
            previous_evaluation_info=evaluation_info,
            last_observe_request_timestamp_by_asset_key={
                **self.last_observe_request_timestamp_by_asset_key,
                **{
                    asset_key: evaluation_timestamp
                    for asset_key in newly_observe_requested_asset_keys
                },
            },
        )

    def __hash__(self) -> int:
        return hash(id(self))


# BACKCOMPAT


def get_backcompat_asset_condition_evaluation_info(
    asset_key: AssetKey,
    latest_evaluation: "AssetConditionEvaluationResult",
    latest_storage_id: Optional[int],
    latest_timestamp: Optional[float],
    handled_root_subset: Optional[AssetSubset],
) -> "AssetConditionEvaluationInfo":
    """Generates an AssetDaemonCursor from information available on the old cursor format."""
    from dagster._core.definitions.asset_condition import (
        AssetConditionEvaluationInfo,
        RuleCondition,
    )
    from dagster._core.definitions.auto_materialize_rule import MaterializeOnMissingRule

    return AssetConditionEvaluationInfo(
        asset_key=asset_key,
        evaluation_result=latest_evaluation,
        timestamp=latest_timestamp,
        max_storage_id=latest_storage_id,
        # the only information we need to preserve from the previous cursor is the handled subset
        extra_state_by_unique_id={
            RuleCondition(MaterializeOnMissingRule()).unique_id: handled_root_subset,
        }
        if handled_root_subset and handled_root_subset.size > 0
        else {},
    )


def backcompat_deserialize_asset_daemon_cursor_str(
    cursor_str: str, asset_graph: Optional[AssetGraph], default_evaluation_id: int
) -> AssetDaemonCursor:
    """This serves as a backcompat layer for deserializing the old cursor format. Some information
    is impossible to fully recover, this will recover enough to continue operating as normal.
    """
    from .asset_condition import AssetConditionEvaluationResult, AssetConditionSnapshot
    from .auto_materialize_rule_evaluation import (
        deserialize_auto_materialize_asset_evaluation_to_asset_condition_evaluation_with_run_ids,
    )

    data = json.loads(cursor_str)

    if isinstance(data, list):
        evaluation_id = data[0] if isinstance(data[0], int) else default_evaluation_id
        return AssetDaemonCursor.empty(evaluation_id)
    elif not isinstance(data, dict):
        return AssetDaemonCursor.empty(default_evaluation_id)

    serialized_last_observe_request_timestamp_by_asset_key = data.get(
        "last_observe_request_timestamp_by_asset_key", {}
    )
    last_observe_request_timestamp_by_asset_key = {
        AssetKey.from_user_string(key_str): timestamp
        for key_str, timestamp in serialized_last_observe_request_timestamp_by_asset_key.items()
    }

    partition_subsets_by_asset_key = {}
    for key_str, serialized_str in data.get("handled_root_partitions_by_asset_key", {}).items():
        asset_key = AssetKey.from_user_string(key_str)
        partitions_def = asset_graph.get_partitions_def(asset_key) if asset_graph else None
        if not partitions_def:
            continue
        try:
            partition_subsets_by_asset_key[asset_key] = partitions_def.deserialize_subset(
                serialized_str
            )
        except:
            continue

    handled_root_asset_graph_subset = AssetGraphSubset(
        non_partitioned_asset_keys={
            AssetKey.from_user_string(key_str)
            for key_str in data.get("handled_root_asset_keys", set())
        },
        partitions_subsets_by_asset_key=partition_subsets_by_asset_key,
    )

    serialized_latest_evaluation_by_asset_key = data.get("latest_evaluation_by_asset_key", {})
    latest_evaluation_by_asset_key = {}
    for key_str, serialized_evaluation in serialized_latest_evaluation_by_asset_key.items():
        key = AssetKey.from_user_string(key_str)
        partitions_def = asset_graph.get_partitions_def(key) if asset_graph else None

        evaluation = deserialize_auto_materialize_asset_evaluation_to_asset_condition_evaluation_with_run_ids(
            serialized_evaluation, partitions_def
        ).evaluation

        latest_evaluation_by_asset_key[key] = evaluation

    previous_evaluation_info = []
    cursor_keys = (
        asset_graph.auto_materialize_policies_by_key.keys()
        if asset_graph
        else latest_evaluation_by_asset_key.keys()
    )
    for asset_key in cursor_keys:
        latest_evaluation_result = latest_evaluation_by_asset_key.get(asset_key)
        # create a placeholder evaluation result if we don't have one
        if not latest_evaluation_result:
            partitions_def = asset_graph.get_partitions_def(asset_key) if asset_graph else None
            latest_evaluation_result = AssetConditionEvaluationResult(
                condition_snapshot=AssetConditionSnapshot("", "", ""),
                true_subset=AssetSubset.empty(asset_key, partitions_def),
                candidate_subset=AssetSubset.empty(asset_key, partitions_def),
                start_timestamp=None,
                end_timestamp=None,
                subsets_with_metadata=[],
                child_evaluations=[],
            )
        backcompat_evaluation_info = get_backcompat_asset_condition_evaluation_info(
            asset_key,
            latest_evaluation_result,
            data.get("latest_storage_id"),
            data.get("latest_evaluation_timestamp"),
            handled_root_asset_graph_subset.get_asset_subset(asset_key, asset_graph)
            if asset_graph
            else None,
        )
        previous_evaluation_info.append(backcompat_evaluation_info)

    return AssetDaemonCursor(
        evaluation_id=default_evaluation_id,
        previous_evaluation_info=previous_evaluation_info,
        last_observe_request_timestamp_by_asset_key=last_observe_request_timestamp_by_asset_key,
    )
