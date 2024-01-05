import functools
import hashlib
from abc import ABC, abstractmethod, abstractproperty
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    FrozenSet,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import pendulum

import dagster._check as check
from dagster._core.definitions.events import AssetKey
from dagster._core.definitions.metadata import MetadataMapping, MetadataValue
from dagster._core.definitions.partition import AllPartitionsSubset
from dagster._serdes.serdes import (
    FieldSerializer,
    PackableValue,
    UnpackContext,
    WhitelistMap,
    pack_value,
    unpack_value,
    whitelist_for_serdes,
)
from dagster._utils.caching_instance_queryer import CachingInstanceQueryer

from .asset_subset import AssetSubset

if TYPE_CHECKING:
    from dagster._core.definitions.asset_graph import AssetGraph

    from .asset_condition_evaluation_context import AssetConditionEvaluationContext
    from .auto_materialize_rule import AutoMaterializeRule


T = TypeVar("T")


@whitelist_for_serdes
class HistoricalAllPartitionsSubset(NamedTuple):
    """Serializable indicator that this value was an AllPartitionsSubset at serialization time, but
    the partitions may have changed since that time.
    """


@whitelist_for_serdes
class AssetConditionSnapshot(NamedTuple):
    """A serializable snapshot of a node in the AutomationCondition tree."""

    class_name: str
    description: str
    unique_id: str


@whitelist_for_serdes
class AssetSubsetWithMetadata(NamedTuple):
    """An asset subset with metadata that corresponds to it."""

    subset: AssetSubset
    metadata: MetadataMapping

    @property
    def frozen_metadata(self) -> FrozenSet[Tuple[str, MetadataValue]]:
        return frozenset(self.metadata.items())


def get_serializable_candidate_subset(
    candidate_subset: Union[AssetSubset, HistoricalAllPartitionsSubset],
) -> Union[AssetSubset, HistoricalAllPartitionsSubset]:
    """Do not serialize the candidate subset directly if it is an AllPartitionsSubset."""
    if isinstance(candidate_subset, AssetSubset) and isinstance(
        candidate_subset.value, AllPartitionsSubset
    ):
        return HistoricalAllPartitionsSubset()
    return candidate_subset


class CandidateSubsetSerializer(FieldSerializer):
    def pack(
        self,
        candidate_subset: AssetSubset,
        whitelist_map: WhitelistMap,
        descent_path: str,
    ) -> Optional[Mapping[str, Any]]:
        # On all ticks, the root condition starts with an AllPartitionsSubset as the candidate
        # subset. This would be wasteful to calculate and serialize in its entirety, so we instead
        # store this as `None` and reconstruct it as needed.
        # This does mean that if new partitions are added between serialization time and read time,
        # the candidate subset will contain those new partitions.
        return pack_value(
            get_serializable_candidate_subset(candidate_subset), whitelist_map, descent_path
        )

    def unpack(
        self,
        serialized_candidate_subset: Optional[Mapping[str, Any]],
        whitelist_map: WhitelistMap,
        context: UnpackContext,
    ) -> Union[AssetSubset, HistoricalAllPartitionsSubset]:
        return unpack_value(
            serialized_candidate_subset,
            (AssetSubset, HistoricalAllPartitionsSubset),
            whitelist_map,
            context,
        )


@whitelist_for_serdes(field_serializers={"candidate_subset": CandidateSubsetSerializer})
class AssetConditionEvaluationResult(NamedTuple):
    """Internal representation of the results of evaluating a node in the evaluation tree."""

    condition_snapshot: AssetConditionSnapshot
    true_subset: AssetSubset
    candidate_subset: Union[AssetSubset, HistoricalAllPartitionsSubset]
    start_timestamp: Optional[float]
    end_timestamp: Optional[float]
    subsets_with_metadata: Sequence[AssetSubsetWithMetadata] = []
    child_evaluations: Sequence["AssetConditionEvaluationResult"] = []

    @property
    def asset_key(self) -> AssetKey:
        return self.true_subset.asset_key

    def get_candidate_subset(
        self, asset_graph: "AssetGraph", instance_queryer: CachingInstanceQueryer
    ) -> AssetSubset:
        """We do not store the candidate subset in the serialized representation if it is an
        AllPartitionsSubset, so we need to create a new AllPartitionsSubset here.
        """
        if isinstance(self.candidate_subset, HistoricalAllPartitionsSubset):
            return AssetSubset.all(
                self.asset_key,
                asset_graph.get_partitions_def(self.asset_key),
                instance_queryer,
                instance_queryer.evaluation_time,
            )
        return self.candidate_subset

    def equivalent_to_stored_evaluation(
        self, other: Optional["AssetConditionEvaluationResult"]
    ) -> bool:
        """Returns if all fields other than `run_ids` are equal."""
        return (
            other is not None
            and self.condition_snapshot == other.condition_snapshot
            and self.true_subset == other.true_subset
            # the candidate subset gets modified during serialization
            and get_serializable_candidate_subset(self.candidate_subset)
            == get_serializable_candidate_subset(other.candidate_subset)
            and self.subsets_with_metadata == other.subsets_with_metadata
            and len(self.child_evaluations) == len(other.child_evaluations)
            and all(
                self_child.equivalent_to_stored_evaluation(other_child)
                for self_child, other_child in zip(self.child_evaluations, other.child_evaluations)
            )
        )

    def discarded_subset(self, condition: "AssetCondition") -> Optional[AssetSubset]:
        """Returns the AssetSubset representing asset partitions that were discarded during this
        evaluation. Note that 'discarding' is a deprecated concept that is only used for backwards
        compatibility.
        """
        not_discard_condition = condition.not_discard_condition
        if not not_discard_condition or len(self.child_evaluations) != 3:
            return None

        not_discard_evaluation = self.child_evaluations[2]
        discard_evaluation = not_discard_evaluation.child_evaluations[0]
        return discard_evaluation.true_subset

    def get_requested_or_discarded_subset(self, condition: "AssetCondition") -> AssetSubset:
        discarded_subset = self.discarded_subset(condition)
        if discarded_subset is None:
            return self.true_subset
        else:
            return self.true_subset | discarded_subset

    def for_child(
        self, child_condition: "AssetCondition"
    ) -> Optional["AssetConditionEvaluationResult"]:
        """Returns the evaluation of a given child condition by finding the child evaluation that
        has an identical hash to the given condition.
        """
        child_unique_id = child_condition.snapshot.unique_id
        for child_evaluation in self.child_evaluations:
            if child_evaluation.condition_snapshot.unique_id == child_unique_id:
                return child_evaluation

        return None

    def with_run_ids(self, run_ids: AbstractSet[str]) -> "AssetConditionEvaluationWithRunIds":
        return AssetConditionEvaluationWithRunIds(evaluation=self, run_ids=frozenset(run_ids))


@whitelist_for_serdes
class AssetConditionEvaluationInfo(NamedTuple):
    """Return value for the evaluate method of an AssetCondition."""

    asset_key: AssetKey
    evaluation_result: AssetConditionEvaluationResult

    max_storage_id: Optional[int]
    timestamp: Optional[float]
    extra_state_by_unique_id: Mapping[str, PackableValue]

    @property
    def true_subset(self) -> AssetSubset:
        return self.evaluation_result.true_subset

    @staticmethod
    def create_from_children(
        context: "AssetConditionEvaluationContext",
        true_subset: AssetSubset,
        child_results: Sequence["AssetConditionEvaluationInfo"],
    ) -> "AssetConditionEvaluationInfo":
        """Returns a new AssetConditionEvaluationResult from the given child results."""
        return AssetConditionEvaluationInfo(
            asset_key=context.asset_key,
            evaluation_result=AssetConditionEvaluationResult(
                condition_snapshot=context.condition.snapshot,
                true_subset=true_subset,
                candidate_subset=context.candidate_subset,
                start_timestamp=context.start_timestamp,
                end_timestamp=pendulum.now("UTC").timestamp(),
                subsets_with_metadata=[],
                child_evaluations=[
                    child_result.evaluation_result for child_result in child_results
                ],
            ),
            extra_state_by_unique_id=dict(
                item
                for child_result in child_results
                for item in child_result.extra_state_by_unique_id.items()
            ),
            max_storage_id=context.new_max_storage_id,
            timestamp=context.evaluation_time.timestamp(),
        )

    @staticmethod
    def create(
        context: "AssetConditionEvaluationContext",
        true_subset: AssetSubset,
        subsets_with_metadata: Sequence[AssetSubsetWithMetadata] = [],
        extra_state: PackableValue = None,
    ) -> "AssetConditionEvaluationInfo":
        """Returns a new AssetConditionEvaluationResult from the given parameters."""
        return AssetConditionEvaluationInfo(
            asset_key=context.asset_key,
            evaluation_result=AssetConditionEvaluationResult(
                context.condition.snapshot,
                true_subset=true_subset,
                start_timestamp=context.start_timestamp,
                end_timestamp=pendulum.now("UTC").timestamp(),
                candidate_subset=context.candidate_subset,
                subsets_with_metadata=subsets_with_metadata,
            ),
            extra_state_by_unique_id={context.condition.unique_id: extra_state}
            if extra_state
            else {},
            max_storage_id=context.new_max_storage_id,
            timestamp=context.evaluation_time.timestamp(),
        )

    def get_extra_state(self, condition: "AssetCondition", as_type: Type[T]) -> Optional[T]:
        """Returns the value from the extras dict for the given condition, if it exists and is of
        the expected type. Otherwise, returns None.
        """
        extra_state = self.extra_state_by_unique_id.get(condition.unique_id)
        if isinstance(extra_state, as_type):
            return extra_state
        return None


@whitelist_for_serdes
class AssetConditionEvaluationWithRunIds(NamedTuple):
    """A union of an AssetConditionEvaluation and the set of run IDs that have been launched in
    response to it.
    """

    evaluation: AssetConditionEvaluationResult
    run_ids: FrozenSet[str]

    @property
    def asset_key(self) -> AssetKey:
        return self.evaluation.asset_key

    @property
    def num_requested(self) -> int:
        return self.evaluation.true_subset.size


class AssetCondition(ABC):
    """An AutomationCondition represents some state of the world that can influence if an asset
    partition should be materialized or not. AutomationConditions can be combined together to create
    new conditions using the `&` (and), `|` (or), and `~` (not) operators.
    """

    @property
    def unique_id(self) -> str:
        parts = [
            self.__class__.__name__,
            *[child.unique_id for child in self.children],
        ]
        return hashlib.md5("".join(parts).encode()).hexdigest()

    @abstractmethod
    def evaluate(self, context: "AssetConditionEvaluationContext") -> AssetConditionEvaluationInfo:
        raise NotImplementedError()

    @abstractproperty
    def description(self) -> str:
        raise NotImplementedError()

    def __and__(self, other: "AssetCondition") -> "AssetCondition":
        # group AndAutomationConditions together
        if isinstance(self, AndAssetCondition):
            return AndAssetCondition(children=[*self.children, other])
        return AndAssetCondition(children=[self, other])

    def __or__(self, other: "AssetCondition") -> "AssetCondition":
        # group OrAutomationConditions together
        if isinstance(self, OrAssetCondition):
            return OrAssetCondition(children=[*self.children, other])
        return OrAssetCondition(children=[self, other])

    def __invert__(self) -> "AssetCondition":
        return NotAssetCondition(children=[self])

    @property
    def is_legacy(self) -> bool:
        """Returns if this condition is in the legacy format. This is used to determine if we can
        do certain types of backwards-compatible operations on it.
        """
        return (
            isinstance(self, AndAssetCondition)
            and len(self.children) in {2, 3}
            and isinstance(self.children[0], OrAssetCondition)
            and isinstance(self.children[1], NotAssetCondition)
            # the third child is the discard condition, which is optional
            and (len(self.children) == 2 or isinstance(self.children[2], NotAssetCondition))
        )

    @property
    def children(self) -> Sequence["AssetCondition"]:
        return []

    @property
    def not_discard_condition(self) -> Optional["AssetCondition"]:
        if not self.is_legacy or not len(self.children) == 3:
            return None
        return self.children[-1]

    @functools.cached_property
    def snapshot(self) -> AssetConditionSnapshot:
        """Returns a snapshot of this condition that can be used for serialization."""
        return AssetConditionSnapshot(
            class_name=self.__class__.__name__,
            description=self.description,
            unique_id=self.unique_id,
        )


class RuleCondition(
    NamedTuple("_RuleCondition", [("rule", "AutoMaterializeRule")]),
    AssetCondition,
):
    """This class represents the condition that a particular AutoMaterializeRule is satisfied."""

    @property
    def unique_id(self) -> str:
        parts = [self.rule.__class__.__name__, self.description]
        return hashlib.md5("".join(parts).encode()).hexdigest()

    @property
    def description(self) -> str:
        return self.rule.description

    def evaluate(self, context: "AssetConditionEvaluationContext") -> AssetConditionEvaluationInfo:
        context.root_context.daemon_context._verbose_log_fn(  # noqa
            f"Evaluating rule: {self.rule.to_snapshot()}"
        )
        evaluation_result = self.rule.evaluate_for_asset(context)
        context.root_context.daemon_context._verbose_log_fn(  # noqa
            f"Rule returned {evaluation_result.true_subset.size} partitions:"
            f"{evaluation_result.true_subset}"
        )
        return evaluation_result


class AndAssetCondition(
    NamedTuple("_AndAssetCondition", [("children", Sequence[AssetCondition])]),
    AssetCondition,
):
    """This class represents the condition that all of its children evaluate to true."""

    @property
    def description(self) -> str:
        return "All of"

    def evaluate(self, context: "AssetConditionEvaluationContext") -> AssetConditionEvaluationInfo:
        child_results: List[AssetConditionEvaluationInfo] = []
        true_subset = context.candidate_subset
        for child in self.children:
            child_context = context.for_child(condition=child, candidate_subset=true_subset)
            child_result = child.evaluate(child_context)
            child_results.append(child_result)
            true_subset &= child_result.true_subset
        return AssetConditionEvaluationInfo.create_from_children(
            context, true_subset, child_results
        )


class OrAssetCondition(
    NamedTuple("_OrAssetCondition", [("children", Sequence[AssetCondition])]),
    AssetCondition,
):
    """This class represents the condition that any of its children evaluate to true."""

    @property
    def description(self) -> str:
        return "Any of"

    def evaluate(self, context: "AssetConditionEvaluationContext") -> AssetConditionEvaluationInfo:
        child_results: List[AssetConditionEvaluationInfo] = []
        true_subset = context.empty_subset()
        for child in self.children:
            child_context = context.for_child(
                condition=child, candidate_subset=context.candidate_subset
            )
            child_result = child.evaluate(child_context)
            child_results.append(child_result)
            true_subset |= child_result.true_subset
        return AssetConditionEvaluationInfo.create_from_children(
            context, true_subset, child_results
        )


class NotAssetCondition(
    NamedTuple("_NotAssetCondition", [("children", Sequence[AssetCondition])]),
    AssetCondition,
):
    """This class represents the condition that none of its children evaluate to true."""

    def __new__(cls, children: Sequence[AssetCondition]):
        check.invariant(len(children) == 1)
        return super().__new__(cls, children)

    @property
    def description(self) -> str:
        return "Not"

    @property
    def child(self) -> AssetCondition:
        return self.children[0]

    def evaluate(self, context: "AssetConditionEvaluationContext") -> AssetConditionEvaluationInfo:
        child_context = context.for_child(
            condition=self.child, candidate_subset=context.candidate_subset
        )
        child_result = self.child.evaluate(child_context)
        true_subset = context.candidate_subset - child_result.true_subset

        return AssetConditionEvaluationInfo.create_from_children(
            context, true_subset, [child_result]
        )
