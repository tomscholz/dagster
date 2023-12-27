import os
import sys
import traceback
import uuid
from types import TracebackType
from typing import Any, NamedTuple, Optional, Sequence, Tuple, Type, Union

from typing_extensions import TypeAlias

import dagster._check as check
from dagster._core.errors import DagsterMaskedUserCodeError
from dagster._serdes import whitelist_for_serdes


# mypy does not support recursive types, so "cause" has to be typed `Any`
@whitelist_for_serdes
class SerializableErrorInfo(
    NamedTuple(
        "SerializableErrorInfo",
        [
            ("message", str),
            ("stack", Sequence[str]),
            ("cls_name", Optional[str]),
            ("cause", Any),
            ("context", Any),
        ],
    )
):
    # serdes log
    # * added cause - default to None in constructor to allow loading old entries
    # * added context - default to None for similar reasons
    #
    def __new__(
        cls,
        message: str,
        stack: Sequence[str],
        cls_name: Optional[str],
        cause: Optional["SerializableErrorInfo"] = None,
        context: Optional["SerializableErrorInfo"] = None,
    ):
        return super().__new__(cls, message, stack, cls_name, cause, context)

    def __str__(self) -> str:
        return self.to_string()

    def to_string(self) -> str:
        stack_str = "\nStack Trace:\n" + "".join(self.stack) if self.stack else ""
        cause_str = (
            "\nThe above exception was caused by the following exception:\n"
            + self.cause.to_string()
            if self.cause
            else ""
        )
        context_str = (
            "\nThe above exception occurred during handling of the following exception:\n"
            + self.context.to_string()
            if self.context
            else ""
        )

        return f"{self.message}{stack_str}{cause_str}{context_str}"

    def to_exception_message_only(self) -> "SerializableErrorInfo":
        """Return a new SerializableErrorInfo with only the message and cause set.

        This is done in cases when the context about the error should not be exposed to the user.
        """
        return SerializableErrorInfo(message=self.message, stack=[], cls_name=self.cls_name)


def _serializable_error_info_from_tb(
    tb: traceback.TracebackException, no_cause: bool = False
) -> SerializableErrorInfo:
    return SerializableErrorInfo(
        # usually one entry, multiple lines for SyntaxError
        "".join(list(tb.format_exception_only())),
        tb.stack.format(),
        tb.exc_type.__name__ if tb.exc_type is not None else None,
        _serializable_error_info_from_tb(tb.__cause__) if tb.__cause__ else None,
        _serializable_error_info_from_tb(tb.__context__)
        if tb.__context__ and not no_cause
        else None,
    )


ExceptionInfo: TypeAlias = Union[
    Tuple[Type[BaseException], BaseException, TracebackType],
    Tuple[None, None, None],
]


def _should_mask_user_code_error() -> bool:
    return os.getenv("DAGSTER_MASK_USER_CODE_ERRORS") == "1"


def _get_masked_traceback_exception(with_instruction: bool) -> traceback.TracebackException:
    """Raises a DagsterMaskedUserCodeError and returns the traceback exception for it.
    Used to replace the traceback of a user code error with a generic message that they
    can search for in their own logs.
    """
    error_id = str(uuid.uuid4())
    try:
        raise DagsterMaskedUserCodeError(
            f"Error occurred during user code execution, error ID {error_id}"
            + ("\nSearch in logs for this error ID for more details" if with_instruction else "")
        )
    except DagsterMaskedUserCodeError:
        # Get the stack trace from the masking exception we just raised, instead of
        # the underlying user code exception
        exc_type, e, tb = sys.exc_info()
        tb_exc = traceback.TracebackException(
            check.not_none(exc_type), check.not_none(e), check.not_none(tb)
        )
        return tb_exc


def serializable_error_info_from_exc_info(
    exc_info: ExceptionInfo,
    # Whether to forward serialized errors thrown from subprocesses
    hoist_user_code_error: Optional[bool] = True,
    hoist_user_code_execution_error: Optional[bool] = False,
) -> SerializableErrorInfo:
    # `sys.exc_info() return Tuple[None, None, None] when there is no exception being processed. We accept this in
    # the type signature here since this function is meant to directly receive the return value of
    # `sys.exc_info`, but the function should never be called when there is no exception to process.
    exc_type, e, tb = exc_info
    additional_message = "sys.exc_info() called but no exception available to process."
    exc_type = check.not_none(exc_type, additional_message=additional_message)
    e = check.not_none(e, additional_message=additional_message)
    tb = check.not_none(tb, additional_message=additional_message)

    from dagster._core.errors import DagsterUserCodeExecutionError, DagsterUserCodeProcessError

    if isinstance(e, DagsterUserCodeExecutionError) and _should_mask_user_code_error():
        masked_tb_exc = _get_masked_traceback_exception(with_instruction=True)
        print(  # noqa: T201
            _serializable_error_info_from_tb(
                _get_masked_traceback_exception(with_instruction=False)
            ).to_string(),
            file=sys.stderr,
        )
        return _serializable_error_info_from_tb(masked_tb_exc, no_cause=True)

    if (
        hoist_user_code_error
        and isinstance(e, DagsterUserCodeProcessError)
        and len(e.user_code_process_error_infos) == 1
    ):
        return e.user_code_process_error_infos[0]
    elif hoist_user_code_execution_error and isinstance(e, DagsterUserCodeExecutionError):
        tb_exc = traceback.TracebackException(type(e.user_exception), e.user_exception, tb.tb_next)
        return _serializable_error_info_from_tb(tb_exc)
    else:
        tb_exc = traceback.TracebackException(exc_type, e, tb)
        return _serializable_error_info_from_tb(tb_exc)
