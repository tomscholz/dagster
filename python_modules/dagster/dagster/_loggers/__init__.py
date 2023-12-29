import logging
import os
from typing import TYPE_CHECKING, Mapping, Optional, Sequence, Tuple

import coloredlogs

from dagster import _seven
from dagster._config import Field
from dagster._core.definitions.logger_definition import LoggerDefinition, logger
from dagster._core.utils import coerce_valid_log_level
from dagster._utils.log import configure_loggers, create_console_logger

if TYPE_CHECKING:
    from dagster._core.execution.context.logger import InitLoggerContext
    from dagster._core.instance import DagsterInstance


@logger(
    Field(
        {
            "log_level": Field(
                str,
                is_required=False,
                default_value="INFO",
                description="The logger's threshold.",
            ),
            "name": Field(
                str,
                is_required=False,
                default_value="dagster",
                description="The name of your logger.",
            ),
        },
        description="The default colored console logger.",
    ),
    description="The default colored console logger.",
)
def colored_console_logger(init_context: "InitLoggerContext") -> logging.Logger:
    """This logger provides support for sending Dagster logs to stdout in a colored format. It is
    included by default on jobs which do not otherwise specify loggers.
    """
    return create_console_logger(
        name=init_context.logger_config["name"],
        level=coerce_valid_log_level(init_context.logger_config["log_level"]),
    )


@logger
def default_logger(init_context: "InitLoggerContext") -> logging.Logger:
    return logging.getLogger("dagster")


@logger(
    Field(
        {
            "log_level": Field(
                str,
                is_required=False,
                default_value="INFO",
                description="The logger's threshold.",
            ),
            "name": Field(
                str,
                is_required=False,
                default_value="dagster",
                description="The name of your logger.",
            ),
        },
        description="A JSON-formatted console logger.",
    ),
    description="A JSON-formatted console logger.",
)
def json_console_logger(init_context: "InitLoggerContext") -> logging.Logger:
    """This logger provides support for sending Dagster logs to stdout in json format.

    Example:
        .. code-block:: python

            from dagster import op, job
            from dagster.loggers import json_console_logger

            @op
            def hello_op(context):
                context.log.info('Hello, world!')
                context.log.error('This is an error')

            @job(logger_defs={'json_logger': json_console_logger})])
            def json_logged_job():
                hello_op()

    """
    level = coerce_valid_log_level(init_context.logger_config["log_level"])
    name = init_context.logger_config["name"]

    klass = logging.getLoggerClass()
    logger_ = klass(name, level=level)

    handler = coloredlogs.StandardErrorHandler()

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            return _seven.json.dumps(record.__dict__)

    handler.setFormatter(JsonFormatter())
    logger_.addHandler(handler)

    return logger_


def default_system_loggers(
    instance: Optional["DagsterInstance"],
) -> Sequence[Tuple["LoggerDefinition", Mapping[str, object]]]:
    """If users don't provide configuration for any loggers, we instantiate these loggers with the
    default config.

    Returns:
        List[Tuple[LoggerDefinition, dict]]: Default loggers and their associated configs.
    """
    log_format = os.getenv("DAGSTER_LOG_FORMAT", "colored")
    log_level = instance.python_log_level if (instance and instance.python_log_level) else "DEBUG"
    if log_format == "colored":
        return [(colored_console_logger, {"name": "dagster", "log_level": log_level})]

    configure_loggers(formatter=log_format, log_level="DEBUG")

    return [(default_logger, {})]


def default_loggers() -> Mapping[str, "LoggerDefinition"]:
    log_format = os.getenv("DAGSTER_LOG_FORMAT", "colored")
    if log_format == "colored":
        return {"console": colored_console_logger}

    configure_loggers(formatter=log_format, log_level="INFO")

    return {"console": default_logger}
