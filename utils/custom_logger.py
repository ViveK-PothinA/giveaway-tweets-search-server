# Custom Logger Using Loguru

import logging
from sys import stdout, stderr
from os import getenv
from pathlib import Path
from loguru import logger


class InterceptHandler(logging.Handler):
    loglevel_mapping = {
        50: "CRITICAL",
        40: "ERROR",
        30: "WARNING",
        20: "INFO",
        10: "DEBUG",
        0: "NOTSET",
    }

    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except AttributeError:
            level = self.loglevel_mapping[record.levelno]

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        log = logger.bind(request_id="app")
        log.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


class CustomizeLogger:
    @classmethod
    def make_logger(cls, debug: bool, file_name: str):

        logger = cls.customize_logging(
            file_name,
            level="DEBUG" if debug else "INFO",
            retention=getenv("LOG_RETENTION", "20 days"),
            rotation=getenv('LOG_ROTATION', "1 months"),
            format=getenv('LOG_FORMAT',"<level>{level: <8}</level> <green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> request id: {extra[request_id]} - <cyan>{name}</cyan>:<cyan>{function}</cyan> - <level>{message}</level>"),
        )
        return logger

    @classmethod
    def customize_logging(
        cls, file_name: Path, level: str, rotation: str, retention: str, format: str
    ):

        logger.remove()
        logger.add(
            stdout, enqueue=True, backtrace=True, level=level.upper(), format=format
        )
        logger.add(
            str(file_name),
            rotation=rotation,
            retention=retention,
            enqueue=True,
            backtrace=True,
            level=level.upper(),
            format=format,
        )
        logging.basicConfig(handlers=[InterceptHandler()], level=0)
        logging.getLogger("uvicorn.access").handlers = [InterceptHandler()]
        for _log in ["uvicorn", "uvicorn.error", "fastapi"]:
            _logger = logging.getLogger(_log)
            _logger.handlers = []
            _logger.propagate = True

        return logger.bind(request_id=None, method=None)
