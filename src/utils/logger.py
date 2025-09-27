import logging

class Logger:
    """
    Logger class
    """
    DEFAULT_FORMAT = "%(asctime)s - %(levelname)s - [%(name)s] - %(filename)s:%(lineno)d - %(message)s"
    DEFAULT_DATEFMT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, class_name: str, level: int = logging.INFO):
        self.class_name = class_name 
        self.level = level
        self.fmt = self.DEFAULT_FORMAT

        self._logger = logging.getLogger(self.class_name)

        if self._logger.handlers:
            self._logger.setLevel(self.level)
            return

        self._logger.setLevel(self.level)
        formatter = logging.Formatter(self.fmt, datefmt=self.DEFAULT_DATEFMT)

        sh = logging.StreamHandler()
        sh.setLevel(self.level)
        sh.setFormatter(formatter)
        self._logger.addHandler(sh)

    def get_logger(self) -> logging.Logger:
        return self._logger

    def debug(self, msg: str, *args, **kwargs) -> None:
        self._logger.debug(msg, *args, **kwargs)

    def info(self, msg: str, *args, **kwargs) -> None:
        self._logger.info(msg, *args, **kwargs)

    def warning(self, msg: str, *args, **kwargs) -> None:
        self._logger.warning(msg, *args, **kwargs)

    def error(self, msg: str, *args, **kwargs) -> None:
        self._logger.error(msg, *args, **kwargs)

    def exception(self, msg: str, *args, **kwargs) -> None:
        self._logger.exception(msg, *args, **kwargs)