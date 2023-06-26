from dataclasses import dataclass
from typing import Dict


@dataclass(frozen=True)
class LoggingSettings:
    MAIN_LOG: str = "info.log"
    ERROR_LOG: str = "error.log"
    LEVEL: str = "INFO"

    @classmethod
    def get_setting_dict(self, log_level) -> Dict:
        self.LEVEL = log_level
        return {
            # LOGGING_CONFIG = {
            "version": 1,
            "disable_existing_loggers": False,
            "loggers": {
                "": {
                    "level": "WARNING",
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
                "__main__": {
                    "level": self.LEVEL,
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
                # modules
                "config": {
                    "level": self.LEVEL,
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
                "module": {
                    "level": self.LEVEL,
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
                "io": {
                    "level": self.LEVEL,
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
                # site-packages
                "beam_mysql": {  # Suppress info log
                    "level": "WARNING",
                    "handlers": [
                        "debug_console_handler",
                        "info_rotating_file_handler",
                        "error_file_handler",
                    ],
                    "propagate": False,
                },
            },
            "handlers": {
                "debug_console_handler": {
                    "level": "DEBUG",
                    "formatter": "simple",
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stdout",
                },
                "info_rotating_file_handler": {
                    "level": "INFO",
                    "formatter": "long",
                    "class": "logging.handlers.RotatingFileHandler",
                    "filename": self.MAIN_LOG,
                    "mode": "a",
                    "maxBytes": (1024 * 1024 * 1),  # 1MB
                    "backupCount": 10,  # 10rotations
                },
                "error_file_handler": {
                    "level": "ERROR",
                    "formatter": "long",
                    "class": "logging.FileHandler",
                    "filename": self.ERROR_LOG,
                    "mode": "a",
                },
            },
            "formatters": {
                "simple": {
                    "format": "[%(levelname)s] %(filename)s:%(lineno)d - %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
                "long": {
                    "format": "[%(asctime)s] [%(levelname)s] %(name)s:%(filename)s:%(lineno)d - %(message)s",
                    "datefmt": "%Y-%m-%d %H:%M:%S",
                },
            },
        }
