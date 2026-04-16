from enum import Enum

class DownloadResult(Enum):
    SUCCEEDED = 1
    FAILED_SERVER_UNRESPONSIVE = 2
    FAILED_LOGIN_REQUIRED = 3
    FAILED_TOO_LARGE = 4