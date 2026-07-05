from enum import Enum

class PersonRole(Enum):
    UPLOADER = 1
    AUTHOR = 2
    OWNER = 3
    OTHER = 4
    UNKNOWN = 5