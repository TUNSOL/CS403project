from . import Clock

class LamportClock(Clock):
    # constructor  
    def __init__(self, id: int):
        self.__id = id
        self.__timestamp = 0

    # getterrss
    @property
    def id(self) -> int:
        return self.__id

    @property
    def timestamp(self) -> int:
        return self.__timestamp

    # if received == None --> local update
    def update(self, received: int) -> None:
        self.__timestamp = max(self.__timestamp, received) + 1

    
    def __str__(self) -> str:
        return f"LamportClock(id={self.__id}, timestamp={self.__timestamp})"