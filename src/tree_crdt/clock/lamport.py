from . import Clock

class LamportClock(Clock):
    def __init__(self, id: int):
        self.__id = id
        self.__timestamp = 0

    @property
    def id(self) -> int:
        return self.__id

    @property
    def timestamp(self) -> int:
        return self.__timestamp

    def update(self, received: int | None) -> None:
        if received is None:
            self.__timestamp += 1
            return

        self.__timestamp = max(self.__timestamp, received) + 1

    def __str__(self) -> str:
        return str(self.__timestamp)
