class MovePayload:
  def __init__(self, i: int, t: dict[int, int], p: int | None, m: dict, c: int) -> None:
    self.__i: int = i
    self.__t: dict[int, int] = t
    self.__p: int | None = p
    self.__m: dict = m
    self.__c: int = c

  @property
  def id(self) -> int:
    return self.__i
  
  @property
  def timestamp(self) -> dict[int, int]:
    return self.__t
  
  @property
  def parent(self) -> int | None:
    return self.__p
  
  @property
  def metadata(self) -> dict:
    return self.__m
  
  @property
  def child(self) -> int:
    return self.__c

  def __call__(self) -> tuple[int | None, dict, int]:
    return self.__p, self.__m, self.__c
  
  def __str__(self) -> str:
    return f"{self.__i},({self.__t},{self.__p},{self.__m},{self.__c})"