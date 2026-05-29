class Node:
  def __init__(self, p: int | None, m: dict, c: int) -> None:
    self.__p: int | None = p
    self.__m: dict = m
    self.__c: int = c

  @property
  def parent(self) -> int | None:
    return self.__p
  
  @property
  def metadata(self) -> dict:
    return self.__m
  
  @property
  def child(self) -> int:
    return self.__c
  
  # Returns whether the node is "active" or "deleted"
  @property
  def status(self) -> str:
    return self.__m["status"]

  def __call__(self) -> tuple[int | None, dict, int]:
    return (self.__p, self.__m, self.__c)
  
  def __str__(self) -> str:
    return f"Node(p={str(self.__p)},m={str(self.__m)},c={str(self.__c)})"
  
  def __eq__(self, other) -> bool:
    if not isinstance(other, self.__class__):
      return False
    
    # Defined in order to be able to handle nested structures
    self_m = tuple(sorted(self.__m.items()))
    other_m = tuple(sorted(other.__m.items()))

    return (self.__p == other.__p) and (self_m == other_m) and (self.__c == other.__c)

  def __ne__(self, other) -> bool:
    return not self.__eq__(other)
  
  def __lt__(self, other) -> bool:
    if not isinstance(other, self.__class__):
      return False

    return self.__c < other.__c
  
  def __le__(self, other) -> bool:
    if not isinstance(other, self.__class__):
      return False

    return self.__c <= other.__c
  
  def __gt__(self, other):
    if not isinstance(other, self.__class__):
      return False

    return self.__c > other.__c
  
  def __ge__(self, other):
    if not isinstance(other, self.__class__):
      return False

    return self.__c >= other.__c

  def __hash__(self) -> int:
    def _make_hashable(obj):
      if isinstance(obj, dict):
        return tuple(sorted((k, _make_hashable(v)) for k, v in obj.items()))
      elif isinstance(obj, (list, tuple)):
        return tuple(_make_hashable(item) for item in obj)
      else:
        return obj
    
    hashable_metadata = _make_hashable(self.__m)
    return hash((self.__p, hashable_metadata, self.__c))