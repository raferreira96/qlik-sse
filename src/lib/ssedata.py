from enum import Enum

class ArgType(Enum):
    # Tipos de Dados que podem ser usados como Argumentos em diferentes funções de script.
    Undefined = -1
    Empty = 0
    String = 1
    Numeric = 2
    Mixed = 3


class ReturnType(Enum):
    # Tipos de Retorno que podem ser usados em avaliação de script.
    Undefined = -1
    String = 0
    Numeric = 1
    Dual = 2


class FunctionType(Enum):
    # Tipos de Funções
    Scalar = 0
    Aggregation = 1
    Tensor = 2
