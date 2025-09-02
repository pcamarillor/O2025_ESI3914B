class zeroGuard:

    def __init__(self, x ) -> None:
        self.x = x
    def __eq__(self, value: object) -> bool:
        return self.x == object.__dict__["x"]

    def __ne__(self, value:object ) -> bool:
        return self.x != object.__dict__["x"]

    def __get__(self):
        return self.x
    
    def __add__(self, q):

        return self.x + q

    def __sub__(self, q:int):

        return self.x - q

    def __iadd__(self, q):
        if q > 0:
            self.x += q

        return self

    def __isub__(self, q):
        if q <= self:
            self.x -= q 
        return self

class BankAccount():
    """docstring for ."""

    def __init__(self, id):
        self.id = id
        self.store = zeroGuard(0)

    def process(self, arg, q):
        funcs = {
            "deposit": (lambda x: self.store += x),
            "withdraw": (lambda x: self.store -= x)
        }
        return funcs[arg](q)
