
class BankAccount():
    _counter = 0

    def __init__(self, balance: int = 0):
        BankAccount._counter += 1
        self._id = BankAccount._counter
        self._balance = balance
        self.processes = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount: int):
        if amount <= 0:
            raise InvalidAmountError("Deposit amount must be positive.")
        self._balance += amount
        return self._balance

    
    def withdraw(self, amount: int):
        if amount <= 0:
            raise InvalidAmountError("Withdraw amount must be positive.")
        if amount > self._balance:
            raise InsufficientFundsError("Insufficient funds.")
        self._balance -= amount
        return self._balance

    def get_balance(self):
        return self._balance
    
    def process(self, option: str, amount: int | None = None):
        method = option.strip().lower()
        if method not in self.processes:
            raise BankAccountError(f"Unknown operation '{option}'.")

        to_run = self.processes[method]
        if method == "get_balance":
            return to_run()
        return to_run(amount)

    def __str__(self):
        return f"Bank account balance with id: {self._id}\n Current balance: ${float(self._balance)}"
        

class BankAccountError(Exception):
    """Base class for all BankAccount errors."""

class InvalidAmountError(BankAccountError):
    """Raised when amount is <= 0."""

class InsufficientFundsError(BankAccountError):
    """Raised when withdrawal exceeds balance."""

