class BankAccount:
    def __init__(self, init_balance: float = 0.0) -> None:
        self.balance: float = init_balance

    def deposit(self, amount: float) -> float:
        self._validate_amount(amount, "deposit")
        self.balance += amount
        print(f"Deposited: {amount}")
        return self.balance

    def withdraw(self, amount: float) -> float:
        self._validate_amount(amount, "withdraw")
        if amount > self.balance:
            raise ValueError("Insufficient funds to withdraw.")
        self.balance -= amount
        return self.balance

    def get_balance(self) -> float:
        if self.balance < 0:
            print("Warning: Your account is overdrawn!")
        print(f"Current balance: {self.balance}")
        return self.balance

    def process(self, operation: str, amount: float = None) -> float:
        operation = operation.lower()
        if operation == "deposit":
            return self.deposit(amount)
        elif operation == "withdraw":
            return self.withdraw(amount)
        else:
            raise ValueError(f"Unknown operation: {operation}")

    @staticmethod
    def _validate_amount(amount: float, operation: str) -> None:
        if amount is None:
            raise ValueError(f"Amount is required for {operation} operation.")
        if amount < 0:
            raise ValueError(f"Invalid {operation} amount.")