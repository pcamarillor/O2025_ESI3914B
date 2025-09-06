class BankAccount:
    def __init__(self, init_balance: float = 0.0) -> None:
        self.balance: float = init_balance
        print(f"Account created with initial balance: ${self.balance}")

    def deposit(self, amount: float) -> float:
        self._validate_amount(amount, "deposit")
        self.balance += amount
        print(f"Deposited: {self._format_amount(amount)}")
        return self._format_amount(self.balance)

    def withdraw(self, amount: float) -> float:
        self._validate_amount(amount, "withdraw")
        if amount > self.balance:
            raise ValueError("Insufficient funds to withdraw.")
        self.balance -= amount
        print(f"Withdrew: {self._format_amount(amount)}")
        return self._format_amount(self.balance)

    def get_balance(self) -> float:
        if self.balance < 0:
            print("Warning: Your account is overdrawn!")
        return print(f"Your current balance is: {self._format_amount(self.balance)}")

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

    @staticmethod
    def _format_amount(amount: float) -> str:
        return f"${round(amount, 2)}"