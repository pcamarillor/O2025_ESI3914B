# spark/notebooks/lib/pablo/bank_account.py

class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance

        self.operations = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount: float):
        if amount <= 0:
            raise ValueError("El depósito debe ser mayor que 0")
        self.balance += amount
        return self.balance

    def withdraw(self, amount: float):
        if amount <= 0:
            raise ValueError("El retiro debe ser mayor que 0")
        if amount > self.balance:
            raise ValueError("Fondos insuficientes")
        self.balance -= amount
        return self.balance

    def get_balance(self):
        return self.balance

    def process(self, operation: str, *args):
        """Ejecuta dinámicamente la operación solicitada."""
        if operation not in self.operations:
            raise ValueError(f"Operación '{operation}' no soportada")
        return self.operations[operation](*args)
