class BankAccount:
    def __init__(self, balance=0):
        self.balance = balance
        self.operations = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount):
        self.balance += amount
        return f"Depositadp: {amount}, Nuevo balance: {self.balance}"

    def withdraw(self, amount):
        if amount > self.balance:
            return f"Fondos insuficiente. Balance: {self.balance}"
        self.balance -= amount
        return f"Retirado: {amount}, Nuevo balance: {self.balance}"

    def get_balance(self):
        return f"Balance actual: {self.balance}"

    def process(self, operation, *args):
        return self.operations[operation](*args)
