class BankAccount:
    def __init__(self, balance):
        self.balance = balance
        self.operations = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount):
        self.balance += amount
        return f"Deposited {amount}. New balance: {self.balance}"

    def withdraw(self, amount):
        if amount > self.balance:
            return "Insufficient funds"
        self.balance -= amount
        return f"Withdrew {amount}. New balance: {self.balance}"

    def get_balance(self):
        return f"Current balance: {self.balance}"

    def process(self, operation, amount=None):
        if operation in self.operations:
            if amount is not None:
                return self.operations[operation](amount)
            return self.operations[operation]()
        return "Invalid operation"
