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
        return self.balance

    def withdraw(self, amount):
        self.balance -= amount
        return self.balance

    def get_balance(self):
        return self.balance

    def process(self, operation_name, amount=None):
        if amount is not None:
            return self.operations[operation_name](amount)
        else:
            return self.operations[operation_name]()
