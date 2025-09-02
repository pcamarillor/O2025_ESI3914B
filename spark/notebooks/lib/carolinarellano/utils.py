class BankAccount:
    def __init__(self, init_balance=0):
        self.balance = init_balance

    def deposit(self, amount):
        self.balance += amount
        print(f"Deposited: {amount}")
        return self.balance

    def withdraw(self, amount):
        if amount > self.balance:
            raise ValueError("Insufficient funds to withdraw :(")
        self.balance -= amount
        return self.balance

    def get_balance(self):
        if self.balance < 0:
            print("Warning: Your account is overdrawn!")
        print(f"Current balance: {self.balance}")
        return self.balance

    def process(self, operation, amount):
        if operation.lower() == "deposit":
            return self.deposit(amount)
        elif operation.lower() == "withdraw":
            return self.withdraw(amount)
        else:
            raise ValueError(f"Unknown operation: {operation}")