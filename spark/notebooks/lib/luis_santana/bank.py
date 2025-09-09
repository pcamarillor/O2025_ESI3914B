# Problem Statement
# Create a BankAccount class and add it in your own module 1 that uses a
# dictionary to store operations (functions) as values and their names (e.g.,
# "deposit", "withdraw", and "get balance") as keys. Dynamically select and
# execute operations based on user input.
# Example Usage
# account = BankAccount(1000)
# account.process("deposit", 13)
# account.process("withdraw", 717)

class BankAccount:
    def __init__(self, initial_balance=0):
        if(initial_balance < 0):
            raise ValueError("Initial balance canot be negative")
        self.balance = initial_balance
        self.dict = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount):
        if (amount > 0):
            self.balance += amount
            return f"Deposited: {amount}, New Balanec: {self.balance}"
        else:
            raise ValueError("Deposit amount must be positive")

    def withdraw(self, amount):
        if(amount > self.balance):
            raise ValueError("Insufficient funds")
        if (amount <= 0):
            raise ValueError("Withdrawal amount must be positive")
        self.balance -= amount
        return f"Withdrew: {amount}, New Balanec: {self.balance}"

    def get_balance(self):
        return self.balance
    
    def process(self, operation, amount=0):        
        if(operation in self.dict):
            if(operation) == "get_balance":
                return self.dict[operation]()
            else:
                return self.dict[operation](amount)
        else:
            raise ValueError("Invalid operation")
    
    def __repr__(self):
        return f"BankAccount(balance={self.balance})"