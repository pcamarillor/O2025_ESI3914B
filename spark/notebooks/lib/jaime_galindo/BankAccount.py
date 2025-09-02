class BankAccount:
    def __init__(self, amount):
        self.amount = amount
        self.process = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }


    def deposit(self, amount):
        self.amount += amount

    def withdraw(self, amount):
        self.amount -= amount
    
    def get_balance(self):
        return self.amount
    
