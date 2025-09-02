class BankAccount:
    def __init__(self, amount):
        self.__amount = amount
        self.process = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }


    def deposit(self, amount):
        self.__amount += amount

    def withdraw(self, amount):
        if amount <= self.__amount:
            self.__amount -= amount
        else: print("fondos insuficientes")
    
    def get_balance(self):
        return self.__amount
    
