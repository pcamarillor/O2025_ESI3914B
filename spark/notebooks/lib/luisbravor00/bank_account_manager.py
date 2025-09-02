class BankAccount():
    def __init__(self, balance:float):
        self._balance = balance
        self.operations = {
            "deposit": self.__deposit,
            "withdraw": self.__withdraw,
            "get_balance": self.__get_balance
        }
    
    @property
    def balance(self):
        return self._balance
    
    @balance.setter
    def balance(self, value):
        if value < 0:
            raise ValueError("Your balance can't be negative.")
        self._balance = value
        
    def process(self, operation:str="get_balance", quantity:float = 0.0):
        if quantity == 0.0:
            self.operations[operation]()
        else:
            self.operations[operation](quantity)

    def __deposit(self, quantity:float):
        if quantity < 0.0:
            raise ValueError("Can't deposit negative quantities.")
        self.balance += quantity

    def __withdraw(self, quantity:float):
        current_balance = self.balance

        if current_balance < quantity:
            raise ValueError("Insuficient funds. Try again with another quantity.")
        self.balance -= quantity

    def __get_balance(self):
        print(f"Your account balance: ${self.balance:.2f}")
    
