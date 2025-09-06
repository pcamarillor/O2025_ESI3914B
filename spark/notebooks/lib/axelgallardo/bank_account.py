class BankAccount:
    def __init__(self, initial_balance=0):
        self.balance = initial_balance
        
        # Diccionario de operaciones
        self.operations = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance
        }

    def deposit(self, amount):
        if amount <= 0:
            raise ValueError("El depósito debe ser mayor que 0")
        self.balance += amount
        return self.balance

    def withdraw(self, amount):
        if amount <= 0:
            raise ValueError("El retiro debe ser mayor que 0")
        if amount > self.balance:
            raise ValueError("Fondos insuficientes")
        self.balance -= amount
        return self.balance

    def get_balance(self):
        return self.balance

    def process(self, operation_name, *args):
        """
        Busca la operación en el diccionario y la ejecuta.
        *args permite pasar los argumentos dinámicamente.
        """
        if operation_name not in self.operations:
            raise ValueError(f"Operación '{operation_name}' no soportada")
        
        func = self.operations[operation_name]
        return func(*args)
