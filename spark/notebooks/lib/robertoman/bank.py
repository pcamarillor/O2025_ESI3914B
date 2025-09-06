class BankAccount:
    def __init__(self, initial_balance: float = 0.0) -> None:
        # combrobar que sea mayor que 0 ya que no tendria sentido comenzar con un balance negativo
        if initial_balance < 0:
            raise ValueError("Initial balance cannot be negative")
        self._balance: float = initial_balance
        # The class should use a dictionary to store operations (functions)
        # as values and their names (e.g., “deposit”, “withdraw”, “get_balance”) as keys.
        self._operations: dict = {
            "deposit": self.deposit,
            "withdraw": self.withdraw,
            "get_balance": self.get_balance,
        }

    def deposit(self, amount: float) -> float:
        # comporbar si el amount es menor o igual que 0, ya que no puede haber depositos de 0 ni negativos.
        if amount <= 0:
            # hacemos una excepcion
            raise ValueError("The amount of the deposit must be bigger than 0")
        # aumentamos el balance general con el deposito
        self._balance += amount

        print(f"Deposited {amount:.2f}. New balance: {self._balance:.2f}") # prints para ver como funcionan los metodos correctamente
        # devolvemos el balance general con el deposito
        return self._balance

    def withdraw(self, amount: float) -> float:
        if amount <= 0:
            raise ValueError("Withdraw amount must be bigger than 0")
        if amount > self._balance:
            raise ValueError("Insufficient funds")
        # restamos el amount al balance
        self._balance -= amount
        
        print(f"Withdrew {amount:.2f}. New balance: {self._balance:.2f}")
        # devolvemos el balance general con el deposito
        return self._balance

    def get_balance(self) -> float:
        print(f"Current balance: {self._balance:.2f}")
        return self._balance

    def process(self, operation: str, *args) -> float:
        # verificamos que la operacion este dentro de nuestro diccionario
        if operation not in self._operations:
            raise ValueError(f"The operation: '{operation}' is not in the dictionary")

        # usar try catch en caso de entrar a una excepcion el flujo continua, en vez de interrumpir el programa
        try:
            return self._operations[operation](*args)
        except ValueError as e:
            print(f"Error executing operation -> {operation}: {e}")
