from collections import defaultdict
class BankAccount:
    def __init__(self, saldo_inicial):
        self.balance = saldo_inicial
        # Diccionario con las operaciones
        self.operaciones = {
            "depositar": self.depositar,
            "retirar": self.retirar,
            "ver_balance": self.ver_balance
        }
    
    def depositar(self, cantidad):
        """Deposita una cierta cantidad en la cuenta."""
        if cantidad > 0:
            self.balance += cantidad
            print(f"Depositado: ${cantidad}. Nuevo balance: ${self.balance}.")
        else:
            print("La cantidad a depositar debe ser positiva.")
    
    def retirar(self, cantidad):
        """Retira una cierta cantidad de la cuenta."""
        if cantidad > 0 and cantidad <= self.balance:
            self.balance -= cantidad
            print(f"Retirado: ${cantidad}. Nuevo balance: ${self.balance}.")
        else:
            print("Saldo insuficiente o cantidad de retiro no válida.")
    
    def ver_balance(self):
        """Muestra el balance actual."""
        print(f"Balance actual: ${self.balance}")
    
    def procesar(self, operacion, *args):
        """Procesa una operación basada en la entrada del usuario."""
        if operacion in self.operaciones:
            # Llama a la función correspondiente del diccionario
            self.operaciones[operacion](*args)
        else:
            print(f"Operación '{operacion}' no reconocida.")
