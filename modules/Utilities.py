import hashlib
import pandas as pd

#Crear Función para hashear/modificar con valores ficticios información necesaria 
# usando la libreria hashlib
def hashing_data(value):
        if pd.isnull(value):
            return value
        if isinstance(value, (int, float)):
         value = str(value)
        return hashlib.sha256(value.encode()).hexdigest()[:6]