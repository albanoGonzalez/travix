# Usa una imagen base ligera de Python
FROM python:3.13-slim

# Copia los archivos necesarios al contenedor
COPY requirements.txt requirements.txt

# Instala las dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Copia todo el código fuente al contenedor
COPY . .

# Configura la variable de entorno para las credenciales
ENV GOOGLE_APPLICATION_CREDENTIALS=key.json

# Establece el comando predeterminado para ejecutar la aplicación
CMD ["python", "src/main.py"]
