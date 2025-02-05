from setuptools import setup, find_packages

setup(
    name="tourism_data_loader",  # Nombre del paquete
    version="0.1",  # Versión del paquete
    packages=find_packages(),  # Encuentra todos los paquetes en el directorio actual
    install_requires=[  # Dependencias que se necesitan para ejecutar el paquete
        # Ejemplo:
        # "numpy", 
        # "requests",
    ],
    description="Una descripción breve de tu paquete",
    long_description="Una descripción más larga de tu paquete",
    long_description_content_type="text/markdown",  # Si usas markdown en la descripción larga
    author="Tu Nombre",  # Tu nombre
    author_email="carlosrodfer946@gmail.com",  # Tu correo
    url="https://github.com/tu_usuario/mi_paquete",  # URL de tu proyecto, si es aplicable
    classifiers=[  # Clasificadores de Python Package Index (PyPI)
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",  # Asegúrate de que coincida con la licencia de tu proyecto
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",  # Versión mínima de Python requerida
)