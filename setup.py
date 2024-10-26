from pathlib import Path
from setuptools import setup, find_packages

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name="wkafka",  # El nombre de tu paquete
    version="0.1.3",  # La versión de tu paquete
    description="Libreria para usar facilmente kafka en diversos proyectos",
    author="William Steve Rodriguez Villamizar",
    author_email="wisrovi.rodriguez@gmail.com",
    packages=find_packages(),
    install_requires=[
        "opencv-python",
        "numpy",
        "kafka-python",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        'Topic :: Software Development :: Build Tools',
        'Intended Audience :: Developers',
    ],
    python_requires=">=3.6, <3.10",  # Requiere Python >=3.6 y <3.10
    long_description_content_type="text/markdown",
    long_description=long_description,
    license='MIT',    
    url = "https://github.com/wisrovi/wkafka",
)
