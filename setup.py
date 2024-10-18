from setuptools import setup, find_packages

setup(
    name="utils",  # Nome do pacote
    version="0.1.0",  # Versão inicial
    description="Utilitários MIIA",
    author="M3-MIIA",
    author_email="ari.oliveira@protonmail.com",
    url="https://github.com/M3-MIIA/dbconn",  # URL do projeto
    packages=find_packages(),  # Automaticamente encontra os pacotes Python
    install_requires=[
        'SQLAlchemy',
        'fastapi',
        'mangum',
        'botocore'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        # "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Versão mínima do Python
)