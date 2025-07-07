from setuptools import setup, find_packages

setup(
    name="utils",
    version="0.1.1",
    description="Utilitários MIIA",
    author="M3-MIIA",
    author_email="ari.oliveira@protonmail.com",
    url="https://github.com/M3-MIIA/utils",
    packages=find_packages(),
    install_requires=[
        'SQLAlchemy == 2.0.41',
        'fastapi == 0.115.14',
        'mangum == 0.19.0',
        'boto3 == 1.39.3',
        'dbconn @ git+https://github.com/M3-MIIA/dbconn.git@v1.0.4',
        'PyJWT == 2.10.1',
        'python-dotenv == 1.1.1',
        'starlette'
    ],
    extras_require={
        'dev': [
            'pytest == 8.4.1',
            'pytest-aio == 1.9.0'
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        # "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',  # Versão mínima do Python
)