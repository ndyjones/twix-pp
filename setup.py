from setuptools import setup, find_packages

setup(
    name="twitter_analysis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        'pandas>=2.0.0',
        'pyarrow>=14.0.1',
        'tqdm>=4.65.0',
        'python-dotenv>=1.0.0',
        'emoji>=2.8.0',
        'unicodedata2>=15.1.0',
    ],
    extras_require={
        'dev': [
            'pytest>=7.4.0',
            'black>=23.9.1',
            'isort>=5.12.0',
            'flake8>=6.1.0',
        ],
    },
)