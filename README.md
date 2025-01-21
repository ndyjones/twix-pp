# README.md
# Twitter Archive Preprocessor

A Python tool for preprocessing Twitter archive data for analysis.

## Setup

1. Clone the repository
2. Create a virtual environment: `python -m venv venv`
3. Activate the virtual environment:
   - Windows: `venv\Scripts\activate`
   - Unix/MacOS: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`

## Usage

```python
from twitter_analysis.preprocessing import TwitterArchivePreprocessor

preprocessor = TwitterArchivePreprocessor(
    archive_path="path/to/twitter/archive",
    output_path="path/to/output"
)

df = preprocessor.process_archive()
```

## Project Structure

```
├── LICENSE
├── README.md
├── pyproject.toml
├── requirements.txt
├── setup.py
├── tests
│   ├── README.md
│   ├── __init__.py
│   ├── conftest.py
│   ├── preprocessing
│   │   ├── __init__.py
│   │   ├── test_archive_processor.py
│   │   └── test_text_cleaner.py
│   └── utils
└── twitter_analysis
    ├── __init__.py
    ├── config
    │   ├── __init__.py
    │   └── settings.py
    ├── preprocessing
    │   ├──  media_handler.py
    │   ├── __init__.py
    │   ├── archive_processor.py
    │   └── text_cleaner.py
    └── utils
        ├── __init__.py
        └── logging_config.py
```

## Contributing

1. Fork the repository
2. Create a new branch
3. Make your changes
4. Submit a pull request

## License

MIT License - See LICENSE file for details
