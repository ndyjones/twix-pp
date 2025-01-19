tests/
├── __init__.py           # Makes the tests directory a package
├── conftest.py          # Shared pytest fixtures and configuration
├── preprocessing/       # Tests for preprocessing module
│   ├── __init__.py
│   ├── test_archive_processor.py
│   └── test_text_cleaner.py
└── utils/              # Tests for utilities module
    ├── __init__.py
    └── test_logging_config.py