import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="django-snowflake-backend",
    version="1.0.2",
    author="Ben Ryan",
    author_email="bkr@bu.edu",
    description="Snowflake backend for django",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/benryan2010/django-snowflake-backend",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'snowflake-connector-python',
        'django>=2.2'
    ]
)
