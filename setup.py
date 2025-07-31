from setuptools import setup, find_packages

setup(
    name="clidesc-common",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "sqlalchemy",
        "Pillow",  # Pillow for image processing
        "numpy",
        # add other dependencies here
    ],
    author="James Sturman",
    description="A package for CliDE database data extraction and analysis.",
)