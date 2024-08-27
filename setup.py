from setuptools import setup, find_packages

setup(
    name="spacerat",
    version="0.0.1",
    package_dir={"": "src"},
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
    ],
    entry_points={
        "console_scripts": [
            "spacerat = spacerat.cli:cli",
        ],
    },
)
