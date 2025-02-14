from setuptools import setup, find_packages

setup(
    name="GCP-London-Bicycle",
    version="1.0",
    install_requires=[
        "apache-beam[gcp]==2.50.0",
        "geopy==1.18.0",
        "build==1.2.2",
        "setuptools>=75.5.1",
        "wheel>=0.45.0",
    ],
    python_requires=">=3.8, <3.11",
    packages=find_packages(exclude=["notebooks"]),
    include_package_data=True,
    description="Coding Challenge",
)
