"""
Setup configuration for univ3_fetcher package.
"""
from setuptools import setup, find_packages
from pathlib import Path

# Read README
readme_file = Path(__file__).parent / "README.md"
long_description = readme_file.read_text() if readme_file.exists() else ""

# Read requirements
requirements_file = Path(__file__).parent / "requirements.txt"
requirements = []
if requirements_file.exists():
    requirements = [
        line.strip()
        for line in requirements_file.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]

setup(
    name="univ3-fetcher",
    version="1.0.0",
    description="Professional toolkit for harvesting and analyzing Uniswap v3 pool data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="DanieleMDiNosse",
    packages=find_packages(exclude=["tests", "scripts"]),
    python_requires=">=3.11,<3.12",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "univ3-fetch-subgraph=univ3_fetcher.core.harvesters.subgraph_harvester:main",
            "univ3-fetch-rpc=univ3_fetcher.core.harvesters.rpc_harvester:main",
            "univ3-liquidity=univ3_fetcher.core.liquidity.animator:main",
            "univ3-streamlit=univ3_fetcher.ui.streamlit_app:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
    ],
)

