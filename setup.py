from __future__ import absolute_import

from setuptools import find_packages, setup

install_requires = ["psycopg2>=2.7", "six>=1.12.0", "SQLAlchemy>=1.0"]
test_requires = ["pytest", "pytest-cov", "pytest-mock"]
dev_requires = test_requires + [
    "autopep8>=1.4.4",
    'black>=18.0.b0,<19;python_version>="3.6"',
    "flake8",
    "ipython",
    "isort>=4.3.21",
    "pip-tools",
    "pylint",
]

with open("VERSION") as version_fd:
    version = version_fd.read().strip()
with open("README.md") as f:
    long_description = f.read()
url = "https://github.com/NerdWalletOSS/savage"
download_url = "{}/archive/v{}.tar.gz".format(url, version)
classifiers = """
Development Status :: 5 - Production/Stable
Intended Audience :: Developers
License :: OSI Approved :: MIT License
Programming Language :: Python
Programming Language :: Python :: 2
Programming Language :: Python :: 2.7
Programming Language :: Python :: 3
Programming Language :: Python :: 3.6
Programming Language :: Python :: 3.7
Programming Language :: SQL
Topic :: Database
Topic :: Database :: Front-Ends
Topic :: Software Development
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: OS Independent
"""

setup(
    name="savage",
    version=version,
    author="Jeremy Lewis",
    author_email="jlewis@nerdwallet.com",
    maintainer="Jeremy Lewis",
    maintainer_email="jlewis@nerdwallet.com",
    url=url,
    description="Automatic version tracking for SQLAlchemy + PostgreSQL (based on versionalchemy)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    download_url=download_url,
    classifiers=[c for c in classifiers.split("\n") if c],
    license="MIT License",
    packages=find_packages("src"),
    package_dir={"": "src"},
    install_requires=install_requires,
    extras_require={"dev": dev_requires, "test": test_requires},
    # Currently `savage` support Python 2.7, and Python 3.6+
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, <4",
    include_package_data=True,
)
