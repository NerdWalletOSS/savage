from setuptools import setup, find_packages

install_requires = [
    'psycopg2>=2.6',
    'simplejson>=3.0',
    'six>=1.10.0',
    'SQLAlchemy>=1.0',
]

with open('VERSION') as version_fd:
    version = version_fd.read().strip()

setup(
    name='savage',
    version=version,
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=install_requires,
    include_package_data=True,
    author='Jeremy Lewis',
    author_email='jlewis@nerdwallet.com',
    license='Other/Proprietary License',
    description='Automatic version tracking for SQLAlchemy + PostgreSQL (based on versionalchemy)',
    url='https://github.com/NerdWalletOSS/savage'
)
