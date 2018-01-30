import re
from setuptools import find_packages, setup

VERSION_RE = re.compile(r'''([0-9dev.]+)''')


def get_version():
    with open('VERSION', 'rU') as fh:
        init = fh.read().strip()
    return VERSION_RE.search(init).group(1)


def get_requirements():
    with open('requirements.txt', 'rU') as f:
        data = f.read().splitlines()
    reqs = []
    for req in data:
        if req.startswith('-e'):
            req = req.split('#egg=')[1]
        reqs.append(req)
    return reqs


setup(
    name='rogers',
    version=get_version(),
    description='Malware Similarity and Nearest Neighbor Tool',
    author='Matthew Maisel',
    author_email='mmaisel@cylance.com',
    url='',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    include_package_data=True,
    install_requires=get_requirements(),
    entry_points={
        'console_scripts': ['rogers=rogers.__main__:main'],
    },
    license="",
)
