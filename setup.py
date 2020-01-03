#!/usr/bin/env python
import io
import os
import re

from setuptools import setup, find_packages

RE_REQUIREMENT = re.compile(r'^\s*-r\s*(?P<filename>.*)$')
RE_BADGE = re.compile(r'^\[\!\[(?P<text>[^\]]+)\]\[(?P<badge>[^\]]+)\]\]\[(?P<target>[^\]]+)\]$', re.M)

BADGES_TO_KEEP = ['gitter-badge', 'readthedocs-badge']


def md(filename):
    '''
    Load .md (markdown) file and sanitize it for PyPI.
    Remove unsupported github tags:
     - code-block directive
     - travis ci build badges
    '''
    content = io.open(filename).read()

    for match in RE_BADGE.finditer(content):
        if match.group('badge') not in BADGES_TO_KEEP:
            content = content.replace(match.group(0), '')
    return content


long_description = '\n'.join((
    md('README.md'),
    md('CHANGELOG.md'),
    ''
))


def pip(filename):
    """Parse pip reqs file and transform it to setuptools requirements."""
    requirements = []
    for line in open(os.path.join('requirements', filename)):
        line = line.strip()
        if not line or '://' in line or line.startswith('#'):
            continue
        match = RE_REQUIREMENT.match(line)
        if match:
            requirements.extend(pip(match.group('filename')))
        else:
            requirements.append(line)
    return requirements


install_requires = pip('install.pip')
tests_require = pip('test.pip')

setup(
    name='csvapi',
    python_requires='>=3.6.1',
    description='An instant JSON API for your CSV',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Opendata Team',
    author_email='opendatateam@data.gouv.fr',
    version='1.0.1.dev',
    license='MIT',
    url='https://github.com/opendatateam/csvapi',
    packages=find_packages(),
    package_data={'csvapi': []},
    include_package_data=True,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
    entry_points='''
        [console_scripts]
        csvapi=csvapi.cli:cli
    ''',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
    ],
)
