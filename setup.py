import setuptools


def read_readme():
    with open('README.md', 'r', encoding='utf-8') as f:
        return f.read()


setuptools.setup(
    name='taxfilingfusion',
    packages=['taxfilingfusion'],
    version='1.0.0',
    license='MIT',
    description='This package will provide the ability for users to access IRS data combined with geographic data in a powerful way.',
    long_description=read_readme(),
    long_description_content_type='text/markdown',
    author='VEDANT RATHI',
    author_email='vedrathi10@gmail.com',
    url='https://github.com/vrathi101/taxfilingfusion.git',
    download_url='https://github.com/vrathi101/taxfilingfusion/archive/refs/tags/v1.0.0.tar.gz',
    keywords=['DATA', 'STATISTICS'],
    install_requires=[
        'pandas',
        'requests',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Information Analysis',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)
