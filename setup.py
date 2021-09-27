import setuptools

with open('README.md', 'r', encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name='aws-glue-schema-registry',
    version='0.0.1',
    author='Corentin Debost',
    author_email='corentin.debost@tenefit.com',
    description='Use the AWS Glue Schema Registry.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    package_dir={'': 'src'},
    packages=setuptools.find_packages(where='src'),
    python_requires='>3.6',
    extras_require={
        'dev': [
            'pytest>=6',
            'flake8>=3'
        ]
    }
)
