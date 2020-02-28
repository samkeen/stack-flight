from setuptools import setup

setup(
    name='StackFlight',
    version='0.1.0',
    py_modules=['stackflight'],
    install_requires=[
        'Click',
        'boto3'
    ],
    entry_points='''
        [console_scripts]
        stack-flight=stackflight:cli
    ''',
)