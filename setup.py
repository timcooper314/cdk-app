import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="api_ingestion",
    version="0.0.1",

    description="A CDK Python app",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="TC",

    package_dir={"": "api_ingestion"},
    packages=setuptools.find_packages(where="api_ingestion"),

    install_requires=[
        "aws-cdk.core==1.120.0",
        "aws-cdk.aws_s3",
        "aws-cdk.aws_lambda",
        "aws-cdk.aws_s3_notifications",
        "aws-cdk.aws_events",
        "aws-cdk.aws_events_targets",
        "aws-cdk.aws_secretsmanager",
        "urllib3==1.26.6",
        "boto3==1.18.31",
        "pytest==6.2.4"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
