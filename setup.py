import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="lazy_action",
    version="0.0.8",
    author="Irid",
    author_email="irid.zzy@gmail.com",
    description="lazy_action make your func lazy",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/iridesc/lazy_action",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        "diskcache"
    ],
)
