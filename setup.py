import setuptools

setuptools.setup(
    name="zmqflp",
    version="0.0.2,
    author="Curtis Wang",
    author_email="ycwang@u.northwestern.edu",
    description="PyZMQ server/client implementing asyncio freelance protocol",
    packages=setuptools.find_packages(),
    install_requires=['pyzmq', 'u-msgpack-python'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0",
        "Operating System :: OS Independent",
    ],
)