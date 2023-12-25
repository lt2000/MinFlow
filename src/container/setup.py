from setuptools import setup, Extension

module = Extension("byteconcat", sources=["byteconcat.c"])

setup(
    name="byteconcat",
    version="1.0",
    description="Concatenate a list of bytes objects into one bytes object",
    ext_modules=[module]
)