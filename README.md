# ![](https://dddmisc.github.io/dddmisc-docs/img/logo.svg)
![GitHub License](https://img.shields.io/github/license/dddmisc/dddmisc)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dddmisc-core)



DDDMisc is a set of libraries that provide basic solutions
for implementing domain-driven design methods
in the Python programming language.


**The development of libraries is in alpha version.
The public API is not guaranteed to be backward
compatible between minor versions of packages.**

## Documentation
- English (Coming soon...)
- [Russian](https://dddmisc.github.io/dddmisc-docs/)

## Libraries
- `dddmisc-core`- this package provides the core interfaces and
    types for dddmisc packages family;
- `dddmisc-domain` - this package provides implementation domain's objects classes;
- `dddmisc-messagebus` - this package provides the implementation messagebus;
- `dddmisc-handlers-collection` - this package provides implementation a collection of command’s and event’s handlers;
- `dddmisc-uow` - This package provides the implementation pattern 'unit of work'.


## Installation
You can only use the packages you need.
All packages install `dddmisc-core` as its dependency.

### Install `dddmics-domain`

```shell
pip install dddmisc-domain
```
![PyPI - Downloads](https://img.shields.io/pypi/dm/dddmisc-domain)

This package install [`pydantic`](https://github.com/pydantic/pydantic) as its dependency.

### Install `dddmisc-messagebus`
```shell
pip install dddmisc-messagebus
```
![PyPI - Downloads](https://img.shields.io/pypi/dm/dddmisc-messagebus)

### Install `dddmisc-handlers-collection`
```shell
pip install dddmisc-handlers-collection
```
![PyPI - Downloads](https://img.shields.io/pypi/dm/dddmisc-handlers-collection)

This package install [`tenacity`](https://github.com/jd/tenacity/tree/main) as its dependency.

### Install `dddmisc-uow`
```shell
pip install dddmisc-unit-of-work
```
![PyPI - Downloads](https://img.shields.io/pypi/dm/dddmisc-unit-of-work)
