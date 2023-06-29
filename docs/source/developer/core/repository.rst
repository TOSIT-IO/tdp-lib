Configuration Versionning
=========================

.. _repository:

Repository
----------

The :py:mod:`~tdp.core.repository.repository.Repository` class is used
to manage files with versionning.
It is used to get a list of file modified since a specific version.

TODO: add python code example to open a file, modify it and create a new version.

GitRepository
-------------

:py:mod:`~tdp.core.repository.git_repository.GitRepository` is an implementation
for :py:mod:`~tdp.core.repository.repository.Repository` which uses
local Git repository to manage files with versionning.
