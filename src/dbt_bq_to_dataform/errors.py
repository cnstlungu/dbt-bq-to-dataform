"""Exceptions shared across the converter."""


class Unsupported(Exception):
    """A node uses something that has no faithful Dataform translation."""


class ProjectError(Exception):
    """The dbt project cannot be loaded at all."""
