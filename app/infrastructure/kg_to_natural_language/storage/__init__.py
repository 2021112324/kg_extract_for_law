"""Optional single-table knowledge storage; importing this package opens no database."""


def prepare_knowledge(*args, **kwargs):
    from .pipeline import prepare_knowledge as prepare
    return prepare(*args, **kwargs)


def persist_knowledge(*args, **kwargs):
    from .pipeline import persist_knowledge as persist
    return persist(*args, **kwargs)


def query_knowledge(*args, **kwargs):
    from .query import query_knowledge as query
    return query(*args, **kwargs)


__all__ = ["prepare_knowledge", "persist_knowledge", "query_knowledge"]
