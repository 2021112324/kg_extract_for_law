"""The sole knowledge table; importing this module never loads project DB config."""

from sqlalchemy import CheckConstraint, Column, Index, MetaData, PrimaryKeyConstraint, Table, Text
from sqlalchemy.dialects.mysql import CHAR, DATETIME, JSON, LONGTEXT, VARCHAR

TABLE_NAME = "t_natural_knowledge_entry"
metadata = MetaData()

knowledge_entry_table = Table(
    TABLE_NAME,
    metadata,
    Column("knowledge_id", VARCHAR(80, collation="utf8mb4_0900_bin"), nullable=False),
    Column("dataset_version", VARCHAR(96, collation="utf8mb4_0900_bin"), nullable=False),
    Column("content", LONGTEXT(), nullable=False),
    Column("knowledge_category", VARCHAR(96), nullable=False),
    Column("knowledge_type", VARCHAR(96), nullable=False),
    Column("validity_status", VARCHAR(128)),
    Column("semantic_types", JSON(none_as_null=True), nullable=False),
    Column("risk_types", JSON(none_as_null=True)),
    Column("economic_industries", JSON(none_as_null=True)),
    Column("tags", JSON(none_as_null=True), nullable=False),
    Column("indicators", JSON(none_as_null=True)),
    Column("document_indicators", JSON(none_as_null=True), nullable=False),
    Column("sources", JSON(none_as_null=True), nullable=False),
    Column("attribute_provenance", JSON(none_as_null=True), nullable=False),
    Column("field_states", JSON(none_as_null=True), nullable=False),
    Column("metadata", JSON(none_as_null=True), nullable=False),
    Column("content_hash", CHAR(64), nullable=False),
    Column("record_hash", CHAR(64), nullable=False),
    Column("run_id", VARCHAR(64), nullable=False),
    Column("conversion_rule_version", VARCHAR(96), nullable=False),
    Column("classification_config_version", VARCHAR(96)),
    Column("publication_status", VARCHAR(16), nullable=False, server_default="staging"),
    Column("generated_at", DATETIME(fsp=6), nullable=False),
    Column("created_at", DATETIME(fsp=6), nullable=False),
    Column("updated_at", DATETIME(fsp=6), nullable=False),
    Column("log_reference", Text()),
    CheckConstraint("publication_status IN ('staging', 'published')",
                    name="ck_natural_knowledge_publication"),
    mysql_engine="InnoDB",
    mysql_charset="utf8mb4",
    mysql_collate="utf8mb4_0900_bin",
)

# Keep PK order independent of the human-facing column order in the design.
knowledge_entry_table.append_constraint(
    PrimaryKeyConstraint("dataset_version", "knowledge_id")
)
Index("ix_natural_knowledge_publication", knowledge_entry_table.c.dataset_version,
      knowledge_entry_table.c.publication_status)
Index("ix_natural_knowledge_category", knowledge_entry_table.c.dataset_version,
      knowledge_entry_table.c.knowledge_category, knowledge_entry_table.c.knowledge_type)

