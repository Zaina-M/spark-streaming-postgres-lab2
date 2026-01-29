"""
Schema Registry for managing data schema versions.
Provides forward and backward compatibility for streaming data.
"""
from typing import Dict, Optional, List
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, TimestampType, BooleanType
)
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, when
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class SchemaVersion:
    # Represents a schema version with metadata.    
    
    def __init__(
        self,
        version: str,
        schema: StructType,
        created_at: datetime,
        description: str = "",
        is_deprecated: bool = False
    ):
        self.version = version
        self.schema = schema
        self.created_at = created_at
        self.description = description
        self.is_deprecated = is_deprecated
    
    def get_field_names(self) -> List[str]:
        # Get list of field names in this schema.
        return [field.name for field in self.schema.fields]
    
    def has_field(self, field_name: str) -> bool:
        # Check if schema has a specific field.
        return field_name in self.get_field_names()


class SchemaRegistry:
   
    
    def __init__(self):
        self._versions: Dict[str, SchemaVersion] = {}
        self._current_version: str = "v2"
        self._register_default_schemas()
    
    def _register_default_schemas(self):
        # Register the default schema versions.
        
        # V1: Original basic schema
        v1_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("price", DoubleType(), False),
            StructField("event_time", StringType(), False),
        ])
        
        self.register_version(
            version="v1",
            schema=v1_schema,
            description="Original basic schema with core fields only",
            created_at=datetime(2025, 1, 1)
        )
        
        # V2: Enhanced schema with session, category, quantity
        v2_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("user_segment", StringType(), True),
            StructField("search_query", StringType(), True),
            StructField("event_time", StringType(), False),
            StructField("source_system", StringType(), True),
        ])
        
        self.register_version(
            version="v2",
            schema=v2_schema,
            description="Enhanced schema with session tracking, categories, and user segments",
            created_at=datetime(2025, 6, 1)
        )
        
        # V3: Future schema with additional analytics fields
        v3_schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
            StructField("session_id", StringType(), True),
            StructField("event_type", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), False),
            StructField("quantity", IntegerType(), True),
            StructField("user_segment", StringType(), True),
            StructField("search_query", StringType(), True),
            StructField("event_time", StringType(), False),
            StructField("source_system", StringType(), True),
            # New fields in V3
            StructField("device_type", StringType(), True),
            StructField("browser", StringType(), True),
            StructField("geo_country", StringType(), True),
            StructField("geo_city", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("campaign_id", StringType(), True),
            StructField("schema_version", StringType(), True),
        ])
        
        self.register_version(
            version="v3",
            schema=v3_schema,
            description="Analytics schema with device, geo, and campaign tracking",
            created_at=datetime(2026, 1, 1)
        )
    
    def register_version(
        self,
        version: str,
        schema: StructType,
        description: str = "",
        created_at: Optional[datetime] = None,
        is_deprecated: bool = False
    ) -> None:
        # Register a new schema version.
        if created_at is None:
            created_at = datetime.now()
        
        self._versions[version] = SchemaVersion(
            version=version,
            schema=schema,
            created_at=created_at,
            description=description,
            is_deprecated=is_deprecated
        )
        
        logger.info(f"Registered schema version: {version}")
    
    def get_schema(self, version: Optional[str] = None) -> StructType:
        # Get schema for a specific version (or current version).
        version = version or self._current_version
        
        if version not in self._versions:
            raise ValueError(f"Unknown schema version: {version}")
        
        schema_version = self._versions[version]
        
        if schema_version.is_deprecated:
            logger.warning(f"Schema version {version} is deprecated")
        
        return schema_version.schema
    
    def get_version_info(self, version: str) -> SchemaVersion:
        # Get full version information.
        if version not in self._versions:
            raise ValueError(f"Unknown schema version: {version}")
        return self._versions[version]
    
    def list_versions(self) -> List[str]:
        # List all registered schema versions.
        return list(self._versions.keys())
    
    def get_current_version(self) -> str:
        # Get the current default schema version.
        return self._current_version
    
    def set_current_version(self, version: str) -> None:
        # Set the current default schema version.
        if version not in self._versions:
            raise ValueError(f"Unknown schema version: {version}")
        self._current_version = version
        logger.info(f"Set current schema version to: {version}")
    
    def deprecate_version(self, version: str) -> None:
        # Mark a schema version as deprecated.
        if version not in self._versions:
            raise ValueError(f"Unknown schema version: {version}")
        self._versions[version].is_deprecated = True
        logger.warning(f"Deprecated schema version: {version}")
    
    def migrate(
        self,
        df: DataFrame,
        from_version: str,
        to_version: str
    ) -> DataFrame:
        
        # Migrate a DataFrame from one schema version to another.
        
      
        if from_version not in self._versions:
            raise ValueError(f"Unknown source version: {from_version}")
        if to_version not in self._versions:
            raise ValueError(f"Unknown target version: {to_version}")
        
        if from_version == to_version:
            return df
        
        logger.info(f"Migrating schema from {from_version} to {to_version}")
        
        from_schema = self._versions[from_version]
        to_schema = self._versions[to_version]
        
        result_df = df
        
        # Add missing fields with default values
        for field in to_schema.schema.fields:
            if not from_schema.has_field(field.name):
                default_value = self._get_default_value(field)
                result_df = result_df.withColumn(field.name, lit(default_value))
                logger.debug(f"Added field {field.name} with default: {default_value}")
        
        # Select only fields in target schema (in correct order)
        target_fields = to_schema.get_field_names()
        existing_fields = [f for f in target_fields if f in result_df.columns]
        result_df = result_df.select(*existing_fields)
        
        # Add schema version marker
        if "schema_version" in target_fields:
            result_df = result_df.withColumn("schema_version", lit(to_version))
        
        return result_df
    
    def _get_default_value(self, field: StructField):
        # Get appropriate default value for a field type.
        type_defaults = {
            StringType: "",
            IntegerType: 0,
            DoubleType: 0.0,
            BooleanType: False,
        }
        
        for type_class, default in type_defaults.items():
            if isinstance(field.dataType, type_class):
                # Use None for nullable fields, otherwise use type default
                return None if field.nullable else default
        
        return None
    
    def validate_against_schema(
        self,
        df: DataFrame,
        version: Optional[str] = None
    ) -> Dict[str, List[str]]:
        # Validate a DataFrame against a schema version.
        
        # Returns:
        #     Dict with 'missing_fields', 'extra_fields', 'type_mismatches'
        
        version = version or self._current_version
        schema = self.get_schema(version)
        
        result = {
            "missing_fields": [],
            "extra_fields": [],
            "type_mismatches": [],
        }
        
        schema_field_names = [f.name for f in schema.fields]
        df_field_names = df.columns
        
        # Check for missing fields
        for field in schema.fields:
            if field.name not in df_field_names:
                if not field.nullable:
                    result["missing_fields"].append(field.name)
        
        # Check for extra fields
        for col_name in df_field_names:
            if col_name not in schema_field_names:
                result["extra_fields"].append(col_name)
        
        return result
    
    def auto_migrate(self, df: DataFrame) -> DataFrame:
       
        detected_version = self._detect_version(df)
        
        if detected_version is None:
            logger.warning("Could not detect schema version, assuming current")
            return df
        
        if detected_version == self._current_version:
            return df
        
        return self.migrate(df, detected_version, self._current_version)
    
    def _detect_version(self, df: DataFrame) -> Optional[str]:
        # Detect the schema version of a DataFrame.
        df_columns = set(df.columns)
        
        # Check for explicit schema version column
        if "schema_version" in df_columns:
            # Get the first non-null value
            version_row = df.select("schema_version").filter(
                col("schema_version").isNotNull()
            ).first()
            if version_row:
                return version_row[0]
        
        # Heuristic detection based on columns present
        for version in reversed(self.list_versions()):
            schema_columns = set(self._versions[version].get_field_names())
            # If all required (non-nullable) columns are present
            required_columns = {
                f.name for f in self._versions[version].schema.fields 
                if not f.nullable
            }
            if required_columns.issubset(df_columns):
                return version
        
        return None


# Global registry instance
schema_registry = SchemaRegistry()


def get_registry() -> SchemaRegistry:
    # Get the global schema registry instance.
    return schema_registry
