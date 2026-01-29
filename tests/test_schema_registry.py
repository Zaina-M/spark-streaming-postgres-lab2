"""
Unit tests for schema registry.
Run with: pytest tests/test_schema_registry.py -v
"""
import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'spark'))

from schema.registry import SchemaRegistry, SchemaVersion, get_registry


@pytest.fixture(scope="module")
def spark():
    #Create a Spark session for testing.
    return SparkSession.builder \
        .master("local[1]") \
        .appName("SchemaRegistryTest") \
        .getOrCreate()


@pytest.fixture
def registry():
    #Create a fresh registry for each test.
    return SchemaRegistry()


class TestSchemaVersion:
    #Tests for SchemaVersion class.
    
    def test_get_field_names(self):
        #Should return list of field names.
        from datetime import datetime
        
        schema = StructType([
            StructField("field1", StringType(), False),
            StructField("field2", IntegerType(), True),
        ])
        
        version = SchemaVersion(
            version="test",
            schema=schema,
            created_at=datetime.now()
        )
        
        assert version.get_field_names() == ["field1", "field2"]
    
    def test_has_field(self):
        #Should check if field exists.  
        from datetime import datetime
        
        schema = StructType([
            StructField("existing_field", StringType(), False),
        ])
        
        version = SchemaVersion(
            version="test",
            schema=schema,
            created_at=datetime.now()
        )
        
        assert version.has_field("existing_field") is True
        assert version.has_field("nonexistent_field") is False


class TestSchemaRegistry:
    #Tests for SchemaRegistry class.
    
    def test_default_schemas_registered(self, registry):
        #Should have default schemas registered.
        versions = registry.list_versions()
        assert "v1" in versions
        assert "v2" in versions
        assert "v3" in versions
    
    def test_get_current_schema(self, registry):
        #Should return current schema version.
        schema = registry.get_schema()
        assert schema is not None
        assert isinstance(schema, StructType)
    
    def test_get_specific_version(self, registry):
        #Should return specific schema version.
        v1_schema = registry.get_schema("v1")
        v2_schema = registry.get_schema("v2")
        
        # v2 should have more fields than v1
        assert len(v2_schema.fields) > len(v1_schema.fields)
    
    def test_get_unknown_version_raises(self, registry):
        #Should raise error for unknown version.
        with pytest.raises(ValueError) as exc_info:
            registry.get_schema("v999")
        
        assert "Unknown schema version" in str(exc_info.value)
    
    def test_register_custom_version(self, registry):
        #Should allow registering custom versions.
        custom_schema = StructType([
            StructField("id", StringType(), False),
            StructField("value", DoubleType(), False),
        ])
        
        registry.register_version(
            version="custom",
            schema=custom_schema,
            description="Custom test schema"
        )
        
        assert "custom" in registry.list_versions()
        assert registry.get_schema("custom") == custom_schema
    
    def test_set_current_version(self, registry):
        # Should allow changing current version.
        registry.set_current_version("v1")
        assert registry.get_current_version() == "v1"
        
        registry.set_current_version("v2")
        assert registry.get_current_version() == "v2"
    
    def test_deprecate_version(self, registry):
        # Should mark version as deprecated.
        registry.deprecate_version("v1")
        version_info = registry.get_version_info("v1")
        assert version_info.is_deprecated is True
    
    def test_get_version_info(self, registry):
        # Should return version metadata.
        info = registry.get_version_info("v2")
        
        assert info.version == "v2"
        assert info.description != ""
        assert info.created_at is not None


class TestSchemaMigration:
    #Tests for schema migration functionality.
    
    def test_migrate_adds_missing_fields(self, spark, registry):
        #Migration should add missing fields with defaults.
        # Create a v1 dataframe
        v1_data = [
            ("event1", 1, "view", 100, 10.0, "2024-01-01T00:00:00Z"),
            ("event2", 2, "purchase", 200, 25.0, "2024-01-01T00:01:00Z"),
        ]
        v1_schema = registry.get_schema("v1")
        v1_df = spark.createDataFrame(v1_data, v1_schema)
        
        # Migrate to v2
        v2_df = registry.migrate(v1_df, from_version="v1", to_version="v2")
        
        # Check new fields exist
        v2_columns = v2_df.columns
        assert "session_id" in v2_columns
        assert "category" in v2_columns
        assert "quantity" in v2_columns
    
    def test_migrate_same_version_noop(self, spark, registry):
        #Migrating same version should return same dataframe.
        data = [("event1", 1, "view", 100, 10.0, "2024-01-01T00:00:00Z")]
        v1_schema = registry.get_schema("v1")
        df = spark.createDataFrame(data, v1_schema)
        
        result = registry.migrate(df, from_version="v1", to_version="v1")
        
        # Should be the same dataframe
        assert result.columns == df.columns
        assert result.count() == df.count()
    
    def test_migrate_unknown_version_raises(self, spark, registry):
        #Migration with unknown version should raise error.
        data = [("event1", 1, "view", 100, 10.0, "2024-01-01T00:00:00Z")]
        v1_schema = registry.get_schema("v1")
        df = spark.createDataFrame(data, v1_schema)
        
        with pytest.raises(ValueError):
            registry.migrate(df, from_version="v1", to_version="v999")


class TestSchemaValidation:
    #Tests for schema validation.
    
    def test_validate_matching_schema(self, spark, registry):
        #Validation should pass for matching schema.
        data = [
            ("event1", 1, "sess1", "view", 100, "electronics", 
             10.0, 1, "new", "", "2024-01-01T00:00:00Z", "web"),
        ]
        v2_schema = registry.get_schema("v2")
        df = spark.createDataFrame(data, v2_schema)
        
        result = registry.validate_against_schema(df, "v2")
        
        assert len(result["missing_fields"]) == 0
        assert len(result["extra_fields"]) == 0
    
    def test_validate_detects_missing_fields(self, spark, registry):
        # Validation should detect missing required fields.
        # Create dataframe with fewer columns than schema requires
        data = [("event1", 1)]
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
        ])
        df = spark.createDataFrame(data, schema)
        
        result = registry.validate_against_schema(df, "v2")
        
        # Should detect missing required fields
        assert len(result["missing_fields"]) > 0
    
    def test_validate_detects_extra_fields(self, spark, registry):
        # Validation should detect extra fields.
        data = [("event1", 1, "view", 100, 10.0, "2024-01-01", "extra_value")]
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("user_id", IntegerType(), True),
            StructField("event_type", StringType(), False),
            StructField("product_id", IntegerType(), False),
            StructField("price", DoubleType(), False),
            StructField("event_time", StringType(), False),
            StructField("extra_field", StringType(), True),  # Not in v1
        ])
        df = spark.createDataFrame(data, schema)
        
        result = registry.validate_against_schema(df, "v1")
        
        assert "extra_field" in result["extra_fields"]


class TestAutoMigration:
    #Tests for automatic schema detection and migration.
    
    def test_auto_migrate_detects_v1(self, spark, registry):
        # Should auto-detect v1 schema and migrate.
        data = [("event1", 1, "view", 100, 10.0, "2024-01-01T00:00:00Z")]
        v1_schema = registry.get_schema("v1")
        df = spark.createDataFrame(data, v1_schema)
        
        registry.set_current_version("v2")
        result = registry.auto_migrate(df)
        
        # Should have v2 columns
        assert "session_id" in result.columns
    
    def test_auto_migrate_already_current(self, spark, registry):
        # Should handle dataframe already at current version.
        data = [
            ("event1", 1, "sess1", "view", 100, "electronics",
             10.0, 1, "new", "", "2024-01-01T00:00:00Z", "web"),
        ]
        v2_schema = registry.get_schema("v2")
        df = spark.createDataFrame(data, v2_schema)
        
        registry.set_current_version("v2")
        result = registry.auto_migrate(df)
        
        # Should have same columns
        assert set(result.columns) == set(df.columns)


class TestGlobalRegistry:
    # Tests for global registry access.
    
    def test_get_registry_returns_instance(self):
        # get_registry should return a SchemaRegistry.
        registry = get_registry()
        assert isinstance(registry, SchemaRegistry)
    
    def test_get_registry_returns_same_instance(self):
        # get_registry should return the same instance.
        registry1 = get_registry()
        registry2 = get_registry()
        assert registry1 is registry2


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
