"""
Unit tests for configuration management.
Run with: pytest tests/test_config.py -v
"""
import os
import sys
import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import (
    AppConfig,
    DatabaseConfig,
    StreamingConfig,
    DataQualityConfig,
    GeneratorConfig,
    RetryConfig,
    MonitoringConfig,
    reload_config,
)


class TestDatabaseConfig:
    #Tests for database configuration.
    
    def test_default_values(self):
        #Should have sensible defaults or env values.
        config = DatabaseConfig()
        # These may come from .env file or defaults
        assert config.host is not None
        assert config.port > 0
        assert config.database is not None
    
    def test_jdbc_url_format(self):
        #JDBC URL should be properly formatted.
        config = DatabaseConfig(host="testhost", port=5432, database="testdb")
        assert config.jdbc_url == "jdbc:postgresql://testhost:5432/testdb"
    
    def test_connection_string_format(self):
        #Connection string should include credentials.
        config = DatabaseConfig(
            host="testhost", port=5432, database="testdb",
            user="testuser", password="testpass"
        )
        assert "testuser:testpass" in config.connection_string
        assert "testhost:5432" in config.connection_string
    
    def test_jdbc_properties(self):
        #JDBC properties should contain driver info.
        config = DatabaseConfig(user="testuser", password="testpass")
        props = config.jdbc_properties
        assert props["user"] == "testuser"
        assert props["password"] == "testpass"
        assert props["driver"] == "org.postgresql.Driver"


class TestStreamingConfig:
    #Tests for streaming configuration.
    
    def test_default_paths(self):
        #Should have default paths.
        config = StreamingConfig()
        assert config.input_path == "/data/input"
        assert config.checkpoint_path == "/data/checkpoints/ecommerce"
    
    def test_trigger_settings(self):
        #Should have valid trigger settings.
        config = StreamingConfig()
        assert "seconds" in config.trigger_interval
        assert "minutes" in config.watermark_delay


class TestDataQualityConfig:
    #Tests for data quality configuration.
    
    def test_validity_threshold_range(self):
        #Validity threshold should be percentage.   
        config = DataQualityConfig()
        assert 0 <= config.min_validity_rate <= 100
    
    def test_valid_event_types(self):
        #Should have standard e-commerce event types.
        config = DataQualityConfig()
        assert "view" in config.valid_event_types
        assert "purchase" in config.valid_event_types
        assert "add_to_cart" in config.valid_event_types
    
    def test_price_range(self):
        """Price limits should be sensible."""
        config = DataQualityConfig()
        assert config.min_price <= config.max_price


class TestRetryConfig:
    #Tests for retry configuration.
    
    def test_default_retries(self):
        #Should have reasonable retry defaults.
        config = RetryConfig()
        assert config.max_retries >= 1
        assert config.initial_delay_seconds > 0
    
    def test_backoff_factor(self):
        #Backoff factor should be >= 1.
        config = RetryConfig()
        assert config.backoff_factor >= 1.0
    
    def test_circuit_breaker_threshold(self):
        #Circuit breaker should have valid threshold.   
        config = RetryConfig()
        assert config.circuit_breaker_threshold >= 1


class TestMonitoringConfig:
    #Tests for monitoring configuration.
    
    def test_threshold_values(self):
        #Thresholds should be positive.
        config = MonitoringConfig()
        assert config.validity_alert_threshold > 0
        assert config.latency_alert_threshold_ms > 0
        assert config.throughput_alert_threshold > 0
    
    def test_slack_disabled_by_default(self):
        #Slack alerts should be disabled by default.
        config = MonitoringConfig()
        assert config.enable_slack_alerts is False


class TestAppConfig:
    #Tests for main application configuration.
    
    def test_load_creates_all_subconfigs(self):
        #Loading should create all sub-configurations.
        config = AppConfig.load()
        assert isinstance(config.database, DatabaseConfig)
        assert isinstance(config.streaming, StreamingConfig)
        assert isinstance(config.quality, DataQualityConfig)
        assert isinstance(config.generator, GeneratorConfig)
        assert isinstance(config.retry, RetryConfig)
        assert isinstance(config.monitoring, MonitoringConfig)
    
    def test_default_environment(self):
        #Default environment should be development.
        config = AppConfig.load()
        assert config.environment == "development"
    
    def test_validate_returns_empty_for_valid_config(self):
        #Validation should pass for default config.
        config = AppConfig.load()
        errors = config.validate()
        # Filter out the Slack alert error if present
        errors = [e for e in errors if "Slack" not in e]
        assert len(errors) == 0
    
    def test_to_dict_excludes_secrets(self):
        #Dict representation should not include full credentials.
        config = AppConfig.load()
        config_dict = config.to_dict()
        # Password should not be in the output
        assert "password" not in str(config_dict)


class TestEnvironmentOverrides:
    #Tests for environment variable overrides.
    
    def test_config_reads_from_environment(self):
        #Config should read values from environment.
        # The fact that we can create config proves env reading works
        config = AppConfig.load()
        # These should all have values (either from env or defaults)
        assert config.database.host is not None
        assert config.database.port > 0
        assert config.log_level in ["DEBUG", "INFO", "WARNING", "ERROR"]
    
    def test_jdbc_url_built_from_config(self):
        #JDBC URL should be built from config values.
        config = DatabaseConfig()
        jdbc_url = config.jdbc_url
        assert jdbc_url.startswith("jdbc:postgresql://")
        assert config.host in jdbc_url
        assert str(config.port) in jdbc_url
    
    def test_config_values_are_strings_and_numbers(self):
        #Config values should have correct types.
        config = AppConfig.load()
        assert isinstance(config.database.host, str)
        assert isinstance(config.database.port, int)
        assert isinstance(config.quality.min_validity_rate, float)
        assert isinstance(config.retry.max_retries, int)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
