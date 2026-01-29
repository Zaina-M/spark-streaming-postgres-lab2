# Centralized configuration management for the e-commerce streaming pipeline.


import os
from dataclasses import dataclass, field
from typing import List, Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class DatabaseConfig:
    #PostgreSQL database configuration.
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = int(os.getenv("POSTGRES_PORT", "5433"))
    database: str = os.getenv("POSTGRES_DB", "ecommerce")
    user: str = os.getenv("POSTGRES_USER", "spark")
    password: str = os.getenv("POSTGRES_PASSWORD", "spark123")
    
    # Connection pool settings
    min_connections: int = int(os.getenv("DB_MIN_CONNECTIONS", "2"))
    max_connections: int = int(os.getenv("DB_MAX_CONNECTIONS", "10"))
    connection_timeout: int = int(os.getenv("DB_CONNECTION_TIMEOUT", "30"))
    
    @property
    def jdbc_url(self) -> str:
        #JDBC connection URL for Spark.
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"
    
    @property
    def connection_string(self) -> str:
        # Standard connection string for psycopg2.
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    @property
    def jdbc_properties(self) -> dict:
        # JDBC properties for Spark.
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver"
        }


@dataclass
class StreamingConfig:
    # Spark streaming configuration.
    input_path: str = os.getenv("INPUT_PATH", "/data/input")
    checkpoint_path: str = os.getenv("CHECKPOINT_PATH", "/data/checkpoints/ecommerce")
    log_path: str = os.getenv("LOG_PATH", "/data/logs")
    
    # Trigger settings
    trigger_interval: str = os.getenv("TRIGGER_INTERVAL", "10 seconds")
    watermark_delay: str = os.getenv("WATERMARK_DELAY", "10 minutes")
    max_files_per_trigger: int = int(os.getenv("MAX_FILES_PER_TRIGGER", "10"))
    
    # Processing settings
    output_mode: str = os.getenv("OUTPUT_MODE", "append")
    
    @property
    def schema_version(self) -> str:
        # Current schema version to use.
        return os.getenv("SCHEMA_VERSION", "v2")


@dataclass
class DataQualityConfig:
    # Data quality thresholds and validation rules.
    # Validity thresholds
    min_validity_rate: float = float(os.getenv("MIN_VALIDITY_RATE", "95.0"))
    max_null_rate: float = float(os.getenv("MAX_NULL_RATE", "0.1"))
    
    # Price validation
    max_price: float = float(os.getenv("MAX_PRICE", "10000.0"))
    min_price: float = float(os.getenv("MIN_PRICE", "0.0"))
    
    # Time validation
    late_arrival_threshold_minutes: int = int(os.getenv("LATE_ARRIVAL_MINUTES", "5"))
    future_event_tolerance_seconds: int = int(os.getenv("FUTURE_EVENT_TOLERANCE", "60"))
    
    # Valid event types
    valid_event_types: List[str] = field(default_factory=lambda: [
        "view", "purchase", "add_to_cart", "remove_from_cart", "wishlist", "search"
    ])
    
    # Valid user segments
    valid_user_segments: List[str] = field(default_factory=lambda: [
        "new", "returning", "premium", "inactive", "anonymous"
    ])


@dataclass
class GeneratorConfig:
    # Data generator configuration.
    batch_size: int = int(os.getenv("BATCH_SIZE", "100"))
    interval_seconds: float = float(os.getenv("INTERVAL_SECONDS", "5.0"))
    anomaly_rate: float = float(os.getenv("ANOMALY_RATE", "0.02"))
    anonymous_rate: float = float(os.getenv("ANONYMOUS_RATE", "0.1"))
    
    # User and product ranges
    min_user_id: int = int(os.getenv("MIN_USER_ID", "1"))
    max_user_id: int = int(os.getenv("MAX_USER_ID", "1000"))
    min_product_id: int = int(os.getenv("MIN_PRODUCT_ID", "1"))
    max_product_id: int = int(os.getenv("MAX_PRODUCT_ID", "500"))
    
    # Output settings
    output_dir: str = os.getenv("GENERATOR_OUTPUT_DIR", "./data/input")


@dataclass
class RetryConfig:
    # Retry and fault tolerance configuration.
    max_retries: int = int(os.getenv("MAX_RETRIES", "3"))
    initial_delay_seconds: float = float(os.getenv("INITIAL_RETRY_DELAY", "1.0"))
    backoff_factor: float = float(os.getenv("RETRY_BACKOFF_FACTOR", "2.0"))
    max_delay_seconds: float = float(os.getenv("MAX_RETRY_DELAY", "60.0"))
    
    # Circuit breaker settings
    circuit_breaker_threshold: int = int(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "5"))
    circuit_breaker_timeout: float = float(os.getenv("CIRCUIT_BREAKER_TIMEOUT", "30.0"))


@dataclass
class MonitoringConfig:
    # Monitoring and alerting configuration.
    # Thresholds for alerts
    validity_alert_threshold: float = float(os.getenv("VALIDITY_ALERT_THRESHOLD", "95.0"))
    latency_alert_threshold_ms: float = float(os.getenv("LATENCY_ALERT_THRESHOLD_MS", "10000.0"))
    throughput_alert_threshold: float = float(os.getenv("THROUGHPUT_ALERT_THRESHOLD", "100.0"))
    
    # Metrics window
    metrics_window_size: int = int(os.getenv("METRICS_WINDOW_SIZE", "100"))
    
    # Alerting settings
    enable_slack_alerts: bool = os.getenv("ENABLE_SLACK_ALERTS", "false").lower() == "true"
    slack_webhook_url: Optional[str] = os.getenv("SLACK_WEBHOOK_URL")
    
    enable_email_alerts: bool = os.getenv("ENABLE_EMAIL_ALERTS", "false").lower() == "true"
    alert_email: Optional[str] = os.getenv("ALERT_EMAIL")
    
    # Metrics export
    enable_prometheus: bool = os.getenv("ENABLE_PROMETHEUS", "false").lower() == "true"
    prometheus_port: int = int(os.getenv("PROMETHEUS_PORT", "9090"))


@dataclass
class AppConfig:
    # Main application configuration container.
    database: DatabaseConfig
    streaming: StreamingConfig
    quality: DataQualityConfig
    generator: GeneratorConfig
    retry: RetryConfig
    monitoring: MonitoringConfig
    
    # General settings
    environment: str = os.getenv("ENVIRONMENT", "development")
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    app_name: str = os.getenv("APP_NAME", "ecommerce-streaming-pipeline")
    
    @classmethod
    def load(cls) -> "AppConfig":
        # Load configuration from environment variables.
        return cls(
            database=DatabaseConfig(),
            streaming=StreamingConfig(),
            quality=DataQualityConfig(),
            generator=GeneratorConfig(),
            retry=RetryConfig(),
            monitoring=MonitoringConfig(),
        )
    
    def validate(self) -> List[str]:
        # Validate configuration and return list of errors.
        errors = []
        
        if self.database.port < 1 or self.database.port > 65535:
            errors.append(f"Invalid database port: {self.database.port}")
        
        if self.quality.min_validity_rate < 0 or self.quality.min_validity_rate > 100:
            errors.append(f"Invalid validity rate: {self.quality.min_validity_rate}")
        
        if self.retry.max_retries < 0:
            errors.append(f"Invalid max retries: {self.retry.max_retries}")
        
        if self.monitoring.enable_slack_alerts and not self.monitoring.slack_webhook_url:
            errors.append("Slack alerts enabled but no webhook URL provided")
        
        return errors
    
    def to_dict(self) -> dict:
        # Convert config to dictionary (for logging).
        return {
            "environment": self.environment,
            "log_level": self.log_level,
            "database": {
                "host": self.database.host,
                "port": self.database.port,
                "database": self.database.database,
            },
            "streaming": {
                "trigger_interval": self.streaming.trigger_interval,
                "watermark_delay": self.streaming.watermark_delay,
            },
            "quality": {
                "min_validity_rate": self.quality.min_validity_rate,
            },
            "monitoring": {
                "validity_alert_threshold": self.monitoring.validity_alert_threshold,
            }
        }


# Global configuration instance - load once, use everywhere
config = AppConfig.load()


def get_config() -> AppConfig:
    #Get the global configuration instance.
    return config


def reload_config() -> AppConfig:
    #Reload configuration from environment (useful for testing).
    global config
    load_dotenv(override=True)
    config = AppConfig.load()
    return config
