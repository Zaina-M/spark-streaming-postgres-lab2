"""
Integration tests for the e-commerce streaming pipeline.
These tests require Docker services to be running.
Run with: pytest tests/test_integration.py -v --integration
"""

import os
import sys
import time
import pytest
import psycopg2
from datetime import datetime


# Mark all tests as integration tests
pytestmark = pytest.mark.integration


# Database connection settings (from environment or defaults)
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "ecommerce"),
    "user": os.getenv("POSTGRES_USER", "spark"),
    "password": os.getenv("POSTGRES_PASSWORD", "spark"),
}


def get_db_connection():
    # Get a database connection.
    return psycopg2.connect(**DB_CONFIG)


@pytest.fixture(scope="module")
def db_connection():
    # Provide a database connection for tests.
    try:
        conn = get_db_connection()
        yield conn
        conn.close()
    except psycopg2.OperationalError as e:
        pytest.skip(f"PostgreSQL not available: {e}")


class TestDatabaseSchema:
    # Tests for database schema validation.
    
    def test_ecommerce_events_table_exists(self, db_connection):
        # Main events table should exist.
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'ecommerce_events'
            );
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        assert exists is True
    
    def test_dead_letter_table_exists(self, db_connection):
        # Dead letter table should exist.
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'dead_letter_events'
            );
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        assert exists is True
    
    def test_quality_metrics_table_exists(self, db_connection):
        # Data quality metrics table should exist.
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'data_quality_metrics'
            );
        """)
        exists = cursor.fetchone()[0]
        cursor.close()
        assert exists is True
    
    def test_events_table_has_required_columns(self, db_connection):
        # Events table should have all required columns.
        expected_columns = [
            "event_id", "user_id", "session_id", "event_type", "product_id",
            "category", "price", "quantity", "total_amount", "user_segment",
            "search_query", "event_time", "event_year", "event_month",
            "event_day", "event_hour", "event_dayofweek", "is_late_arrival",
            "source_file", "source_system", "processed_at"
        ]
        
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'ecommerce_events';
        """)
        actual_columns = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        for col in expected_columns:
            assert col in actual_columns, f"Missing column: {col}"
    
    def test_indexes_exist(self, db_connection):
        # Required indexes should exist.
        cursor = db_connection.cursor()
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'ecommerce_events';
        """)
        indexes = [row[0] for row in cursor.fetchall()]
        cursor.close()
        
        # At least primary key and some additional indexes
        assert len(indexes) >= 3


class TestDataIngestion:
    #  Tests for data ingestion into PostgreSQL.
    
    def test_can_insert_valid_event(self, db_connection):
        # Should be able to insert a valid event.
        cursor = db_connection.cursor()
        
        test_event_id = f"test-{datetime.now().timestamp()}"
        
        try:
            cursor.execute("""
                INSERT INTO ecommerce_events (
                    event_id, user_id, session_id, event_type, product_id,
                    category, price, quantity, total_amount, user_segment,
                    search_query, event_time, event_year, event_month,
                    event_day, event_hour, event_dayofweek, is_late_arrival,
                    source_file, source_system, processed_at
                ) VALUES (
                    %s, 1, 'test-session', 'view', 100,
                    'electronics', 0.0, 0, 0.0, 'new',
                    '', NOW(), 2026, 1, 15, 14, 4, FALSE,
                    '/test/file.csv', 'test', NOW()
                )
            """, (test_event_id,))
            
            db_connection.commit()
            
            # Verify insertion
            cursor.execute(
                "SELECT event_id FROM ecommerce_events WHERE event_id = %s",
                (test_event_id,)
            )
            result = cursor.fetchone()
            assert result is not None
            
            # Cleanup
            cursor.execute(
                "DELETE FROM ecommerce_events WHERE event_id = %s",
                (test_event_id,)
            )
            db_connection.commit()
            
        finally:
            cursor.close()
    
    def test_duplicate_event_id_rejected(self, db_connection):
        # Duplicate event_id should be rejected (primary key).
        cursor = db_connection.cursor()
        
        test_event_id = f"test-dup-{datetime.now().timestamp()}"
        
        try:
            # First insert
            cursor.execute("""
                INSERT INTO ecommerce_events (
                    event_id, user_id, session_id, event_type, product_id,
                    category, price, quantity, total_amount, user_segment,
                    search_query, event_time, event_year, event_month,
                    event_day, event_hour, event_dayofweek, is_late_arrival,
                    source_file, source_system, processed_at
                ) VALUES (
                    %s, 1, 'test-session', 'view', 100,
                    'electronics', 0.0, 0, 0.0, 'new',
                    '', NOW(), 2026, 1, 15, 14, 4, FALSE,
                    '/test/file.csv', 'test', NOW()
                )
            """, (test_event_id,))
            db_connection.commit()
            
            # Second insert should fail
            with pytest.raises(psycopg2.IntegrityError):
                cursor.execute("""
                    INSERT INTO ecommerce_events (
                        event_id, user_id, session_id, event_type, product_id,
                        category, price, quantity, total_amount, user_segment,
                        search_query, event_time, event_year, event_month,
                        event_day, event_hour, event_dayofweek, is_late_arrival,
                        source_file, source_system, processed_at
                    ) VALUES (
                        %s, 1, 'test-session', 'view', 100,
                        'electronics', 0.0, 0, 0.0, 'new',
                        '', NOW(), 2026, 1, 15, 14, 4, FALSE,
                        '/test/file.csv', 'test', NOW()
                    )
                """, (test_event_id,))
            
            db_connection.rollback()
            
        finally:
            # Cleanup
            cursor.execute(
                "DELETE FROM ecommerce_events WHERE event_id = %s",
                (test_event_id,)
            )
            db_connection.commit()
            cursor.close()
    
    def test_invalid_event_type_rejected(self, db_connection):
        # Invalid event_type should be rejected by CHECK constraint.
        cursor = db_connection.cursor()
        
        test_event_id = f"test-invalid-{datetime.now().timestamp()}"
        
        try:
            with pytest.raises(psycopg2.IntegrityError):
                cursor.execute("""
                    INSERT INTO ecommerce_events (
                        event_id, user_id, session_id, event_type, product_id,
                        category, price, quantity, total_amount, user_segment,
                        search_query, event_time, event_year, event_month,
                        event_day, event_hour, event_dayofweek, is_late_arrival,
                        source_file, source_system, processed_at
                    ) VALUES (
                        %s, 1, 'test-session', 'INVALID_TYPE', 100,
                        'electronics', 0.0, 0, 0.0, 'new',
                        '', NOW(), 2026, 1, 15, 14, 4, FALSE,
                        '/test/file.csv', 'test', NOW()
                    )
                """, (test_event_id,))
            
            db_connection.rollback()
            
        finally:
            cursor.close()
    
    def test_negative_price_rejected(self, db_connection):
        # Negative price should be rejected by CHECK constraint.
        cursor = db_connection.cursor()
        
        test_event_id = f"test-neg-{datetime.now().timestamp()}"
        
        try:
            with pytest.raises(psycopg2.IntegrityError):
                cursor.execute("""
                    INSERT INTO ecommerce_events (
                        event_id, user_id, session_id, event_type, product_id,
                        category, price, quantity, total_amount, user_segment,
                        search_query, event_time, event_year, event_month,
                        event_day, event_hour, event_dayofweek, is_late_arrival,
                        source_file, source_system, processed_at
                    ) VALUES (
                        %s, 1, 'test-session', 'purchase', 100,
                        'electronics', -10.0, 1, -10.0, 'new',
                        '', NOW(), 2026, 1, 15, 14, 4, FALSE,
                        '/test/file.csv', 'test', NOW()
                    )
                """, (test_event_id,))
            
            db_connection.rollback()
            
        finally:
            cursor.close()


class TestDeadLetterQueue:
    # Tests for dead letter queue functionality.
    
    def test_can_insert_to_dead_letter(self, db_connection):
        """Should be able to insert invalid records to dead letter table."""
        cursor = db_connection.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO dead_letter_events (
                    event_id, user_id, event_type, product_id, price,
                    event_time, validation_errors, source_file, processed_at
                ) VALUES (
                    'test-dead-letter', NULL, 'INVALID', 100, -10.0,
                    NOW(), 'null_user_id,invalid_event_type,negative_price',
                    '/test/file.csv', NOW()
                )
            """)
            db_connection.commit()
            
            # Verify insertion
            cursor.execute(
                "SELECT COUNT(*) FROM dead_letter_events WHERE event_id = 'test-dead-letter'"
            )
            count = cursor.fetchone()[0]
            assert count >= 1
            
            # Cleanup
            cursor.execute(
                "DELETE FROM dead_letter_events WHERE event_id = 'test-dead-letter'"
            )
            db_connection.commit()
            
        finally:
            cursor.close()


class TestAnalyticsViews:
    # Tests for analytics views.
    
    def test_hourly_summary_view_exists(self, db_connection):
        # Hourly summary view should be queryable.
        cursor = db_connection.cursor()
        try:
            cursor.execute("SELECT * FROM v_hourly_event_summary LIMIT 1;")
            # Just checking it doesn't error
            cursor.fetchall()
        finally:
            cursor.close()
    
    def test_user_sessions_view_exists(self, db_connection):
        # User sessions view should be queryable.
        cursor = db_connection.cursor()
        try:
            cursor.execute("SELECT * FROM v_user_sessions LIMIT 1;")
            cursor.fetchall()
        finally:
            cursor.close()
    
    def test_category_performance_view_exists(self, db_connection):
        # Category performance view should be queryable.
        cursor = db_connection.cursor()
        try:
            cursor.execute("SELECT * FROM v_category_performance LIMIT 1;")
            cursor.fetchall()
        finally:
            cursor.close()
    
    def test_quality_summary_view_exists(self, db_connection):
        # Data quality summary view should be queryable.
        cursor = db_connection.cursor()
        try:
            cursor.execute("SELECT * FROM v_data_quality_summary LIMIT 1;")
            cursor.fetchall()
        finally:
            cursor.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "integration"])
