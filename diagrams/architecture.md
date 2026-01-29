# E-Commerce Streaming Pipeline - Architecture Diagrams

## 1. High-Level Data Flow

```mermaid
flowchart LR
    subgraph DataGeneration[" Data Generation"]
        DG[Python Generator]
        CSV[(CSV Files)]
    end
    
    subgraph SparkProcessing[" Spark Streaming"]
        READ[Read Stream]
        VALIDATE[Validate]
        TRANSFORM[Transform]
        ROUTE{Route}
    end
    
    subgraph Storage[" PostgreSQL"]
        VALID[(Valid Events)]
        DLQ[(Dead Letter Queue)]
        METRICS[(Quality Metrics)]
    end
    
    DG -->|"100 events/5s"| CSV
    CSV -->|"File watch"| READ
    READ --> VALIDATE
    VALIDATE --> TRANSFORM
    TRANSFORM --> ROUTE
    ROUTE -->|" Valid"| VALID
    ROUTE -->|" Invalid"| DLQ
    ROUTE -->|" Stats"| METRICS
```

---

## 2. Fault Tolerance Architecture

```mermaid
flowchart TD
    subgraph Request[" Request Flow"]
        REQ[Database Write]
    end
    
    subgraph RetryLogic[" Retry with Backoff"]
        R1[Attempt 1]
        R2[Attempt 2]
        R3[Attempt 3]
        WAIT1[Wait 1s]
        WAIT2[Wait 2s]
        WAIT4[Wait 4s]
    end
    
    subgraph CircuitBreaker[" Circuit Breaker"]
        CLOSED[CLOSED<br/>Normal]
        OPEN[OPEN<br/>Rejecting]
        HALFOPEN[HALF-OPEN<br/>Testing]
    end
    
    subgraph Outcome[" Outcome"]
        SUCCESS[ Success]
        DLQ[Dead Letter Queue]
        ALERT[ Alert]
    end
    
    REQ --> R1
    R1 -->|"Fail"| WAIT1
    WAIT1 --> R2
    R2 -->|"Fail"| WAIT2
    WAIT2 --> R3
    R3 -->|"Fail"| WAIT4
    
    R1 -->|"Success"| SUCCESS
    R2 -->|"Success"| SUCCESS
    R3 -->|"Success"| SUCCESS
    
    WAIT4 -->|"5 failures"| OPEN
    OPEN -->|"30s timeout"| HALFOPEN
    HALFOPEN -->|"Success"| CLOSED
    HALFOPEN -->|"Fail"| OPEN
    
    OPEN --> DLQ
    OPEN --> ALERT
```

---

## 3. Schema Evolution

```mermaid
flowchart LR
    subgraph V1[" Schema v1"]
        V1F1[event_id]
        V1F2[user_id]
        V1F3[event_type]
        V1F4[product_id]
        V1F5[price]
        V1F6[event_time]
    end
    
    subgraph V2[" Schema v2"]
        V2F1[event_id]
        V2F2[user_id]
        V2F3[session_id ]
        V2F4[event_type]
        V2F5[product_id]
        V2F6[category ]
        V2F7[price]
        V2F8[quantity ]
        V2F9[user_segment ]
        V2F10[event_time]
    end
    
    subgraph V3[" Schema v3"]
        V3NEW[+ device_type<br/>+ browser<br/>+ geo_country<br/>+ geo_city<br/>+ referrer<br/>+ campaign_id]
    end
    
    V1 -->|"migrate()"| V2
    V2 -->|"migrate()"| V3
```

---

## 4. Database Entity Relationship

```mermaid
erDiagram
    ecommerce_events {
        varchar event_id PK "UUID"
        int user_id "NULL for anonymous"
        varchar session_id
        varchar event_type "view|purchase|cart|etc"
        int product_id
        varchar category
        decimal price
        int quantity
        decimal total_amount
        varchar user_segment
        timestamp event_time
        boolean is_late_arrival
        timestamp processed_at
    }
    
    dead_letter_events {
        serial id PK
        varchar event_id
        varchar raw_data
        varchar validation_errors
        timestamp failed_at
    }
    
    data_quality_metrics {
        serial id PK
        varchar batch_id
        int total_records
        int valid_records
        decimal validity_rate
        timestamp recorded_at
    }
    
    ecommerce_events ||--o{ dead_letter_events : "rejected"
    ecommerce_events ||--o{ data_quality_metrics : "tracked"
```

---

## 5. Event Processing Pipeline

```mermaid
sequenceDiagram
    participant Gen as Data Generator
    participant FS as File System
    participant Spark as Spark Streaming
    participant Val as Validator
    participant Enrich as Enricher
    participant PG as PostgreSQL
    participant DLQ as Dead Letter Queue
    participant Mon as Monitor
    
    loop Every 5 seconds
        Gen->>FS: Write 100 events (CSV)
    end
    
    loop Every 10 seconds
        Spark->>FS: Read new files
        Spark->>Val: Validate records
        Val-->>Spark: Valid/Invalid split
        
        Spark->>Enrich: Enrich valid records
        Enrich->>PG: Write to ecommerce_events
        
        Spark->>DLQ: Route invalid to dead_letter
        
        Spark->>Mon: Report batch metrics
        Mon-->>Spark: Check thresholds
        
        alt Below threshold
            Mon->>Mon: Log alert
        end
    end
```

---

## 6. Monitoring & Alerting Flow

```mermaid
flowchart TD
    subgraph Metrics[" Metrics Collection"]
        BATCH[Batch Complete]
        VALID[Valid Count]
        INVALID[Invalid Count]
        LATENCY[Processing Time]
    end
    
    subgraph Analysis[" Analysis"]
        RATE[Calculate Validity Rate]
        THRESH[Check Thresholds]
        CONSEC[Track Consecutive Failures]
    end
    
    subgraph Alerts[" Alert Handlers"]
        CONSOLE[Console Logger]
        FILE[File Logger]
        SLACK[Slack Webhook]
    end
    
    BATCH --> VALID
    BATCH --> INVALID
    BATCH --> LATENCY
    
    VALID --> RATE
    INVALID --> RATE
    
    RATE --> THRESH
    LATENCY --> THRESH
    
    THRESH -->|"Breach"| CONSEC
    CONSEC -->|"3+ failures"| CONSOLE
    CONSEC -->|"3+ failures"| FILE
    CONSEC -->|"5+ failures"| SLACK
```

---

## How to View These Diagrams
### VS Code
Install the "Markdown Preview Mermaid Support" extension.



