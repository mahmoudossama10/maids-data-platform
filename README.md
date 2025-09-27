```mermaid
flowchart LR
  %% =============== Styles ===============
  classDef layer fill:#0b3b57,stroke:#072a40,color:#fff,stroke-width:1px,rx:6,ry:6;
  classDef process fill:#475569,stroke:#1f2937,color:#fff,stroke-width:1px,rx:6,ry:6;
  classDef table fill:#f8fafc,stroke:#94a3b8,color:#0f172a,stroke-width:1px,rx:4,ry:4;
  classDef test fill:#16a34a,stroke:#166534,color:#fff,stroke-width:1px,rx:6,ry:6;
  classDef metric fill:#7c3aed,stroke:#5b21b6,color:#fff,stroke-width:1px,rx:6,ry:6;
  classDef bi fill:#ea580c,stroke:#9a3412,color:#fff,stroke-width:1px,rx:6,ry:6;
  classDef orchestrate fill:#0ea5e9,stroke:#0369a1,color:#fff,stroke-width:1px,rx:6,ry:6;

  %% =============== Sources ===============
  subgraph S[Sources]
    direction TB
    S1[CSV: customers.csv<br/>workers.csv<br/>bookings.csv]:::table
    S2["API: Open-Meteo (weather)"]:::table
    end
    class S layer


  %% =============== Ingestion (Python) ===============
  subgraph ING[Ingestion \(Python\)]
    direction TB
    P1[generate_synthetic.py]:::process
    P2[load_csvs.py<br/><small>merge_upsert() → RAW</small>]:::process
    P3[fetch_weather.py<br/><small>upsert → RAW.WEATHER</small>]:::process
    P4[utils.py<br/><small>get_conn(), ensure_tables(), merge_upsert()</small>]:::process
  end

  %% =============== Snowflake (ANALYTICS DB) ===============
  subgraph SF[Snowflake: ANALYTICS]
    direction LR

    subgraph RAW[RAW schema]
      direction TB
      R1[RAW.CUSTOMERS]:::table
      R2[RAW.WORKERS]:::table
      R3[RAW.BOOKINGS]:::table
      R4[RAW.WEATHER]:::table
    end
    class RAW layer

    subgraph STG[STAGING schema]
      direction TB
      T1[stg_customers (view)]:::table
      T2[stg_workers (view)]:::table
      T3[stg_bookings (view)]:::table
      T4[stg_weather (view)]:::table
    end
    class STG layer

    subgraph MARTS[MARTS schema]
      direction TB
      M1[dim_customer (table)]:::table
      M2[dim_worker (table)]:::table
      M3[fact_bookings (incremental MERGE)]:::table
      M4[metrics_daily (table)]:::table
      M5[anomalies_daily (table)]:::table
    end
    class MARTS layer

    subgraph OPS[OPS schema]
      direction TB
      O1[OPS.INGESTION_WATERMARKS]:::table
    end
    class OPS layer
  end

  %% =============== dbt ===============
  subgraph DBT[dbt]
    direction TB
    D1[dbt run<br/><small>staging → marts</small>]:::process
    D2[dbt test<br/><small>unique, not_null, relationships, freshness</small>]:::test
    D3[dbt docs<br/><small>lineage, catalog</small>]:::process
  end
  class DBT layer

  %% =============== Orchestration ===============
  subgraph ORCH[Orchestration]
    direction TB
    OX[Prefect flow: elt_pipeline<br/><small>generate_data → load_csvs → fetch_weather → dbt run/test</small>]:::orchestrate
    OY[run_all.py<br/><small>simple fallback runner</small>]:::orchestrate
  end
  class ORCH layer

  %% =============== BI / Consumption ===============
  subgraph BI[BI / Consumption]
    direction TB
    B1[Tableau: Executive Ops<br/><small>KPIs, trends by city/channel</small>]:::bi
    B2[Tableau: Data Health<br/><small>anomalies, freshness</small>]:::bi
  end
  class BI layer

  %% =============== Flows ===============
  %% Sources → Ingestion
  S1 -->|reads| P1
  P1 -->|writes CSV| S1
  S1 -->|read & upsert| P2
  S2 -->|fetch| P3

  %% Ingestion → Snowflake RAW
  P2 -->|MERGE by keys| R1
  P2 -->|MERGE by keys| R2
  P2 -->|MERGE by keys| R3
  P3 -->|MERGE by (city,date)| R4
  P4 -. used by .- P2
  P4 -. used by .- P3

  %% RAW → STAGING (dbt)
  R1 --> T1
  R2 --> T2
  R3 --> T3
  R4 --> T4

  %% STAGING → MARTS (dbt)
  T1 --> M1
  T2 --> M2
  T3 --> M3
  T3 --> M4
  T4 --> M4
  M4 --> M5

  %% Orchestration triggers
  OX -->|run| P1
  OX -->|run| P2
  OX -->|run| P3
  OX -->|run| D1
  OX -->|run| D2
  OY -->|run| P1
  OY -->|run| P2
  OY -->|run| P3
  OY -->|run| D1
  OY -->|run| D2

  %% dbt processes operate on Snowflake
  D1 --> STG
  D1 --> MARTS
  D2 --> MARTS
  D3 --> MARTS

  %% Consumption
  MARTS -->|Snowflake live/extract| B1
  MARTS -->|Snowflake live/extract| B2