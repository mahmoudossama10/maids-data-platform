```mermaid
flowchart LR
  A[CSV + API Sources] --> B[Snowflake RAW]
  B --> C[dbt STAGING]
  C --> D[dbt MARTS (facts, dims, metrics)]
  D --> E[Tableau Dashboards]
  D --> F[dbt Tests + Anomaly Detection]
  subgraph Orchestration
    G[Prefect Flow / run_all.py]
  end
  A --> G --> B
