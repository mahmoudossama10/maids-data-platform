flowchart TB
  subgraph Sources
    A1[CSV Files]
    A2[API Data]
  end

  subgraph Snowflake
    B1[RAW Layer]
    B2[STAGING Layer]
    B3[MARTS Layer]
  end

  subgraph dbt
    C1[Transformations]
    C2[Tests]
    C3[Anomaly Detection]
  end

  subgraph BI
    D1[Tableau Dashboards]
  end

  subgraph Orchestration
    E1[Prefect Flow / run_all.py]
  end

  %% Connections
  A1 --> B1
  A2 --> B1
  B1 --> B2
  B2 --> B3
  B3 --> D1
  B3 --> C1
  C1 --> C2
  C1 --> C3
  A1 --> E1
  A2 --> E1
  E1 --> B1
