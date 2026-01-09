NYC Traffic Analysis Pipeline (TMAS + Weather + Incidents)
==========================================================

Project Overview
----------------

This project implements an **end-to-end data engineering pipeline** to analyze urban traffic patterns in **New York City (2020)** by integrating:

*   Traffic volume data (TMAS)
    
*   Historical weather conditions
    
*   Motor vehicle collision incidents
    

The objective is to **leverage urban mobility data** to identify traffic patterns, congestion proxies, and incident-prone areas, supporting **urban planning and smarter transportation solutions**.

The pipeline follows modern data engineering practices including:

*   Distributed-style storage with Parquet
    
*   Processing with Apache Spark
    
*   Workflow orchestration using Apache Airflow
    
*   Interactive geospatial analysis in Power BI
    

Objectives
----------

*   Perform **geospatial analysis** of traffic and incidents
    
*   Enable **interactive exploration** by time, location, and weather
    
*   Integrate heterogeneous public datasets into a unified analytical model
    
*   Demonstrate scalable data architecture aligned with HDFS-style storage
    
*   Apply feature engineering to enrich traffic analytics
    

Architecture Overview
---------------------

The project is organized using a **data lake–style layered architecture**:

Plain textANTLR4BashCC#CSSCoffeeScriptCMakeDartDjangoDockerEJSErlangGitGoGraphQLGroovyHTMLJavaJavaScriptJSONJSXKotlinLaTeXLessLuaMakefileMarkdownMATLABMarkupObjective-CPerlPHPPowerShell.propertiesProtocol BuffersPythonRRubySass (Sass)Sass (Scss)SchemeSQLShellSwiftSVGTSXTypeScriptWebAssemblyYAMLXML`   data/  ├── raw/            # Raw extracted data (as-is)  │   ├── traffic/  │   ├── stations/  │   ├── weather/  │   └── incidents/  │  ├── processed/      # Cleaned and standardized datasets  │   ├── traffic/  │   ├── stations/  │   ├── weather/  │   └── incidents/  │  ├── curated/        # Fully integrated datasets ready for analytics  │    ├── incidents_geospatial/  │    ├── traffic_enriched/  │    ├── traffic_weather_enriched/  │    └── traffic_weather_incidents/  │  ├── src/            # Processing of data  │   ├── airflow/  │   ├── extract/  │   ├── transform/  │   └── incidents/   `

Although the project runs on a local filesystem, the use of:

*   **Partitioned Parquet files**
    
*   **Columnar storage**
    
*   **Spark-compatible layout**
    

makes the solution **fully portable to HDFS** without architectural changes.

Technologies Used
-----------------

*   **Python 3**
    
*   **Apache Spark (PySpark)** – large-scale data processing
    
*   **Apache Airflow** – pipeline orchestration
    
*   **Parquet** – efficient columnar storage
    
*   **Power BI** – visualization & geospatial analytics
    
*   **Open-Meteo API** – historical weather data
    
*   **NYC Open Data (Socrata API)** – traffic incidents
    

Data Pipeline
-------------

### 1\. Data Extraction

*   TMAS traffic volume data (hourly counts per station)
    
*   TMAS station metadata (geolocation, urban/rural classification)
    
*   Historical weather data (daily aggregates)
    
*   NYC motor vehicle collision records
    

### 2\. Data Cleaning & Preprocessing

Performed using **PySpark**:

*   Removal of irrelevant or noisy columns
    
*   Type normalization
    
*   Null handling with domain-specific logic
    
*   **Domain-aware duplicate handling**:
    
    *   Traffic duplicates aggregated (lane/direction partial counts)
        
    *   Incident duplicates removed (true duplicates)
        

### 3\. Feature Engineering

Key engineered features include:

*   Temporal features (date, day of week)
    
*   Weather categories (snow day, precipitation intensity)
    
*   Aggregated traffic volumes (hourly & daily)
    
*   Urban vs rural station classification
    

> Feature engineering was performed across Spark transformations and Power BI calculated columns/measures.

### 4\. Data Integration

Sequential joins:

1.  Traffic + Stations (spatial enrichment)
    
2.  Traffic + Weather (temporal enrichment)
    
3.  Final join with Incidents (impact analysis)
    

Workflow Orchestration (Airflow)
--------------------------------

The entire pipeline is orchestrated with **Apache Airflow**, including:

*   Independent extract tasks
    
*   Parallel cleaning stages
    
*   Controlled integration dependencies
    
*   Retry logic and failure isolation
    

**DAG:** nyc\_traffic\_2020\_pipeline

This demonstrates production-style workflow management and reproducibility.

Analytics & Visualization (Power BI)
------------------------------------

The final datasets were exported to CSV for easy ingestion into Power BI.

### Implemented Analyses:

*   **Geospatial traffic volume maps**
    
*   **Incident density visualization**
    
*   Traffic patterns by:
    
    *   Hour
        
    *   Day of week
        
    *   Weather conditions
        
*   Comparative analysis between snow / non-snow days
    

### Key Measures:

*   Average daily vehicle volume
    
*   Average hourly vehicle volume
    
*   Total vehicle volume
    
*   Average incidents per day
    
*   Days analyzed
    

### Interactive Elements:

*   Time slicers
    
*   Weather filters
    
*   Map-based exploration
    
*   Category-based segmentation
    

Datasets & External Resources
-----------------------------

### Data Sources

*   **TMAS Traffic & Stations**
    
    *   [https://data.transportation.gov/Roadways-and-Bridges/Traffic-Monitoring-Analysis-System-TMAS-Traffic-Vo/ymmm-mwzp/about\_data](https://data.transportation.gov/Roadways-and-Bridges/Traffic-Monitoring-Analysis-System-TMAS-Traffic-Vo/ymmm-mwzp/about_data)
        
    *   [https://geodata.bts.gov/datasets/usdot::travel-monitoring-analysis-system-stations/about](https://geodata.bts.gov/datasets/usdot::travel-monitoring-analysis-system-stations/about)
        
*   **NYC Motor Vehicle Collisions**
    
    *   [https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about\_data](https://data.cityofnewyork.us/Public-Safety/Motor-Vehicle-Collisions-Crashes/h9gi-nx95/about_data)
        
*   **Historical Weather (Open-Meteo)**
    
    *   [https://open-meteo.com/](https://open-meteo.com/)
        

### Power BI Dashboard

[https://fiunamedu-my.sharepoint.com/:u:/g/personal/elias\_basaldua\_fi\_unam\_edu/IQAdx1tRvc83SZqNpHAIBP2GAQkczoipvQw9kvOiWsQPEbU?e=jg2g5F](https://fiunamedu-my.sharepoint.com/:u:/g/personal/elias_basaldua_fi_unam_edu/IQAdx1tRvc83SZqNpHAIBP2GAQkczoipvQw9kvOiWsQPEbU?e=jg2g5F)

Supporting Documentation
------------------------

A detailed technical and analytical report accompanies this repository, covering:

*   Design decisions
    
*   Trade-offs
    
*   Domain-specific reasoning
    
*   Limitations and future improvements
    

**Document:**Urban\_Mobility\_Traffic\_Data\_Pipeline\_NYC\_2020.docx (English document)

Pipeline\_de\_Datos\_para\_Movilidad\_Urbana\_NYC\_2020.docx (Spanish document)

This README provides operational context, while the document serves as a **technical design report**.

Author
------

**Elias Basaldua Garcia**

2026