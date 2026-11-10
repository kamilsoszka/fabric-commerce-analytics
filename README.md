# Microsoft Fabric Commerce Analytics

End-to-end sales analytics solution built with Microsoft Fabric, featuring a Bronze-Silver-Gold architecture, PySpark transformations, a Direct Lake semantic model, Power BI reporting, and Git integration.

## Project Overview

This project demonstrates a complete analytics workflow built with Microsoft Fabric and AdventureWorks data. It covers source ingestion, Lakehouse storage, data transformation, dimensional modeling, semantic modeling, reporting, application delivery, data-quality validation, and source control.

## Microsoft Fabric Workspace

The workspace contains the complete analytics solution, including the source SQL database, ingestion job, Lakehouse, transformation notebook, Direct Lake semantic model, Power BI report, and organizational app.

![Microsoft Fabric workspace overview](docs/images/fabric-workspace-overview.png)

## Architecture

The project implements a medallion architecture:

- **Bronze:** raw AdventureWorks source tables
- **Silver:** cleaned and validated sales data
- **Gold:** dimensional star schema optimized for analytics

## Data Flow

```text
AdventureWorks SQL Database
        |
        v
Fabric Copy Job
        |
        v
Bronze tables
        |
        v
PySpark transformations
        |
        v
Silver sales table
        |
        v
Gold star schema
        |
        v
Direct Lake semantic model
        |
        v
Power BI report
        |
        v
Fabric Org App
```

## Components

- Microsoft Fabric SQL Database
- Fabric Copy Job
- Fabric Lakehouse
- PySpark Notebook
- Delta tables
- Direct Lake semantic model
- DAX measures
- Power BI report
- Fabric Org App
- GitHub source control

## Analytical Model

The Gold layer contains:

- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_date`
- `gold.fact_sales`
- `gold.dax`

The semantic model includes active relationships for customers, products, and order dates, together with inactive relationships for due dates and ship dates.

![Direct Lake semantic model](docs/images/02_direct-lake-semantic-model.png)

## Data Quality

The transformation notebook validates:

- source schemas
- required columns
- row counts
- unique sales-line identifiers
- duplicate records
- null values
- quantities, prices, and discounts
- calculated sales totals
- fact-to-dimension relationships
- Silver-to-Gold reconciliation

Validation failures stop processing before invalid data reaches the analytical model.

## Power BI Report

The `CommerceSalesReport` report presents:

- total sales
- total orders
- total quantity
- customer count
- top customers
- top products
- sales by product color

![Power BI report key insights](docs/images/01_power-bi-report-key-insights.png)

## Business Questions

The report supports analysis of the following questions:

- Which customers generate the highest sales?
- Which products contribute most to revenue and order volume?
- How does sales performance change over time?
- Which product colors and categories drive commercial performance?
- Are sales records complete and consistent across the Silver and Gold layers?

## Repository Structure

```text
fabric-commerce-analytics/
|-- AdventureWorksSource.SQLDatabase/
|-- Commerce Analytics App.OrgApp/
|-- CommerceLakehouse.Lakehouse/
|-- CommerceSalesReport.Report/
|-- CommerceSemanticModel.SemanticModel/
|-- LoadAdventureWorks.CopyJob/
|-- TransformAdventureWorks.Notebook/
|-- docs/
|   `-- images/
|       |-- 01_power-bi-report-key-insights.png
|       |-- 02_direct-lake-semantic-model.png
|       `-- fabric-workspace-overview.png
|-- LICENSE
`-- README.md
```

Fabric-generated folders contain the source definitions for the SQL database, Lakehouse, Copy Job, notebook, semantic model, report, and Org App.

## Key Skills Demonstrated

- Microsoft Fabric workspace design
- medallion architecture implementation
- PySpark data transformation
- Delta table management
- dimensional modeling and star-schema design
- Direct Lake semantic modeling
- DAX measure development
- Power BI report development
- automated data-quality validation
- Git-based source control for Fabric artifacts

## License

This project is licensed under the terms specified in the [LICENSE](LICENSE) file.
