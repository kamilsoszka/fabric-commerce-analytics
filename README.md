# Microsoft Fabric Commerce Analytics

An end-to-end commerce analytics solution built with Microsoft Fabric, featuring a Fabric SQL Database, Copy Job ingestion, a Bronze-Silver-Gold Lakehouse architecture, validated PySpark transformations, a Direct Lake semantic model, Power BI reporting, a Fabric organizational app, and GitHub source control.

## Solution overview

The solution ingests AdventureWorks commerce data from a Microsoft Fabric SQL Database, loads source tables into the Bronze layer, transforms and validates the data in PySpark, creates a dimensional Gold model, and exposes the analytical model through Direct Lake for Power BI reporting.

The implementation demonstrates a complete analytics lifecycle:

1. source-data management
2. data ingestion
3. medallion-layer transformations
4. automated data-quality validation
5. dimensional modeling
6. semantic modeling
7. business reporting
8. organizational application publishing
9. Git-based version control

## Components

| Component | Fabric item | Purpose |
|---|---|---|
| Source database | `AdventureWorksSource` | Stores the AdventureWorks source tables |
| Ingestion job | `LoadAdventureWorks` | Copies source data into the Bronze layer |
| Lakehouse | `CommerceLakehouse` | Stores Bronze, Silver, and Gold Delta tables |
| Transformation notebook | `TransformAdventureWorks` | Transforms, validates, and persists analytical data |
| Semantic model | `CommerceSemanticModel` | Exposes the Gold star schema through Direct Lake |
| Power BI report | `CommerceSalesReport` | Presents commerce KPIs and analytical breakdowns |
| Organizational app | `Commerce Analytics App` | Publishes the report through a Fabric Org App |
| Source control | GitHub integration | Versions supported Microsoft Fabric artifacts |

## Microsoft Fabric workspace

The Microsoft Fabric workspace contains the complete analytics solution, including the source SQL database, Copy Job, Lakehouse, transformation notebook, Direct Lake semantic model, Power BI report, and organizational app.

![Microsoft Fabric workspace overview](docs/images/03-fabric-workspace-overview.png)

## Architecture

The project implements a medallion architecture with three logical data layers.

### Bronze layer

The Bronze layer stores source-aligned AdventureWorks tables loaded by `LoadAdventureWorks`.

The ingestion process includes the following source entities:

- customers
- products
- product categories
- sales-order headers
- sales-order details

Bronze tables preserve the source structure and provide the input for PySpark transformations.

### Silver layer

The Silver layer contains the validated `silver.sales` table.

The grain is:

> One row represents one sales-order detail line.

The business key is:

```text
sales_order_detail_id
```

The Silver transformation:

- joins sales-order details with order headers
- adds customer attributes
- adds product attributes
- adds product-category attributes
- standardizes names and data types
- calculates sales-line totals
- validates required values
- rejects duplicate sales-line identifiers
- applies business-rule checks
- reconciles source and target row counts

### Gold layer

The Gold layer contains the dimensional model used by the Direct Lake semantic model:

- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_date`
- `gold.fact_sales`
- `gold.DAX`

The fact-table grain is:

> One row represents one sales-order detail line.

The principal relationships are:

- `gold.dim_customer[customer_id]` to `gold.fact_sales[customer_id]`
- `gold.dim_product[product_id]` to `gold.fact_sales[product_id]`
- `gold.dim_date[date_key]` to the order, due, and ship date keys in `gold.fact_sales`

### Data flow

```text
AdventureWorksSource
        |
        v
LoadAdventureWorks
        |
        v
Bronze source-aligned tables
        |
        v
TransformAdventureWorks
        |
        v
silver.sales
        |
        v
Gold dimensional star schema
        |
        v
CommerceSemanticModel
        |
        v
CommerceSalesReport
        |
        v
Commerce Analytics App
```

## Analytical model

The analytical model follows a star-schema design.

### Customer dimension

`gold.dim_customer` contains one row per customer and includes:

- customer identifier
- customer name
- company name

### Product dimension

`gold.dim_product` contains one row per product and includes:

- product identifier
- product name
- product number
- product color
- product-category identifier
- product-category name

### Date dimension

`gold.dim_date` contains a continuous date range covering order, due, and ship dates.

The dimension includes:

- integer date key in `YYYYMMDD` format
- full date
- year
- quarter
- month
- year-month
- week number
- day number
- weekday
- weekend indicator

### Sales fact table

`gold.fact_sales` contains:

- sales-order detail identifier
- sales-order identifier
- order date key
- due date key
- ship date key
- customer identifier
- product identifier
- order status
- order quantity
- unit price
- unit-price discount
- sales-line total

### Semantic-model relationships

The Direct Lake semantic model includes:

- an active customer relationship
- an active product relationship
- an active order-date relationship
- inactive due-date and ship-date relationships

Filtering flows from the dimensions to the fact table.

![Direct Lake semantic model](docs/images/02-direct-lake-semantic-model.png)

## Data-quality validation

The `TransformAdventureWorks` notebook performs fail-fast validation before and after writing the Silver and Gold tables.

The implemented checks include:

- required source-table validation
- required source-column validation
- source business-key validation
- source-to-Silver row-count reconciliation
- sales-line grain validation
- duplicate-key detection
- required-value validation
- quantity validation
- price validation
- discount-range validation
- negative-total validation
- order, due, and ship date validation
- independent recalculation of sales-line totals
- expected Silver column validation
- expected Silver data-type validation
- persisted Silver row-count validation
- persisted Silver schema validation
- dimension-key uniqueness validation
- conflicting dimension-attribute detection
- fact-table grain validation
- customer relationship validation
- product relationship validation
- order-date relationship validation
- due-date relationship validation
- ship-date relationship validation
- persisted Gold table validation
- Silver-to-Gold quantity reconciliation
- Silver-to-Gold sales reconciliation

Target Delta tables are overwritten during sequential executions. This prevents duplicate records when the notebook is executed repeatedly with the same source data.

## Validated execution results

The validated execution produced the following results:

| Metric | Result |
|---|---:|
| Bronze sales-detail rows | 542 |
| Silver sales rows | 542 |
| Gold fact rows | 542 |
| Unique sales-line identifiers | 542 |
| Duplicate sales-line identifiers | 0 |
| Orders | 32 |
| Customers | 32 |
| Products represented in sales | 142 |
| Product categories represented in sales | 26 |
| Total quantity | 2,087 |
| Total sales | $708,690.07 |
| Silver-to-Gold reconciliation | Passed |

The validated sales data covers order activity dated `2025-06-01`. The date dimension extends through `2025-06-13` because it also covers due and ship dates.

## Power BI report

`CommerceSalesReport` uses the Direct Lake semantic model to present the following KPI cards:

1. Total Sales
2. Total Quantity
3. Total Orders
4. Customers
5. Products
6. Product Categories

The report also includes:

- Sales by Product Category
- Sales by Product Color
- Top 3 Customers
- Top 3 Products
- Top 3 Colors

The report was reconciled against the validated Gold results:

| Report KPI | Validated value |
|---|---:|
| Total Sales | $708,690.07 |
| Total Quantity | 2,087 |
| Total Orders | 32 |
| Customers | 32 |
| Products | 142 |
| Product Categories | 26 |

![Power BI report key insights](docs/images/01-power-bi-report-key-insights.png)

## Fabric organizational app

The report is published through `Commerce Analytics App`.

The published application exposes the current report version and preserves the validated KPI values and analytical visuals.

Access is intentionally restricted for the portfolio implementation. No additional users or groups are required to reproduce the technical solution in a private development workspace.

## Repository structure

```text
.
├── AdventureWorksSource.SQLDatabase/
├── Commerce Analytics App.OrgApp/
├── CommerceLakehouse.Lakehouse/
├── CommerceSalesReport.Report/
├── CommerceSemanticModel.SemanticModel/
├── LoadAdventureWorks.CopyJob/
├── TransformAdventureWorks.Notebook/
├── docs/
│   └── images/
│       ├── 01-power-bi-report-key-insights.png
│       ├── 02-direct-lake-semantic-model.png
│       └── 03-fabric-workspace-overview.png
├── LICENSE
└── README.md
```

## Prerequisites

Reproducing the project requires:

- a Microsoft Fabric tenant
- an active Fabric capacity or eligible Fabric trial capacity
- permission to create a Fabric workspace
- permission to create a Fabric SQL Database
- permission to create and execute Copy Jobs
- ports
- a GitHub repository available for Fabric Git integration
- access to compatible AdventureWorks source data

The SQL source data is not reconstructed solely by synchronizing the repository. The AdventureWorks source database must exist or be restored separately before running the ingestion process.

## Reproduce the project

### 1. Create the workspace

Create a Microsoft Fabric workspace assigned to an active Fabric capacity.

### 2. Connect source control

Connect the workspace to the repository and the `main` branch through Microsoft Fabric Git integration.

Synchronize the supported Fabric items into the workspace.

### 3. Prepare the source database

Create or restore `AdventureWorksSource`.

Confirm that the required source tables contain the columns expected by `TransformAdventureWorks`, including product-category identifiers.

### 4. Configure the Copy Job

Open `LoadAdventureWorks` and verify:

- source connection
- destination Lakehouse
- Bronze target schema
- table mappings
- authentication
- incremental-copy configuration, if applicable

Connection credentials and environment-specific secrets are not stored in the repository and must be configured in the target Fabric environment.

### 5. Run ingestion

Run:

```text
LoadAdventureWorks
```

Confirm that all source-table copy activities succeed and that the read and written row counts reconcile.

### 6. Run transformations

Attach `CommerceLakehouse` as the default Lakehouse for the notebook.

Run:

```text
TransformAdventureWorks
```

The notebook must complete without validation exceptions.

Confirm that the following targets exist:

```text
silver.sales
gold.dim_customer
gold.dim_product
gold.dim_date
gold.fact_sales
gold.DAX
```

### 7. Verify the semantic model

Open `CommerceSemanticModel` and refresh the model metadata if required.

Verify that:

- `dim_product` includes `product_category_id`
- `dim_product` includes `product_category_name`
- dimension-to-fact relationships use one-to-many cardinality
- filter direction flows from dimensions to the fact table
- the order-date relationship is active
- due-date and ship-date relationships are inactive

### 8. Verify the report

Open `CommerceSalesReport` and confirm that the report values reconcile with the validated Gold results.

Expected results for the documented dataset are:

```text
Total Sales: $708,690.07
Total Quantity: 2,087
Total Orders: 32
Customers: 32
Products: 142
Product Categories: 26
```

### 9. Publish the application

Publish or update `Commerce Analytics App`.

Open the published application in the audience view and verify that the current report layout and KPI values are visible.

### 10. Verify Git synchronization

Return to the Fabric workspace and confirm:

- no pending workspace changes
- all supported artifacts show `Synced`
- the workspace is connected to the expected branch

## Known limitations

- The documented source dataset contains order activity for one order date: `2025-06-01`.
- The dataset is sufficient for pipeline, modeling, validation, and reporting demonstrations, but it is not suitable for meaningful long-term trend analysis.
- Copy Job credentials and other environment-specific connection settings are not stored in Git.
- Fabric Git synchronization does not independently restore source data or credentials.
- Sequential notebook reruns are idempotent through Delta-table overwrite operations.
- Concurrent notebook executions are not explicitly controlled and should be avoided.
- The Gold dimensions represent current-state attributes and do not implement slowly changing dimension history.
- The portfolio implementation uses a restricted access model rather than a production audience configuration.
- Deployment pipelines and automated CI/CD validation are outside the current project scope.

## Skills demonstrated

- Microsoft Fabric
- Fabric SQL Database
- Copy Job ingestion
- Lakehouse architecture
- Bronze-Silver-Gold medallion design
- PySpark
- Delta tables
- schema validation
- data-quality validation
- dimensional modeling
- star-schema design
- Direct Lake
- DAX
- Power BI
- Fabric organizational apps
- Git integration
- GitHub source control
- analytical reconciliation
- technical documentation

## License

This project is licensed under the terms specified in the LICENSE file.
