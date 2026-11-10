# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "d99dfe8a-81cb-4e39-9c51-28f1a1120ecf",
# META       "default_lakehouse_name": "CommerceLakehouse",
# META       "default_lakehouse_workspace_id": "8f884394-33ba-4d62-bed2-a3f3e69c2dc0",
# META       "known_lakehouses": [
# META         {
# META           "id": "d99dfe8a-81cb-4e39-9c51-28f1a1120ecf"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType
)


# ============================================================
# AdventureWorks Bronze-to-Silver-to-Gold Transformation
#
# Source:
# - bronze.customer
# - bronze.product
# - bronze.product_category
# - bronze.sales_order_header
# - bronze.sales_order_detail
#
# Targets:
# - silver.sales
# - gold.dim_customer
# - gold.dim_product
# - gold.dim_date
# - gold.fact_sales
# - gold.DAX
#
# The script is idempotent for sequential executions.
# Target Delta tables are replaced during every successful run.
# ============================================================


# ------------------------------------------------------------
# 1. Utility functions
# ------------------------------------------------------------

def validate_required_columns(table_name, dataframe, required_columns):
    actual_columns = set(dataframe.columns)

    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in actual_columns
    ]

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing required columns: "
            f"{missing_columns}"
        )


def validate_column_types(
    table_name,
    dataframe,
    expected_column_types
):
    actual_types = {
        field.name: field.dataType
        for field in dataframe.schema.fields
    }

    type_errors = {}

    for column_name, expected_type in expected_column_types.items():
        actual_type = actual_types.get(column_name)

        if actual_type is None:
            type_errors[column_name] = {
                "expected": str(expected_type),
                "actual": "missing"
            }
        elif actual_type != expected_type:
            type_errors[column_name] = {
                "expected": str(expected_type),
                "actual": str(actual_type)
            }

    if type_errors:
        raise ValueError(
            f"{table_name} contains unexpected column types: "
            f"{type_errors}"
        )


def validate_non_null_columns(
    dataset_name,
    dataframe,
    required_columns
):
    null_summary = dataframe.agg(
        *[
            F.sum(
                F.when(
                    F.col(column_name).isNull(),
                    1
                ).otherwise(0)
            ).alias(column_name)
            for column_name in required_columns
        ]
    ).first().asDict()

    columns_with_nulls = {
        column_name: null_count
        for column_name, null_count in null_summary.items()
        if null_count > 0
    }

    if columns_with_nulls:
        raise ValueError(
            f"{dataset_name} contains null values in required columns: "
            f"{columns_with_nulls}"
        )


def validate_unique_key(
    dataset_name,
    dataframe,
    key_column
):
    row_count = dataframe.count()

    distinct_key_count = (
        dataframe
        .select(key_column)
        .distinct()
        .count()
    )

    null_key_count = (
        dataframe
        .filter(F.col(key_column).isNull())
        .count()
    )

    if null_key_count > 0:
        raise ValueError(
            f"{dataset_name} contains {null_key_count} null values "
            f"in key column {key_column}."
        )

    if row_count != distinct_key_count:
        duplicated_keys = (
            dataframe
            .groupBy(key_column)
            .count()
            .filter(F.col("count") > 1)
        )

        display(duplicated_keys)

        raise ValueError(
            f"{dataset_name} contains duplicated keys: "
            f"rows={row_count}, "
            f"distinct_keys={distinct_key_count}"
        )


def validate_dimension_attribute_consistency(
    dataset_name,
    dataframe,
    key_column,
    attribute_columns
):
    distinct_versions = (
        dataframe
        .select(
            key_column,
            F.struct(
                *[
                    F.col(column_name)
                    for column_name in attribute_columns
                ]
            ).alias("attribute_version")
        )
        .distinct()
        .groupBy(key_column)
        .agg(
            F.count("*").alias("attribute_version_count")
        )
        .filter(F.col("attribute_version_count") > 1)
    )

    conflicting_key_count = distinct_versions.count()

    if conflicting_key_count > 0:
        display(distinct_versions)

        raise ValueError(
            f"{dataset_name} contains {conflicting_key_count} keys "
            f"with conflicting attribute versions."
        )


def validate_persisted_table(
    table_name,
    expected_dataframe,
    expected_columns
):
    persisted_dataframe = spark.table(table_name)

    expected_count = expected_dataframe.count()
    actual_count = persisted_dataframe.count()

    if expected_count != actual_count:
        raise ValueError(
            f"Persisted row-count validation failed for {table_name}: "
            f"expected={expected_count}, actual={actual_count}"
        )

    actual_columns = persisted_dataframe.columns

    if actual_columns != expected_columns:
        raise ValueError(
            f"Persisted schema validation failed for {table_name}: "
            f"expected={expected_columns}, actual={actual_columns}"
        )

    return persisted_dataframe


# ------------------------------------------------------------
# 2. Read Bronze source tables
# ------------------------------------------------------------

customers = spark.table("bronze.customer")
products = spark.table("bronze.product")
categories = spark.table("bronze.product_category")
orders = spark.table("bronze.sales_order_header")
order_details = spark.table("bronze.sales_order_detail")


# ------------------------------------------------------------
# 3. Validate Bronze source schemas
# ------------------------------------------------------------

required_source_columns = {
    "bronze.customer": [
        "CustomerID",
        "FirstName",
        "LastName",
        "CompanyName"
    ],
    "bronze.product": [
        "ProductID",
        "Name",
        "ProductNumber",
        "Color",
        "ProductCategoryID"
    ],
    "bronze.product_category": [
        "ProductCategoryID",
        "Name"
    ],
    "bronze.sales_order_header": [
        "SalesOrderID",
        "OrderDate",
        "DueDate",
        "ShipDate",
        "Status",
        "CustomerID"
    ],
    "bronze.sales_order_detail": [
        "SalesOrderID",
        "SalesOrderDetailID",
        "OrderQty",
        "ProductID",
        "UnitPrice",
        "UnitPriceDiscount"
    ]
}

source_dataframes = {
    "bronze.customer": customers,
    "bronze.product": products,
    "bronze.product_category": categories,
    "bronze.sales_order_header": orders,
    "bronze.sales_order_detail": order_details
}

for table_name, required_columns in required_source_columns.items():
    validate_required_columns(
        table_name,
        source_dataframes[table_name],
        required_columns
    )

print("Bronze source-schema validation passed.")


# ------------------------------------------------------------
# 4. Validate Bronze business keys
# ------------------------------------------------------------

validate_unique_key(
    "bronze.customer",
    customers,
    "CustomerID"
)

validate_unique_key(
    "bronze.product",
    products,
    "ProductID"
)

validate_unique_key(
    "bronze.product_category",
    categories,
    "ProductCategoryID"
)

validate_unique_key(
    "bronze.sales_order_header",
    orders,
    "SalesOrderID"
)

validate_unique_key(
    "bronze.sales_order_detail",
    order_details,
    "SalesOrderDetailID"
)

print("Bronze business-key validation passed.")


# ------------------------------------------------------------
# 5. Build the Silver sales dataset
#
# Grain:
# One row represents one sales order detail line.
#
# Business key:
# sales_order_detail_id
# ------------------------------------------------------------

sales = (
    order_details.alias("d")
    .join(
        orders.alias("h"),
        F.col("d.SalesOrderID") == F.col("h.SalesOrderID"),
        "inner"
    )
    .join(
        products.alias("p"),
        F.col("d.ProductID") == F.col("p.ProductID"),
        "left"
    )
    .join(
        categories.alias("pc"),
        F.col("p.ProductCategoryID")
        == F.col("pc.ProductCategoryID"),
        "left"
    )
    .join(
        customers.alias("c"),
        F.col("h.CustomerID") == F.col("c.CustomerID"),
        "left"
    )
    .select(
        F.col("h.SalesOrderID")
        .cast("long")
        .alias("sales_order_id"),

        F.col("d.SalesOrderDetailID")
        .cast("long")
        .alias("sales_order_detail_id"),

        F.to_date(F.col("h.OrderDate"))
        .alias("order_date"),

        F.to_date(F.col("h.DueDate"))
        .alias("due_date"),

        F.to_date(F.col("h.ShipDate"))
        .alias("ship_date"),

        F.col("h.Status")
        .cast("integer")
        .alias("order_status"),

        F.col("h.CustomerID")
        .cast("long")
        .alias("customer_id"),

        F.trim(
            F.concat_ws(
                " ",
                F.col("c.FirstName"),
                F.col("c.LastName")
            )
        ).alias("customer_name"),

        F.col("c.CompanyName")
        .cast("string")
        .alias("company_name"),

        F.col("d.ProductID")
        .cast("long")
        .alias("product_id"),

        F.col("p.Name")
        .cast("string")
        .alias("product_name"),

        F.col("p.ProductNumber")
        .cast("string")
        .alias("product_number"),

        F.col("p.Color")
        .cast("string")
        .alias("product_color"),

        F.col("p.ProductCategoryID")
        .cast("long")
        .alias("product_category_id"),

        F.col("pc.Name")
        .cast("string")
        .alias("product_category_name"),

        F.col("d.OrderQty")
        .cast("integer")
        .alias("order_quantity"),

        F.col("d.UnitPrice")
        .cast("decimal(18,4)")
        .alias("unit_price"),

        F.col("d.UnitPriceDiscount")
        .cast("decimal(18,4)")
        .alias("unit_price_discount"),

        (
            F.col("d.OrderQty")
            * F.col("d.UnitPrice")
            * (
                F.lit(1)
                - F.col("d.UnitPriceDiscount")
            )
        )
        .cast("decimal(18,2)")
        .alias("line_total")
    )
)


# ------------------------------------------------------------
# 6. Validate the Silver row count and grain
# ------------------------------------------------------------

source_detail_count = order_details.count()
silver_source_count = sales.count()

if source_detail_count != silver_source_count:
    raise ValueError(
        "Silver row-count validation failed: "
        f"bronze.sales_order_detail={source_detail_count}, "
        f"transformed={silver_source_count}"
    )

validate_unique_key(
    "silver.sales candidate",
    sales,
    "sales_order_detail_id"
)


# ------------------------------------------------------------
# 7. Validate required Silver values
# ------------------------------------------------------------

required_silver_columns = [
    "sales_order_id",
    "sales_order_detail_id",
    "order_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "product_number",
    "product_category_id",
    "product_category_name",
    "order_quantity",
    "unit_price",
    "unit_price_discount",
    "line_total"
]

validate_non_null_columns(
    "silver.sales candidate",
    sales,
    required_silver_columns
)


# ------------------------------------------------------------
# 8. Validate Silver business rules
# ------------------------------------------------------------

business_rule_summary = (
    sales
    .agg(
        F.sum(
            F.when(
                F.col("order_quantity") <= 0,
                1
            ).otherwise(0)
        ).alias("invalid_quantity"),

        F.sum(
            F.when(
                F.col("unit_price") < 0,
                1
            ).otherwise(0)
        ).alias("invalid_price"),

        F.sum(
            F.when(
                (F.col("unit_price_discount") < 0)
                | (F.col("unit_price_discount") > 1),
                1
            ).otherwise(0)
        ).alias("invalid_discount"),

        F.sum(
            F.when(
                F.col("line_total") < 0,
                1
            ).otherwise(0)
        ).alias("invalid_line_total"),

        F.sum(
            F.when(
                F.col("due_date") < F.col("order_date"),
                1
            ).otherwise(0)
        ).alias("invalid_due_date"),

        F.sum(
            F.when(
                F.col("ship_date") < F.col("order_date"),
                1
            ).otherwise(0)
        ).alias("invalid_ship_date")
    )
    .first()
    .asDict()
)

detected_business_rule_errors = {
    rule_name: error_count
    for rule_name, error_count in business_rule_summary.items()
    if error_count > 0
}

if detected_business_rule_errors:
    raise ValueError(
        "Silver business-rule validation failed: "
        f"{detected_business_rule_errors}"
    )


# ------------------------------------------------------------
# 9. Validate Silver line-total calculations
# ------------------------------------------------------------

incorrect_calculations = (
    sales
    .withColumn(
        "recalculated_line_total",
        (
            F.col("order_quantity")
            * F.col("unit_price")
            * (
                F.lit(1)
                - F.col("unit_price_discount")
            )
        ).cast("decimal(18,2)")
    )
    .withColumn(
        "calculation_difference",
        F.abs(
            F.col("line_total")
            - F.col("recalculated_line_total")
        )
    )
    .filter(
        F.col("calculation_difference")
        > F.lit(0.01)
    )
)

incorrect_calculation_count = incorrect_calculations.count()

if incorrect_calculation_count > 0:
    display(
        incorrect_calculations.select(
            "sales_order_detail_id",
            "order_quantity",
            "unit_price",
            "unit_price_discount",
            "line_total",
            "recalculated_line_total",
            "calculation_difference"
        )
    )

    raise ValueError(
        "Silver line-total calculation validation failed: "
        f"{incorrect_calculation_count} incorrect rows found."
    )


# ------------------------------------------------------------
# 10. Validate the Silver schema
# ------------------------------------------------------------

expected_silver_columns = [
    "sales_order_id",
    "sales_order_detail_id",
    "order_date",
    "due_date",
    "ship_date",
    "order_status",
    "customer_id",
    "customer_name",
    "company_name",
    "product_id",
    "product_name",
    "product_number",
    "product_color",
    "product_category_id",
    "product_category_name",
    "order_quantity",
    "unit_price",
    "unit_price_discount",
    "line_total"
]

if sales.columns != expected_silver_columns:
    raise ValueError(
        "Silver column-order validation failed: "
        f"expected={expected_silver_columns}, "
        f"actual={sales.columns}"
    )

expected_silver_types = {
    "sales_order_id": LongType(),
    "sales_order_detail_id": LongType(),
    "order_date": DateType(),
    "due_date": DateType(),
    "ship_date": DateType(),
    "order_status": IntegerType(),
    "customer_id": LongType(),
    "customer_name": StringType(),
    "company_name": StringType(),
    "product_id": LongType(),
    "product_name": StringType(),
    "product_number": StringType(),
    "product_color": StringType(),
    "product_category_id": LongType(),
    "product_category_name": StringType(),
    "order_quantity": IntegerType(),
    "unit_price": DecimalType(18, 4),
    "unit_price_discount": DecimalType(18, 4),
    "line_total": DecimalType(18, 2)
}

validate_column_types(
    "silver.sales candidate",
    sales,
    expected_silver_types
)


# ------------------------------------------------------------
# 11. Write and validate Silver
# ------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

(
    sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.sales")
)

silver_sales = validate_persisted_table(
    "silver.sales",
    sales,
    expected_silver_columns
)

validate_column_types(
    "silver.sales",
    silver_sales,
    expected_silver_types
)

print("Silver layer completed successfully.")


# ------------------------------------------------------------
# 12. Validate dimension attribute consistency
# ------------------------------------------------------------

validate_dimension_attribute_consistency(
    "Customer dimension source",
    silver_sales,
    "customer_id",
    [
        "customer_name",
        "company_name"
    ]
)

validate_dimension_attribute_consistency(
    "Product dimension source",
    silver_sales,
    "product_id",
    [
        "product_name",
        "product_number",
        "product_color",
        "product_category_id",
        "product_category_name"
    ]
)


# ------------------------------------------------------------
# 13. Build the customer dimension
# ------------------------------------------------------------

dim_customer = (
    silver_sales
    .select(
        F.col("customer_id").cast("long"),
        F.col("customer_name").cast("string"),
        F.col("company_name").cast("string")
    )
    .dropDuplicates(["customer_id"])
)


# ------------------------------------------------------------
# 14. Build the product dimension
# ------------------------------------------------------------

dim_product = (
    silver_sales
    .select(
        F.col("product_id").cast("long"),
        F.col("product_name").cast("string"),
        F.col("product_number").cast("string"),
        F.col("product_color").cast("string"),
        F.col("product_category_id").cast("long"),
        F.col("product_category_name").cast("string")
    )
    .dropDuplicates(["product_id"])
)


# ------------------------------------------------------------
# 15. Build the date dimension
# ------------------------------------------------------------

date_boundaries = (
    silver_sales
    .select(
        F.least(
            F.col("order_date"),
            F.col("due_date"),
            F.coalesce(
                F.col("ship_date"),
                F.col("due_date")
            )
        ).alias("minimum_date"),

        F.greatest(
            F.col("order_date"),
            F.col("due_date"),
            F.coalesce(
                F.col("ship_date"),
                F.col("order_date")
            )
        ).alias("maximum_date")
    )
    .agg(
        F.min("minimum_date").alias("minimum_date"),
        F.max("maximum_date").alias("maximum_date")
    )
    .first()
)

minimum_date = date_boundaries["minimum_date"]
maximum_date = date_boundaries["maximum_date"]

if minimum_date is None or maximum_date is None:
    raise ValueError(
        "The date dimension cannot be created because "
        "the Silver date range is empty."
    )

dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(minimum_date),
                F.lit(maximum_date),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )
    .select(
        F.date_format(
            F.col("full_date"),
            "yyyyMMdd"
        ).cast("integer").alias("date_key"),

        F.col("full_date"),

        F.year("full_date").alias("year"),

        F.quarter("full_date").alias("quarter_number"),

        F.concat(
            F.lit("Q"),
            F.quarter("full_date")
        ).alias("quarter_name"),

        F.month("full_date").alias("month_number"),

        F.date_format(
            F.col("full_date"),
            "MMMM"
        ).alias("month_name"),

        F.date_format(
            F.col("full_date"),
            "yyyy-MM"
        ).alias("year_month"),

        F.weekofyear("full_date").alias("week_of_year"),

        F.dayofmonth("full_date").alias("day_of_month"),

        F.dayofweek("full_date").alias("day_of_week_number"),

        F.date_format(
            F.col("full_date"),
            "EEEE"
        ).alias("day_of_week_name"),

        F.when(
            F.dayofweek("full_date").isin(1, 7),
            F.lit(True)
        )
        .otherwise(F.lit(False))
        .alias("is_weekend")
    )
)


# ------------------------------------------------------------
# 16. Build the sales fact table
# ------------------------------------------------------------

fact_sales = (
    silver_sales
    .select(
        F.col("sales_order_detail_id")
        .cast("long"),

        F.col("sales_order_id")
        .cast("long"),

        F.date_format(
            F.col("order_date"),
            "yyyyMMdd"
        ).cast("integer").alias("order_date_key"),

        F.when(
            F.col("due_date").isNotNull(),
            F.date_format(
                F.col("due_date"),
                "yyyyMMdd"
            ).cast("integer")
        ).alias("due_date_key"),

        F.when(
            F.col("ship_date").isNotNull(),
            F.date_format(
                F.col("ship_date"),
                "yyyyMMdd"
            ).cast("integer")
        ).alias("ship_date_key"),

        F.col("customer_id")
        .cast("long"),

        F.col("product_id")
        .cast("long"),

        F.col("order_status")
        .cast("integer"),

        F.col("order_quantity")
        .cast("integer"),

        F.col("unit_price")
        .cast("decimal(18,4)")
        .alias("unit_price"),

        F.col("unit_price_discount")
        .cast("decimal(18,4)")
        .alias("unit_price_discount"),

        F.col("line_total")
        .cast("decimal(18,2)")
        .alias("line_total")
    )
)


# ------------------------------------------------------------
# 17. Validate Gold grains and relationships
# ------------------------------------------------------------

validate_unique_key(
    "gold.dim_customer candidate",
    dim_customer,
    "customer_id"
)

validate_unique_key(
    "gold.dim_product candidate",
    dim_product,
    "product_id"
)

validate_unique_key(
    "gold.dim_date candidate",
    dim_date,
    "date_key"
)

validate_unique_key(
    "gold.fact_sales candidate",
    fact_sales,
    "sales_order_detail_id"
)

if fact_sales.count() != silver_sales.count():
    raise ValueError(
        "Fact-table row-count validation failed: "
        f"silver.sales={silver_sales.count()}, "
        f"gold.fact_sales candidate={fact_sales.count()}"
    )

relationship_validations = [
    (
        "customer",
        fact_sales,
        "customer_id",
        dim_customer,
        "customer_id"
    ),
    (
        "product",
        fact_sales,
        "product_id",
        dim_product,
        "product_id"
    ),
    (
        "order date",
        fact_sales,
        "order_date_key",
        dim_date,
        "date_key"
    )
]

for (
    relationship_name,
    fact_dataframe,
    fact_key,
    dimension_dataframe,
    dimension_key
) in relationship_validations:
    missing_relationship_count = (
        fact_dataframe.alias("f")
        .join(
            dimension_dataframe.alias("d"),
            F.col(f"f.{fact_key}")
            == F.col(f"d.{dimension_key}"),
            "left_anti"
        )
        .count()
    )

    if missing_relationship_count > 0:
        raise ValueError(
            f"{relationship_name.title()} relationship validation "
            f"failed: {missing_relationship_count} unmatched fact rows."
        )

optional_date_relationships = [
    "due_date_key",
    "ship_date_key"
]

for fact_date_key in optional_date_relationships:
    missing_relationship_count = (
        fact_sales
        .filter(F.col(fact_date_key).isNotNull())
        .alias("f")
        .join(
            dim_date.alias("d"),
            F.col(f"f.{fact_date_key}")
            == F.col("d.date_key"),
            "left_anti"
        )
        .count()
    )

    if missing_relationship_count > 0:
        raise ValueError(
            f"{fact_date_key} relationship validation failed: "
            f"{missing_relationship_count} unmatched fact rows."
        )


# ------------------------------------------------------------
# 18. Write Gold tables
# ------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

gold_tables = {
    "gold.dim_customer": dim_customer,
    "gold.dim_product": dim_product,
    "gold.dim_date": dim_date,
    "gold.fact_sales": fact_sales
}

for table_name, dataframe in gold_tables.items():
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


# ------------------------------------------------------------
# 19. Validate persisted Gold tables
# ------------------------------------------------------------

persisted_dim_customer = validate_persisted_table(
    "gold.dim_customer",
    dim_customer,
    dim_customer.columns
)

persisted_dim_product = validate_persisted_table(
    "gold.dim_product",
    dim_product,
    dim_product.columns
)

persisted_dim_date = validate_persisted_table(
    "gold.dim_date",
    dim_date,
    dim_date.columns
)

persisted_fact_sales = validate_persisted_table(
    "gold.fact_sales",
    fact_sales,
    fact_sales.columns
)


# ------------------------------------------------------------
# 20. Reconcile Silver and Gold totals
# ------------------------------------------------------------

silver_totals = (
    silver_sales
    .agg(
        F.sum("order_quantity").alias("total_quantity"),
        F.round(
            F.sum("line_total"),
            2
        ).alias("total_sales")
    )
    .first()
)

gold_totals = (
    persisted_fact_sales
    .agg(
        F.sum("order_quantity").alias("total_quantity"),
        F.round(
            F.sum("line_total"),
            2
        ).alias("total_sales")
    )
    .first()
)

if silver_totals["total_quantity"] != gold_totals["total_quantity"]:
    raise ValueError(
        "Silver-to-Gold quantity reconciliation failed: "
        f"silver={silver_totals['total_quantity']}, "
        f"gold={gold_totals['total_quantity']}"
    )

if silver_totals["total_sales"] != gold_totals["total_sales"]:
    raise ValueError(
        "Silver-to-Gold sales reconciliation failed: "
        f"silver={silver_totals['total_sales']}, "
        f"gold={gold_totals['total_sales']}"
    )


# ------------------------------------------------------------
# 21. Create the Power BI measure container
# ------------------------------------------------------------

dax_table = spark.createDataFrame(
    [("DAX Measures",)],
    ["measure_group"]
)

(
    dax_table.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.DAX")
)

persisted_dax_count = spark.table("gold.DAX").count()

if persisted_dax_count != 1:
    raise ValueError(
        "gold.DAX validation failed: "
        f"expected=1, actual={persisted_dax_count}"
    )


# ------------------------------------------------------------
# 22. Create the execution summary
# ------------------------------------------------------------

execution_summary = (
    silver_sales
    .agg(
        F.count("*").alias("sales_lines"),

        F.countDistinct("sales_order_id")
        .alias("orders"),

        F.countDistinct("sales_order_detail_id")
        .alias("unique_sales_lines"),

        F.countDistinct("customer_id")
        .alias("customers"),

        F.countDistinct("product_id")
        .alias("products"),

        F.countDistinct("product_category_id")
        .alias("product_categories"),

        F.sum("order_quantity")
        .alias("total_quantity"),

        F.round(
            F.sum("line_total"),
            2
        ).alias("total_sales"),

        F.round(
            F.avg("line_total"),
            2
        ).alias("average_sales_line_value"),

        F.min("order_date")
        .alias("first_order_date"),

        F.max("order_date")
        .alias("last_order_date")
    )
)

table_summary = spark.createDataFrame(
    [
        (
            "silver.sales",
            silver_sales.count()
        ),
        (
            "gold.dim_customer",
            persisted_dim_customer.count()
        ),
        (
            "gold.dim_product",
            persisted_dim_product.count()
        ),
        (
            "gold.dim_date",
            persisted_dim_date.count()
        ),
        (
            "gold.fact_sales",
            persisted_fact_sales.count()
        ),
        (
            "gold.DAX",
            persisted_dax_count
        )
    ],
    [
        "table_name",
        "row_count"
    ]
)


# ------------------------------------------------------------
# 23. Display final validation results
# ------------------------------------------------------------

print("")
print("============================================================")
print("AdventureWorks transformation completed successfully.")
print("============================================================")
print(f"Bronze sales detail rows: {source_detail_count}")
print(f"Silver sales rows: {silver_sales.count()}")
print(f"Gold fact rows: {persisted_fact_sales.count()}")
print(f"Customers: {persisted_dim_customer.count()}")
print(f"Products: {persisted_dim_product.count()}")
print(
    "Product categories: "
    f"{persisted_dim_product.select('product_category_id').distinct().count()}"
)
print(f"Dates: {persisted_dim_date.count()}")
print(f"Total quantity: {gold_totals['total_quantity']}")
print(f"Total sales: {gold_totals['total_sales']}")
print("Duplicate sales detail IDs: 0")
print("Invalid required null values: 0")
print("Invalid business-rule rows: 0")
print("Incorrect line-total calculations: 0")
print("Silver-to-Gold reconciliation: passed")
print("============================================================")

display(execution_summary)

display(
    table_summary
    .orderBy("table_name")
)

display(
    persisted_fact_sales
    .orderBy(
        "sales_order_id",
        "sales_order_detail_id"
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
