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

# ============================================================
# TransformAdventureWorks
# Source: CommerceLakehouse, Bronze schema
# Target: CommerceLakehouse, Silver schema
#
# Purpose:
# - Read raw AdventureWorks data from the Bronze layer
# - Join and standardize the source tables
# - Validate data quality and row-level uniqueness
# - Save the validated result as silver.sales
#
# The script is idempotent:
# running it again overwrites silver.sales instead of appending
# duplicate records.
# ============================================================


# ------------------------------------------------------------
# 1. Read source tables from the Bronze layer
# ------------------------------------------------------------

customers = spark.table("bronze.customer")
products = spark.table("bronze.product")
categories = spark.table("bronze.product_category")
orders = spark.table("bronze.sales_order_header")
order_details = spark.table("bronze.sales_order_detail")


# ------------------------------------------------------------
# 2. Validate that required source columns are available
# ------------------------------------------------------------

required_columns = {
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
        "Color"
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

for table_name, expected_columns in required_columns.items():
    actual_columns = set(source_dataframes[table_name].columns)

    missing_columns = [
        column_name
        for column_name in expected_columns
        if column_name not in actual_columns
    ]

    if missing_columns:
        raise ValueError(
            f"Source table {table_name} is missing required columns: "
            f"{missing_columns}"
        )

print("Source schema validation passed.")


# ------------------------------------------------------------
# 3. Transform and combine sales data
#
# Grain:
# One row represents one sales order detail line.
#
# Business key:
# sales_order_detail_id uniquely identifies each row.
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
        .alias("company_name"),

        F.col("d.ProductID")
        .cast("long")
        .alias("product_id"),

        F.col("p.Name")
        .alias("product_name"),

        F.col("p.ProductNumber")
        .alias("product_number"),

        F.col("p.Color")
        .alias("product_color"),

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
# 4. Validate the transformed DataFrame before writing
# ------------------------------------------------------------

source_count = order_details.count()
transformed_count = sales.count()

if source_count != transformed_count:
    raise ValueError(
        "Row-count validation failed before writing: "
        f"source={source_count}, transformed={transformed_count}"
    )

unique_detail_id_count = (
    sales
    .select("sales_order_detail_id")
    .distinct()
    .count()
)

if transformed_count != unique_detail_id_count:
    raise ValueError(
        "Uniqueness validation failed for sales_order_detail_id: "
        f"rows={transformed_count}, "
        f"unique_ids={unique_detail_id_count}"
    )

duplicate_detail_ids = (
    sales
    .groupBy("sales_order_detail_id")
    .count()
    .filter(F.col("count") > 1)
)

duplicate_detail_id_count = duplicate_detail_ids.count()

if duplicate_detail_id_count > 0:
    display(duplicate_detail_ids)

    raise ValueError(
        "Duplicate validation failed: "
        f"{duplicate_detail_id_count} duplicated identifiers found."
    )


# ------------------------------------------------------------
# 5. Validate required non-null values
# ------------------------------------------------------------

required_target_columns = [
    "sales_order_id",
    "sales_order_detail_id",
    "order_date",
    "customer_id",
    "customer_name",
    "product_id",
    "product_name",
    "order_quantity",
    "unit_price",
    "unit_price_discount",
    "line_total"
]

null_summary = sales.agg(
    *[
        F.sum(
            F.when(
                F.col(column_name).isNull(),
                1
            ).otherwise(0)
        ).alias(column_name)
        for column_name in required_target_columns
    ]
)

null_results = null_summary.first().asDict()

columns_with_nulls = {
    column_name: null_count
    for column_name, null_count in null_results.items()
    if null_count > 0
}

if columns_with_nulls:
    display(null_summary)

    raise ValueError(
        "Null-value validation failed: "
        f"{columns_with_nulls}"
    )


# ------------------------------------------------------------
# 6. Validate numeric and business rules
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
        ).alias("invalid_line_total")
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
        "Business-rule validation failed: "
        f"{detected_business_rule_errors}"
    )


# ------------------------------------------------------------
# 7. Recalculate and validate line_total
#
# Formula:
# quantity * unit price * (1 - discount)
# ------------------------------------------------------------

calculation_validation = (
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
)

incorrect_calculations = (
    calculation_validation
    .filter(
        F.col("calculation_difference") > F.lit(0.01)
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
        "Line-total calculation validation failed: "
        f"{incorrect_calculation_count} incorrect rows found."
    )


# ------------------------------------------------------------
# 8. Validate the transformed schema
# ------------------------------------------------------------

expected_target_columns = [
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
    "order_quantity",
    "unit_price",
    "unit_price_discount",
    "line_total"
]

actual_target_columns = sales.columns

missing_target_columns = [
    column_name
    for column_name in expected_target_columns
    if column_name not in actual_target_columns
]

unexpected_target_columns = [
    column_name
    for column_name in actual_target_columns
    if column_name not in expected_target_columns
]

if missing_target_columns or unexpected_target_columns:
    raise ValueError(
        "Target-schema validation failed: "
        f"missing={missing_target_columns}, "
        f"unexpected={unexpected_target_columns}"
    )


# ------------------------------------------------------------
# 9. Create the Silver schema
# ------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")


# ------------------------------------------------------------
# 10. Write the validated dataset to the Silver layer
#
# Overwrite mode makes repeated sequential executions safe.
# Existing silver.sales data is replaced, not duplicated.
# ------------------------------------------------------------

(
    sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("silver.sales")
)


# ------------------------------------------------------------
# 11. Validate the persisted Silver table
# ------------------------------------------------------------

silver_sales = spark.table("silver.sales")
target_count = silver_sales.count()

if source_count != target_count:
    raise ValueError(
        "Persisted row-count validation failed: "
        f"source={source_count}, target={target_count}"
    )

persisted_columns = silver_sales.columns

if persisted_columns != expected_target_columns:
    raise ValueError(
        "Persisted-schema validation failed: "
        f"expected={expected_target_columns}, "
        f"actual={persisted_columns}"
    )


# ------------------------------------------------------------
# 12. Create the final execution summary
# ------------------------------------------------------------

execution_summary = (
    silver_sales
    .agg(
        F.count("*")
        .alias("sales_lines"),

        F.countDistinct("sales_order_id")
        .alias("orders"),

        F.countDistinct("sales_order_detail_id")
        .alias("unique_sales_lines"),

        F.countDistinct("customer_id")
        .alias("customers"),

        F.countDistinct("product_id")
        .alias("products"),

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


# ------------------------------------------------------------
# 13. Display the final validation results
# ------------------------------------------------------------

print("")
print("============================================================")
print("TransformAdventureWorks completed successfully.")
print("============================================================")
print(f"Source rows: {source_count}")
print(f"Silver rows: {target_count}")
print(f"Unique sales detail IDs: {unique_detail_id_count}")
print(f"Duplicate detail IDs: {duplicate_detail_id_count}")
print("Invalid business-rule rows: 0")
print("Incorrect line-total calculations: 0")
print("Target table: silver.sales")
print("============================================================")

display(execution_summary)

display(
    silver_sales
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

# CELL ********************

from pyspark.sql import functions as F

# ============================================================
# BuildAdventureWorksStarSchema
# Source: CommerceLakehouse, Silver schema
# Targets:
# - gold.dim_customer
# - gold.dim_product
# - gold.dim_date
# - gold.fact_sales
#
# Purpose:
# Build and validate a reusable analytical star schema.
#
# The script is idempotent:
# target tables are overwritten during each execution.
# ============================================================


# ------------------------------------------------------------
# 1. Read and validate the Silver source table
# ------------------------------------------------------------

silver_sales = spark.table("silver.sales")

required_source_columns = [
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
    "order_quantity",
    "unit_price",
    "unit_price_discount",
    "line_total"
]

missing_source_columns = [
    column_name
    for column_name in required_source_columns
    if column_name not in silver_sales.columns
]

if missing_source_columns:
    raise ValueError(
        "silver.sales is missing required columns: "
        f"{missing_source_columns}"
    )

print("Silver source-table validation passed.")


# ------------------------------------------------------------
# 2. Create the customer dimension
#
# Grain:
# One row represents one customer.
# ------------------------------------------------------------

dim_customer = (
    silver_sales
    .select(
        F.col("customer_id").cast("long"),
        F.col("customer_name"),
        F.col("company_name")
    )
    .dropDuplicates(["customer_id"])
)


# ------------------------------------------------------------
# 3. Create the product dimension
#
# Grain:
# One row represents one product.
# ------------------------------------------------------------

dim_product = (
    silver_sales
    .select(
        F.col("product_id").cast("long"),
        F.col("product_name"),
        F.col("product_number"),
        F.col("product_color")
    )
    .dropDuplicates(["product_id"])
)


# ------------------------------------------------------------
# 4. Determine the required date range
#
# The date dimension covers order, due, and ship dates.
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

print(f"Date dimension range: {minimum_date} to {maximum_date}")


# ------------------------------------------------------------
# 5. Create the date dimension
#
# date_key uses the YYYYMMDD integer format.
# ------------------------------------------------------------

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
# 6. Create the sales fact table
#
# Grain:
# One row represents one sales order detail line.
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
# 7. Validate dimension keys
# ------------------------------------------------------------

dimension_validations = [
    ("dim_customer", dim_customer, "customer_id"),
    ("dim_product", dim_product, "product_id"),
    ("dim_date", dim_date, "date_key")
]

for dimension_name, dataframe, key_column in dimension_validations:
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

    if row_count != distinct_key_count:
        raise ValueError(
            f"{dimension_name} contains duplicate keys: "
            f"rows={row_count}, "
            f"distinct_keys={distinct_key_count}"
        )

    if null_key_count > 0:
        raise ValueError(
            f"{dimension_name} contains "
            f"{null_key_count} null keys."
        )

    print(
        f"{dimension_name} validation passed: "
        f"{row_count} unique rows."
    )


# ------------------------------------------------------------
# 8. Validate the fact-table grain
# ------------------------------------------------------------

fact_row_count = fact_sales.count()

unique_fact_key_count = (
    fact_sales
    .select("sales_order_detail_id")
    .distinct()
    .count()
)

silver_row_count = silver_sales.count()

if fact_row_count != unique_fact_key_count:
    raise ValueError(
        "fact_sales grain validation failed: "
        f"rows={fact_row_count}, "
        f"unique_keys={unique_fact_key_count}"
    )

if fact_row_count != silver_row_count:
    raise ValueError(
        "Fact-table row-count validation failed: "
        f"silver.sales={silver_row_count}, "
        f"fact_sales={fact_row_count}"
    )

print(
    "Fact-table grain validation passed: "
    f"{fact_row_count} unique sales lines."
)


# ------------------------------------------------------------
# 9. Validate customer relationships
# ------------------------------------------------------------

missing_customer_relationships = (
    fact_sales.alias("f")
    .join(
        dim_customer.alias("c"),
        F.col("f.customer_id") == F.col("c.customer_id"),
        "left_anti"
    )
    .count()
)

if missing_customer_relationships > 0:
    raise ValueError(
        "Customer relationship validation failed: "
        f"{missing_customer_relationships} fact rows "
        "do not have a matching customer."
    )

print("Customer relationship validation passed.")


# ------------------------------------------------------------
# 10. Validate product relationships
# ------------------------------------------------------------

missing_product_relationships = (
    fact_sales.alias("f")
    .join(
        dim_product.alias("p"),
        F.col("f.product_id") == F.col("p.product_id"),
        "left_anti"
    )
    .count()
)

if missing_product_relationships > 0:
    raise ValueError(
        "Product relationship validation failed: "
        f"{missing_product_relationships} fact rows "
        "do not have a matching product."
    )

print("Product relationship validation passed.")


# ------------------------------------------------------------
# 11. Validate order-date relationships
# ------------------------------------------------------------

missing_order_date_relationships = (
    fact_sales.alias("f")
    .join(
        dim_date.alias("d"),
        F.col("f.order_date_key") == F.col("d.date_key"),
        "left_anti"
    )
    .count()
)

if missing_order_date_relationships > 0:
    raise ValueError(
        "Order-date relationship validation failed: "
        f"{missing_order_date_relationships} fact rows "
        "do not have a matching date."
    )

print("Order-date relationship validation passed.")


# ------------------------------------------------------------
# 12. Validate due-date relationships
# ------------------------------------------------------------

missing_due_date_relationships = (
    fact_sales
    .filter(F.col("due_date_key").isNotNull())
    .alias("f")
    .join(
        dim_date.alias("d"),
        F.col("f.due_date_key") == F.col("d.date_key"),
        "left_anti"
    )
    .count()
)

if missing_due_date_relationships > 0:
    raise ValueError(
        "Due-date relationship validation failed: "
        f"{missing_due_date_relationships} fact rows "
        "do not have a matching date."
    )

print("Due-date relationship validation passed.")


# ------------------------------------------------------------
# 13. Validate ship-date relationships
# ------------------------------------------------------------

missing_ship_date_relationships = (
    fact_sales
    .filter(F.col("ship_date_key").isNotNull())
    .alias("f")
    .join(
        dim_date.alias("d"),
        F.col("f.ship_date_key") == F.col("d.date_key"),
        "left_anti"
    )
    .count()
)

if missing_ship_date_relationships > 0:
    raise ValueError(
        "Ship-date relationship validation failed: "
        f"{missing_ship_date_relationships} fact rows "
        "do not have a matching date."
    )

print("Ship-date relationship validation passed.")


# ------------------------------------------------------------
# 14. Create the Gold schema if required
# ------------------------------------------------------------

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")


# ------------------------------------------------------------
# 15. Write the star-schema tables to the Gold layer
# ------------------------------------------------------------

(
    dim_customer.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_customer")
)

(
    dim_product.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_product")
)

(
    dim_date.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.dim_date")
)

(
    fact_sales.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fact_sales")
)


# ------------------------------------------------------------
# 16. Validate persisted Gold tables
# ------------------------------------------------------------

persisted_customer_count = (
    spark.table("gold.dim_customer").count()
)

persisted_product_count = (
    spark.table("gold.dim_product").count()
)

persisted_date_count = (
    spark.table("gold.dim_date").count()
)

persisted_fact_count = (
    spark.table("gold.fact_sales").count()
)

persisted_validations = {
    "gold.dim_customer": (
        dim_customer.count(),
        persisted_customer_count
    ),
    "gold.dim_product": (
        dim_product.count(),
        persisted_product_count
    ),
    "gold.dim_date": (
        dim_date.count(),
        persisted_date_count
    ),
    "gold.fact_sales": (
        fact_sales.count(),
        persisted_fact_count
    )
}

for table_name, counts in persisted_validations.items():
    expected_count, actual_count = counts

    if expected_count != actual_count:
        raise ValueError(
            f"Persisted-table validation failed for "
            f"{table_name}: expected={expected_count}, "
            f"actual={actual_count}"
        )

    print(
        f"{table_name} persisted successfully: "
        f"{actual_count} rows."
    )


# ------------------------------------------------------------
# 17. Reconcile Silver and Gold business totals
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
    spark.table("gold.fact_sales")
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
        "Quantity reconciliation failed: "
        f"silver={silver_totals['total_quantity']}, "
        f"gold={gold_totals['total_quantity']}"
    )

if silver_totals["total_sales"] != gold_totals["total_sales"]:
    raise ValueError(
        "Sales reconciliation failed: "
        f"silver={silver_totals['total_sales']}, "
        f"gold={gold_totals['total_sales']}"
    )

print("Silver-to-Gold business-total reconciliation passed.")


# ------------------------------------------------------------
# 18. Display the final execution summary
# ------------------------------------------------------------

summary = spark.createDataFrame(
    [
        ("gold.dim_customer", persisted_customer_count),
        ("gold.dim_product", persisted_product_count),
        ("gold.dim_date", persisted_date_count),
        ("gold.fact_sales", persisted_fact_count)
    ],
    [
        "table_name",
        "row_count"
    ]
)

print("")
print("============================================================")
print("AdventureWorks star schema completed successfully.")
print("============================================================")
print("Source table: silver.sales")
print(f"Customers: {persisted_customer_count}")
print(f"Products: {persisted_product_count}")
print(f"Dates: {persisted_date_count}")
print(f"Sales fact rows: {persisted_fact_count}")
print(f"Total quantity: {gold_totals['total_quantity']}")
print(f"Total sales: {gold_totals['total_sales']}")
print("============================================================")

display(summary)

display(
    spark.table("gold.fact_sales")
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

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("measure_group", StringType(), False)
])

dax_table = spark.createDataFrame(
    [("DAX Measures",)],
    schema
)

(
    dax_table.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.DAX")
)

print("gold.DAX created successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT
        OrderDate,
        COUNT(*) AS order_count
    FROM bronze.sales_order_header
    GROUP BY OrderDate
    ORDER BY OrderDate
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
