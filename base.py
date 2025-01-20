from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

#Note that number of rows below is really number of orders. Every order can have 1 - 5 lines. So generating as-is will have some variability on the actualy row_count
NUM_ROWS = 2_000_000_000
NUM_PARTITIONS = 10_000

# Pre-build arrays of Product_IDs as Spark Column objects:
product_list_summer = [F.lit(x) for x in [5, 6]]
product_list_non_summer = [F.lit(x) for x in [1,2,3,4,7,8,9,10]]
product_list_others = [F.lit(x) for x in [11,12,13]]

arr_summer = F.array(*product_list_summer)
arr_non_summer = F.array(*product_list_non_summer)
arr_others = F.array(*product_list_others)

# ------------------------------------------------------------------------------
# 1) Generate a base DataFrame with one row per order:
#    - We pick Location_ID once per order so all lines share the same location.
base_df = (
    spark.range(start=0, end=NUM_ROWS, numPartitions=NUM_PARTITIONS)
    # Generate a unique order ID from id + random
    .withColumn("Order_ID", F.hex(F.md5(F.concat_ws("_", F.col("id").cast("string"), F.rand()))))
    
    # Uniform random for date in [2023-01-01 .. 2024-12-31]
    .withColumn("rand_date", F.rand())
    .withColumn("Order_Date", F.expr("date_add(to_date('2023-01-01'), cast(rand_date * 730 as int))"))
    
    # Derive month & season
    .withColumn("Month", F.month("Order_Date"))
    .withColumn(
        "Season",
        F.when(F.col("Month").isin([12,1,2]), "winter")
         .when(F.col("Month").isin([3,4,5]), "spring")
         .when(F.col("Month").isin([6,7,8]), "summer")
         .otherwise("fall")
    )
    
    # Number of lines distribution (60%=1, 30%=2, 5%=3, 1%=4, 4%=5)
    .withColumn("rand_lines", F.rand())
    .withColumn(
        "Num_Lines",
        F.when(F.col("rand_lines") < 0.60, 1)
         .when(F.col("rand_lines") < 0.90, 2)
         .when(F.col("rand_lines") < 0.95, 3)
         .when(F.col("rand_lines") < 0.96, 4)
         .otherwise(5)
    )

    # Time-of-Day distribution (Morning=50%, Afternoon=30%, Night=20%)
    .withColumn("rand_tod", F.rand())
    .withColumn(
        "Time_Of_Day",
        F.when(F.col("rand_tod") < 0.50, "Morning")
         .when(F.col("rand_tod") < 0.80, "Afternoon")
         .otherwise("Night")
    )

    # Location_ID with variability (assigned once per order)
    .withColumn("rand_loc", F.rand())
    .withColumn(
        "Location_ID",
        F.when(F.col("rand_loc") < 0.30, F.floor(F.rand() * 50) + 1)       # 1..50   (30%)
         .when(F.col("rand_loc") < 0.80, F.floor(F.rand() * 150) + 51)     # 51..200 (50%)
         .when(F.col("rand_loc") < 0.95, F.floor(F.rand() * 300) + 201)    # 201..500 (15%)
         .otherwise(F.floor(F.rand() * 500) + 501)                         # 501..1000 (5%)
    )

    # Drop intermediate columns
    .drop("rand_date", "rand_lines", "rand_tod", "rand_loc")
)

# ------------------------------------------------------------------------------
# 2) Explode out by Num_Lines (one row per line_item), carrying forward the base fields.
exploded_df = (
    base_df
    .withColumn("line_array", F.expr("sequence(1, Num_Lines)"))
    .select("*", F.posexplode("line_array").alias("Line_Pos", "Line_Val"))
    .drop("line_array", "Num_Lines")
    .withColumn("Order_Line_ID", F.concat(F.col("Order_ID"), F.lit("_"), F.col("Line_Val")))
)

# ------------------------------------------------------------------------------
# 3) Add line-level random columns:
#    - We do new random draws at the line level for Quantity, Discount, and Product.
line_df = (
    exploded_df
    # Quantity (1–5, skewed)
    .withColumn("rand_qty", F.rand())
    .withColumn(
        "Quantity",
        F.when(F.col("rand_qty") < 0.40, 1)
         .when(F.col("rand_qty") < 0.70, 2)
         .when(F.col("rand_qty") < 0.85, 3)
         .when(F.col("rand_qty") < 0.95, 4)
         .otherwise(5)
    )
    # Discount (80% = 0, else 1–15)
    .withColumn("rand_disc", F.rand())
    .withColumn(
        "Discount_Rate",
        F.when(F.col("rand_disc") < 0.80, 0)
         .otherwise(F.floor(F.rand() * 15 + 1))
    )
    # Product distribution
    .withColumn("rand_prod", F.rand())
    .withColumn(
        "Product_ID",
        F.when(
            F.col("Season") == "summer",
            # summer => (5,6)=40%, (1..4,7..10)=50%, (11..13)=10%
            F.when(F.col("rand_prod") < 0.40,
                   F.element_at(arr_summer, 1 + F.floor(F.rand() * 2).cast("int")))
             .when(F.col("rand_prod") < 0.90,
                   F.element_at(arr_non_summer, 1 + F.floor(F.rand() * 8).cast("int")))
             .otherwise(F.element_at(arr_others, 1 + F.floor(F.rand() * 3).cast("int")))
        ).otherwise(
            # non-summer => (1..4,7..10)=70%, (5,6)=10%, (11..13)=20%
            F.when(F.col("rand_prod") < 0.70,
                   F.element_at(arr_non_summer, 1 + F.floor(F.rand() * 8).cast("int")))
             .when(F.col("rand_prod") < 0.80,
                   F.element_at(arr_summer, 1 + F.floor(F.rand() * 2).cast("int")))
             .otherwise(F.element_at(arr_others, 1 + F.floor(F.rand() * 3).cast("int")))
        )
    )
    .drop("rand_qty", "rand_disc", "rand_prod")
)

# ------------------------------------------------------------------------------
# 4) Final select & write the dataset
final_df = line_df.select(
    "Order_ID",
    "Order_Line_ID",
    "Order_Date",
    "Time_Of_Day",
    "Season",
    "Month",
    "Location_ID",   # same for all lines of the same order
    "Product_ID",
    "Quantity",
    "Discount_Rate"
)

# Example write (Delta). For 50B rows, ensure sufficient cluster resources & partitioning.
# Make sure your_catalog.your_schema are already created in Unity Catalog 
# (e.g., CREATE SCHEMA IF NOT EXISTS your_catalog.your_schema).

(final_df
    .write
    .format("delta")        # or omit if delta is your default
    .mode("overwrite")      # or "append"
    .saveAsTable("sweetcoffeetree.base")
)