
"""
Making the new table summary as a script
Based on the initial EDA and the vendor summary we are making that as script
"""

import sqlite3
import pandas as pd
import logging
import numpy as np

logging.basicConfig(
    filename="logs/get_vendor_summary.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


def ingest_db(df: pd.DataFrame, table_name: str, conn: sqlite3.Connection) -> None:
    """Ingest the dataframe into a database table."""
    df.to_sql(table_name, con=conn, if_exists="replace", index=False)


def create_vendor_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Merge the different tables to get the overall vendor summary
    and return the resultant dataframe.
    """
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),

    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY
            p.VendorNumber, p.VendorName, p.Brand, p.Description,
            p.PurchasePrice, pp.Price, pp.Volume
    ),

    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )

    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean the data and add derived metrics."""
    df = df.copy()

    # Convert datatypes
    df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # Fill missing values
    df.fillna(0, inplace=True)

    # Trim strings
    df["VendorName"] = df["VendorName"].astype(str).str.strip()
    df["Description"] = df["Description"].astype(str).str.strip()

    # Derived metrics
    df["GrossProfit"] = df["TotalSalesDollars"] - df["TotalPurchaseDollars"]

    # Avoid division by zero for profit margin
    df["ProfitMargin"] = np.where(
        df["TotalSalesDollars"] != 0,
        (df["GrossProfit"] / df["TotalSalesDollars"]) * 100,
        0
    )

    df["StockTurnover"] = np.where(
        df["TotalPurchaseQuantity"] != 0,
        df["TotalSalesQuantity"] / df["TotalPurchaseQuantity"],
        0
    )

    df["SalesToPurchaseRatio"] = np.where(
        df["TotalPurchaseDollars"] != 0,
        df["TotalSalesDollars"] / df["TotalPurchaseDollars"],
        0
    )

    return df


if __name__ == "__main__":
    conn = sqlite3.connect("inventory1.db")

    logging.info("Creating Vendor Summary Table.....")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head().to_string())

    logging.info("Cleaning Data.....")
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head().to_string())

    logging.info("Ingesting data.....")
    ingest_db(clean_df, "vendor_sales_summary", conn)

    logging.info("Completed")
    conn.close()