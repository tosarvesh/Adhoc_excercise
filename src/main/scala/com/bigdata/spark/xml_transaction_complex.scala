package com.bigdata.spark

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object xml_transaction_complex {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("xml_transaction_complex").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    //spark = SparkSession.builder.master("local[2]").appName("csvdata").getOrCreate()
   val data="file:///c:\\bigdata\\datasets\\xmldata\\transactions.xml"

  val  readXMLfile = spark.read.format("com.databricks.spark.xml").option("rootTag", "POSLog").option("rowTag","Transaction").load(data)
    readXMLfile.show()
    readXMLfile.printSchema()
  val  res=readXMLfile.withColumn("LineItem", explode(col("RetailTransaction.LineItem"))).withColumn("Total", explode(col("RetailTransaction.Total"))).select("BusinessDayDate", "ControlTransaction.OperatorSignOff.CloseBusinessDayDate",
      "ControlTransaction.OperatorSignOff.CloseTransactionSequenceNumber",
      "ControlTransaction.OperatorSignOff.EndDateTimestamp", "ControlTransaction.OperatorSignOff.OpenBusinessDayDate",
      "ControlTransaction.OperatorSignOff.OpenTransactionSequenceNumber",
      "ControlTransaction.OperatorSignOff.StartDateTimestamp",
      "ControlTransaction.ReasonCode", "ControlTransaction._Version", "CurrencyCode", "EndDateTime",
      "OperatorID._OperatorName", "OperatorID._VALUE", "RetailStoreID",
      "RetailTransaction.ItemCount",
      "LineItem.Sale.Description", "LineItem.Sale.DiscountAmount", "LineItem.Sale.ExtendedAmount",
      "LineItem.Sale.ExtendedDiscountAmount", "LineItem.Sale.ItemID",
      "LineItem.Sale.Itemizers._FoodStampable", "LineItem.Sale.Itemizers._Itemizer6",
      "LineItem.Sale.Itemizers._Itemizer8", "LineItem.Sale.Itemizers._Tax1", "LineItem.Sale.Itemizers._VALUE",
      "LineItem.Sale.MerchandiseHierarchy._DepartmentDescription", "LineItem.Sale.MerchandiseHierarchy._Level",
      "LineItem.Sale.MerchandiseHierarchy._VALUE",
      "LineItem.Sale.OperatorSequence",
      "LineItem.Sale.POSIdentity.POSItemID", "LineItem.Sale.POSIdentity.Qualifier",
      "LineItem.Sale.POSIdentity._POSIDType",
      "LineItem.Sale.Quantity", "LineItem.Sale.RegularSalesUnitPrice", "LineItem.Sale.ReportCode",
      "LineItem.Sale._ItemType",
      "LineItem.SequenceNumber",
      "LineItem.Tax.Amount", "LineItem.Tax.Percent", "LineItem.Tax.Reason", "LineItem.Tax.TaxableAmount",
      "LineItem.Tax._TaxDescription", "LineItem.Tax._TaxID",
      "LineItem.Tender.Amount", "LineItem.Tender.Authorization.AuthorizationCode",
      "LineItem.Tender.Authorization.AuthorizationDateTime", "LineItem.Tender.Authorization.ReferenceNumber",
      "LineItem.Tender.Authorization.RequestedAmount", "LineItem.Tender.Authorization._ElectronicSignature",
      "LineItem.Tender.Authorization._HostAuthorized",
      "LineItem.Tender.OperatorSequence", "LineItem.Tender.TenderID", "LineItem.Tender._TenderDescription",
      "LineItem.Tender._TenderType", "LineItem.Tender._TypeCode",
      "LineItem._EntryMethod", "LineItem._weightItem",
      "RetailTransaction.PerformanceMetrics.IdleTime", "RetailTransaction.PerformanceMetrics.RingTime",
      "RetailTransaction.PerformanceMetrics.TenderTime",
      "RetailTransaction.ReceiptDateTime",
      "Total._TotalType", "Total._VALUE",
      "RetailTransaction.TransactionCount", "RetailTransaction._Version",
      "SequenceNumber", "WorkstationID")
   val res1 = res

    res1.show()
    res.printSchema()






    spark.stop()
  }
}
