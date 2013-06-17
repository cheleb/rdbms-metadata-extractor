package net.orcades.db

import scala.slick.session.Database
import scala.slick.driver.BasicDriver.Implicit._
import Database.threadLocalSession
import scala.slick.session.Session
import java.sql.ResultSet
import java.sql.Types
import scala.collection.mutable.MutableList
import java.sql.DatabaseMetaData

import scala.collection.parallel.mutable.ParMap

sealed trait DBElement {
  def schema
}

case class Constraints(size: Option[Int], cols: Option[Int], rows: Option[Int])
case class Table(name: String, tableName: String, columns: List[Column], fks: List[FK], props: Map[String, String]) // extends DBElement 
case class Column(name: String, dataType: String, pk: Boolean, nullable: Boolean, autoInc: Boolean, constraints: Constraints, props: Map[String, String]) {
  def option = nullable || autoInc
}
case class FK(name: String, keys: List[(String, (String, String))])

object RDBMSMetadataExtractor extends App {
  Database.forURL("jdbc:postgresql:jug", "test", "test", driver = "org.postgresql.Driver") withSession {
    //    println("Speakers:")
    //    val v = Users.all
    //    println(v)
    //
    allTables.map(e => println(e))

  }

  def allTables = {

    val metadata = threadLocalSession.metaData
    val tables = metadata.getTables(threadLocalSession.conn.getCatalog(), "public", null, Array("TABLE"))
    val entities = MutableList[Table]()
    while (tables.next()) {

      val entity = dumpTable(metadata, tables.getString("table_name"))
      if (entity != null)
        entities += entity

    }
    entities

  }

  def entityName(tableName: String) = {
    tableName.substring(0, 1).toUpperCase() + tableName.substring(1)
  }

  def uri(tableName: String) = {
    tableName match {
      case name if name.endsWith("s") => name.toLowerCase()
      case name if name.endsWith("y") => name.substring(0, name.length() - 1) + "ies"
      case name => name.toLowerCase() + "s"
    }
  }

  def columnType(t: Int) = {
    t match {
      case Types.INTEGER => "Int"
      case Types.BIGINT => "Long"
      case Types.VARCHAR => "String"
      case Types.CLOB => "String"
      case Types.BIT => "Boolean"
      case Types.BOOLEAN => "Boolean"
      case Types.TIMESTAMP => "Timestamp"
      case Types.DATE => "Calendar"
      case Types.CHAR => "Char"
    }
  }

  def dumpTable(metadata: DatabaseMetaData, tableName: String): Table = {

    val rs = metadata.getTables(threadLocalSession.conn.getCatalog(), "public", tableName, null)
    val props = if (rs.next()) {
      getPropertiesFromRemarks(rs.getString("REMARKS"))
    } else {
      Map[String, String]()
    }

    if (props.isEmpty)
      return null

    def getPks(): Set[String] = {
      val rs = metadata.getPrimaryKeys(threadLocalSession.conn.getCatalog(), "public", tableName);
      val set = MutableList[String]()
      while (rs.next()) {
        set += rs.getString("COLUMN_NAME")
      }
      set.toSet
    }

    def getFks(): List[FK] = {
      val rs = metadata.getImportedKeys(threadLocalSession.conn.getCatalog(), "public", tableName);
      val ff = MutableList[(String, (String, String))]()
      val fks = MutableList[FK]()

      while (rs.next()) {
        if (rs.getInt("KEY_SEQ") == 1 && !ff.isEmpty) {
          fks += FK(rs.getString("FK_NAME"), ff.toList)
          ff.clear
        }
        val i = (rs.getString("FKCOLUMN_NAME"), (rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")))
        ff += i
        if (rs.isLast())
          fks += FK(rs.getString("FK_NAME"), ff.toList)
      }

      fks.toList
    }

    val members = MutableList[Column]()

    val pks = getPks()

    val fks = getFks()

    val columns = metadata.getColumns(threadLocalSession.conn.getCatalog(), "public", tableName, null);
    while (columns.next()) {
      val columnName = columns.getString("COLUMN_NAME")
      val remarks = columns.getString("REMARKS")

      val props = getPropertiesFromRemarks(remarks)

      def p(key: String) = {
        props.get(key).map(Integer.parseInt(_))
      }
      val c = Constraints(p("size"), p("cols"), p("rows"))

      members += Column(name = columnName,
        dataType = columnType(columns.getInt("DATA_TYPE")),
        pk = pks.contains(columnName),
        nullable = columns.getBoolean("NULLABLE"),
        autoInc = columns.getString("IS_AUTOINCREMENT") == "YES",
        constraints = c,
        props = props)

    }

    Table(entityName(tableName), tableName, members.toList, getFks(), props)

  }

  def getPropertiesFromRemarks(remarks: String) = if (remarks == null)
    Map[String, String]()
  else
    remarks.split(",").map(_.trim()).map(_.split("=")).map(a => (a(0) -> a(1))).toMap

}