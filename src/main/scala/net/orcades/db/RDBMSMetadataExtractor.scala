package net.orcades.db

import java.sql.ResultSet
import java.sql.Types
import scala.collection.mutable.MutableList
import java.sql.DatabaseMetaData
import scala.collection.parallel.mutable.ParMap
import java.sql.Connection
import java.sql.DriverManager

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

  println("test")

  Class.forName("org.postgresql.Driver")
  val conn = DriverManager.getConnection("jdbc:postgresql:jug", "test", "test")

  val tables = allTables(conn)

  tables.foreach(table => println(table.name + ": " + table.fks))

  def allTables(implicit connection: Connection) = {

    val tables = connection.getMetaData().getTables(connection.getCatalog(), "public", null, Array("TABLE"))
    val entities = MutableList[Table]()
    while (tables.next()) {

      val entity = dumpTable(connection.getMetaData(), tables.getString("table_name"))
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

  def dumpTable(metadata: DatabaseMetaData, tableName: String)(implicit connection: Connection): Table = {

    val rs = metadata.getTables(connection.getCatalog(), "public", tableName, null)
    val props = if (rs.next()) {
      getPropertiesFromRemarks(rs.getString("REMARKS"))
    } else {
      Map[String, String]()
    }

    if (props.isEmpty)
      return null

    def getPks(implicit connection: Connection): Set[String] = {
      val rs = metadata.getPrimaryKeys(connection.getCatalog(), "public", tableName);
      val set = MutableList[String]()
      while (rs.next()) {
        set += rs.getString("COLUMN_NAME")
      }
      set.toSet
    }

    def getFks(implicit connection: Connection): List[FK] = {
      val rs = metadata.getImportedKeys(connection.getCatalog(), "public", tableName);
      val ff = MutableList[(String, (String, String))]()
      val fks = MutableList[FK]()

      if (rs.next()) {
        var fkName = rs.getString("FK_NAME")
        do {
          println(tableName + ": " + rs.getString("FK_NAME") + " - " + rs.getString("FKCOLUMN_NAME"))
          if (rs.getString("FK_NAME") != fkName) {
            fks += FK(fkName, ff.toList)
            ff.clear
            fkName = rs.getString("FK_NAME")
          }
          val i = (rs.getString("FKCOLUMN_NAME"), (rs.getString("PKTABLE_NAME"), rs.getString("PKCOLUMN_NAME")))
          ff += i
          if (rs.isLast())
            fks += FK(rs.getString("FK_NAME"), ff.toList)
        } while (rs.next())
      }
      fks.toList
    }

    val members = MutableList[Column]()

    val pks = getPks(connection)

    //val fks = getFks(connection)

    val columns = metadata.getColumns(connection.getCatalog(), "public", tableName, null);
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

    Table(entityName(tableName), tableName, members.toList, getFks(connection), props)

  }

  def getPropertiesFromRemarks(remarks: String) = if (remarks == null)
    Map[String, String]()
  else
    remarks.split(",").map(_.trim()).map(_.split("=")).map(a => (a(0) -> a(1))).toMap

}