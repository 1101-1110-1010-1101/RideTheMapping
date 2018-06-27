package ru.ifmo.se.ridethemapping

import org.postgresql.util.PSQLException
import java.sql.DriverManager
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.jvm.isAccessible

class RTM(url: String, username: String, password: String) {
  val db = DriverManager.getConnection(url, username, password)

  annotation class Table(val name: String)

  @Target(AnnotationTarget.FIELD)
  annotation class PrimaryKey()

  @Target(AnnotationTarget.FIELD)
  annotation class NotNull()

  fun <T : Any> map(obj: T) {
    val fields = obj::class.declaredMemberProperties
    val types = fields.map { it.returnType }
    val sqlTypes = types.map {
      when (it.toString()) {
        "String" -> "text"
        "Integer" -> "integer"
        "Double" -> "double precision"
        "Boolean" -> "boolean"
        else -> throw IllegalArgumentException("This type is not supported")
      }
    }
    val pairs = fields.map { it.name }.zip(sqlTypes)
  }


  fun convertType(s: String): String {
    when (s) {
      "java.lang.String" -> return "text"
      "int" -> return "integer"
      "boolean" -> return "boolean"
      "double" -> return "double precision"
      else -> throw IllegalArgumentException("This type is not supported")
    }
  }

  fun createTable(t: Class<*>) {
    val name = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Class should have a 'Table' annotation")
    val rs = db.metaData.getTables(null, "public", "%", null)
    val tables = ArrayList<String>()
    while (rs.next())
      tables.add(rs.getString(3).substringBefore("_"))
    if (name in tables) return
    val fields = t.declaredFields
    db.createStatement().executeUpdate("create table $name (${fields.map {
      "${it.name} ${convertType(it.type.typeName)}" +
          if (it.annotations.any { it is PrimaryKey }) " primary key" else "" +
              if (it.annotations.any { it is NotNull }) " not null" else ""
    }.joinToString(", ")})")
  }

  fun insert(obj: Any) {
    val tableName = obj::class.java.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    val fields = obj::class.declaredMemberProperties
    val values = fields.map {
      it.isAccessible = true
      println(it.returnType.toString())
      if (it.returnType.toString().equals("kotlin.String"))
        "'${it.getter.call(obj)}'"
      else it.getter.call(obj)
    }
    val rs = db.metaData.getTables(null, "public", "%", null)
    val tables = ArrayList<String>()
    val fullTables = ArrayList<String>()
    while (rs.next())
      fullTables.add(rs.getString(3))
    while (rs.next())
      tables.add(rs.getString(3).substringBefore("_"))
    var table = ""
    fullTables.map { if (it.substringBefore("_").equals(tableName.toLowerCase())) table = it }
    val colums = ArrayList<String>()
    val metaColums = db.metaData.getColumns(null, "public", table, null)
    while (metaColums.next())
      colums.add(metaColums.getString("COLUMN_NAME"))
    println(colums)
    val statement = "insert into $tableName (${ colums.map { it }.joinToString(", ") }) values (${ values.map { it }.joinToString(", ") })"
    println(statement)
      db.createStatement().execute(statement)
  }
}
