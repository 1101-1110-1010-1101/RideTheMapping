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
      if (it.returnType.toString().equals("kotlin.String"))
        "'${it.getter.call(obj)}'"
      else it.getter.call(obj)
      //println(it.name + " " + it.getter.call(obj))
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
    val statement = "insert into $tableName (${ colums.map { it }.sorted().joinToString(", ") }) values (${ values.map { it }.joinToString(", ") })"
    println(statement)
    try {
      db.createStatement().execute(statement)
    } catch (p: PSQLException) {
      createTable(obj::class.java)
      insert(obj)
    }
  }

  fun select(t: Class<*>, column: String? = "*", id: Int? = null): ArrayList<ArrayList<String>> {
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    var statement = "select $column from $tableName"
    if (id != null) { statement += " where id = $id" }
    val result = db.createStatement().executeQuery(statement)
    val resArray = ArrayList<String>()
    val answer = ArrayList<ArrayList<String>>()
    if (column.equals("*")) {
        while (result.next()) {
          for (cName in t.declaredFields)
            resArray.add(result.getString(cName.name))
        }
      while (resArray.size != 0) {
        answer.add(resArray.subList(0, t.declaredFields.size).toCollection(ArrayList()))
        for (i in 0..(t.declaredFields.size - 1)) {
          resArray.remove(resArray[0])
        }
      }
    } else {
      while (result.next())
        resArray.add(result.getString(column))
      while (resArray.size != 0) {
        answer.add(arrayListOf(resArray[0]))
        resArray.remove(resArray[0])
      }
    }
    return answer
  }

  fun update(t: Class<*>, column: String, value: Any, id: Int){
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    var tValue = ""
    if (value.javaClass.typeName.equals("java.lang.String"))
      tValue = "'$value'"
    else tValue = value.toString()
    val statement = "update $tableName set $column = $tValue where id = $id"
    println(statement)
    db.createStatement().execute(statement)
  }

  fun delete(t: Class<*>, id: Int) {
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    val statement = "delete from $tableName where id = $id"
    db.createStatement().execute(statement)
  }
}
