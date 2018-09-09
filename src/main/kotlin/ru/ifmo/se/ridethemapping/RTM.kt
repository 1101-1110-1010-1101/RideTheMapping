package ru.ifmo.se.ridethemapping

import javafx.scene.paint.Color
import java.sql.DriverManager
import java.time.ZonedDateTime

class RTM(url: String, username: String, password: String) {
  val db = DriverManager.getConnection(url, username, password)

  annotation class Table(val name: String)

  @Target(AnnotationTarget.FIELD)
  annotation class PrimaryKey()

  @Target(AnnotationTarget.FIELD)
  annotation class NotNull()

  fun convertType(s: String): String {
    when (s) {
      "java.lang.String" -> return "text"
      "int" -> return "integer"
      "boolean" -> return "boolean"
      "double" -> return "double precision"
      else -> return "text"
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
      "${it.name} " +
          if (it.annotations.any { it is PrimaryKey }) { " serial primary key" } else "${convertType(it.type.typeName)}" +
          if (it.annotations.any { it is PrimaryKey }) "" else "" +
              if (it.annotations.any { it is NotNull }) " not null" else ""
    }.joinToString(", ")})")
  }

  fun insert(obj: Any) {
    val tableName = obj::class.java.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    val fields = obj::class.java.declaredFields.filterNot { it.annotations.any { it is PrimaryKey } }
    val values = fields.map {
      it.isAccessible = true
      if (!(it.type.name in (arrayOf("int", "double", "boolean"))))
        "'${it.get(obj)}'"
      else it.get(obj)
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
    val statement = "insert into $tableName (${ fields.map{ it.name }.joinToString(", ") }) values (${ values.map { it }.joinToString(", ") }) returning id"
    println(statement)
      db.createStatement().execute(statement)
  }

  fun <T> selectById(t: Class<T>, id: Int): T {
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    var statement = "select * from $tableName where id = $id"
    val result = db.createStatement().executeQuery(statement)
    val fields = t.declaredFields
    fields.map { it.isAccessible = true }
    val f = t.newInstance()
    while (result.next())
      for (i in 0..fields.size - 1)
        when (fields[i].type.name) {
          "java.lang.String" -> fields[i].set(f, result.getString(i + 1))
          "double" -> fields[i].set(f, result.getDouble(i + 1))
          "int" -> fields[i].set(f, result.getInt(i + 1))
          "javafx.scene.paint.Color" -> fields[i].set(f, Color.valueOf(result.getString(i + 1)))
          "java.time.ZonedDateTime" -> fields[i].set(f, ZonedDateTime.parse(result.getString(i + 1)))
        }
    return f
  }

  fun <T> selectAll(t: Class<T>): List<T> {
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    var statement = "select * from $tableName"
    val result = db.createStatement().executeQuery(statement)
    val resArray = ArrayList<String>()
    val answer = ArrayList<ArrayList<String>>()
      while (result.next()) {
        for (cName in t.declaredFields) {
          resArray.add(result.getString(cName.name))
          println(cName.name)
        }
      }
      while (resArray.size != 0) {
        answer.add(resArray.subList(0, t.declaredFields.size).toCollection(ArrayList()))
        for (i in 0..(t.declaredFields.size - 1)) {
          resArray.remove(resArray[0])
        }
      }
    print(answer)
    val fields = t.declaredFields
    for (field in fields) {
      println(field.type.name)
      field.isAccessible = true
    }
    var res = listOf<T>()
    for (element in answer) {
      val f = t.newInstance()
      for (i in 0..fields.size - 1)
        when (fields[i].type.name) {
          "java.lang.String" -> fields[i].set(f, element[i])
          "double" -> fields[i].set(f, element[i].toDouble())
          "int" -> fields[i].set(f, element[i].toInt())
          "javafx.scene.paint.Color" -> fields[i].set(f, Color.valueOf(element[i]))
          "java.time.ZonedDateTime" -> fields[i].set(f, ZonedDateTime.parse(element[i]))
        }
      res += f
    }
    return res
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

  fun delete(t: Class<*>, id: Int? = null) {
    val tableName = t.annotations.find { it is Table }?.let { (it as Table).name }
        ?: throw IllegalArgumentException("Object should be represented as table")
    var statement = "delete from $tableName"
    if (id != null)
      statement += " where id = $id"
    db.createStatement().execute(statement)
  }
}
