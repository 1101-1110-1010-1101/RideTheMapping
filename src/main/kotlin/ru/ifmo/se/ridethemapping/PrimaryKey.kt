package ru.ifmo.se.ridethemapping

class PrimaryKey {
  private var key = 1

  fun getNextKey(): Int {
    key += 1
    return key - 1
  }

  fun refershKey() { key = 1 }
}
