package net.orcades.db;

trait Cruded[A] {
  def insert(a: A): Long
  def all: List[A]
}

