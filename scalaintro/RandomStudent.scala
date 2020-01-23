package scalaintro

object RandomStudent extends App {
  val students = """
Thoai
Arthur
Ben H.
Tyler
Alex
Gabby
Morgan
Austin
Yusuf
Stanley
Shiyu
Loder
Daniel
Ryan
Nihil
Nathaniel
Ben T.
Lauren
Yayo
  """.trim.split("\n")
  println(students(util.Random.nextInt(students.length)))
}