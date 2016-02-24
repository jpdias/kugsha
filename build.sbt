name := "kugsha"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq("org.mongodb.scala" %% "mongo-scala-driver" % "1.1.0",
                            "org.apache.commons" % "commons-lang3" % "3.0",
                            "com.typesafe" % "config" % "1.3.0",
                            "org.jsoup" % "jsoup" % "1.8.3",
                            "org.graphstream" % "gs-core" % "1.3",
                            "org.graphstream" % "gs-ui" % "1.3"
)

