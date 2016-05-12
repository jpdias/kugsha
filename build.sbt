import scalariform.formatter.preferences._
import com.typesafe.sbt.SbtScalariform
import com.typesafe.sbt.SbtScalariform.ScalariformKeys

SbtScalariform.scalariformSettings

ScalariformKeys.preferences := ScalariformKeys.preferences.value
  .setPreference(AlignParameters, true)
  .setPreference(DoubleIndentClassDeclaration, true)

name := "kugsha"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.mongodb.scala"          %% "mongo-scala-driver" % "1.1.0",
  "org.apache.commons"         %  "commons-lang3"      % "3.4",
  "com.typesafe"               %  "config"             % "1.3.0",
  "org.jsoup"                  %  "jsoup"              % "1.9.1",
  "org.graphstream"            %  "gs-core"            % "1.3",
  "org.graphstream"            %  "gs-ui"              % "1.3",
  "com.netaporter"             %% "scala-uri"          % "0.4.14",
  "com.github.nscala-time"     %% "nscala-time"        % "2.10.0",
  "com.typesafe.play"          %  "play-json_2.11"     % "2.5.2",
  "org.apache.spark"           %% "spark-core"         % "1.6.1",
  "org.apache.spark"           %% "spark-mllib"        % "1.6.1"
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)
