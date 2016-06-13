organization := "org.zouzias"
name := "spark-lucenerdd.examples"

scalaVersion := "2.11.8"
val sparkV = "1.6.1"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % "0.0.13",
	"org.apache.spark" %% "spark-core" % sparkV % "provided",
	"org.apache.spark" %% "spark-sql" % sparkV % "provided" ,
	"com.holdenkarau"  %% "spark-testing-base" % s"${sparkV}_0.3.3" % "test" intransitive(),
	"org.scala-lang"    % "scala-library" % scalaVersion.value % "compile"
)

