organization := "org.zouzias"
name := "spark-lucenerdd-examples"
scalaVersion := "2.11.12"
val sparkV = "2.4.8"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases"
resolvers += "OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % version.value,
	"org.apache.spark" %% "spark-core" % sparkV % "provided",
	"org.apache.spark" %% "spark-sql" % sparkV % "provided"
)

mainClass in assembly := Some("org.zouzias.spark.lucenerdd.examples.wikipedia.WikipediaSearchExample")
