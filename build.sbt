organization := "org.zouzias"
name := "spark-lucenerdd-examples"
scalaVersion := "2.12.18"
val sparkV = "3.3.3"

javacOptions ++= Seq("-source", "1.11", "-target", "1.11", "-Xlint")

resolvers += "Apache Repos" at "https://repository.apache.org/content/repositories/releases"
resolvers += "OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % version.value,
	"org.apache.spark" %% "spark-core" % sparkV % "provided",
	"org.apache.spark" %% "spark-sql" % sparkV % "provided"
)

assembly / mainClass := Some("org.zouzias.spark.lucenerdd.examples.wikipedia.WikipediaSearchExample")


// To avoid merge issues
assembly / assemblyMergeStrategy := {
    case PathList("module-info.class", xs @ _*) => MergeStrategy.first
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}
