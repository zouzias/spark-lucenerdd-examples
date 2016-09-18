organization := "org.zouzias"
name := "spark-lucenerdd-examples"
version := "0.0.24"
scalaVersion := "2.10.6"
val sparkV = "1.6.2"

javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint")

libraryDependencies ++= Seq(
	"org.zouzias" %% "spark-lucenerdd" % version.value,
	"org.apache.spark" %% "spark-core" % sparkV % "provided",
	"org.apache.spark" %% "spark-sql" % sparkV % "provided" ,
	"com.holdenkarau"  %% "spark-testing-base" % s"${sparkV}_0.4.5" % "test" intransitive(),
	"org.scala-lang"    % "scala-library" % scalaVersion.value % "compile"
)

enablePlugins(DockerPlugin)

mainClass in assembly := Some("org.zouzias.spark.lucenerdd.examples.wikipedia.WikipediaSearchExample")


dockerfile in docker := {
	// The assembly task generates a fat JAR file
	val artifact: File = assembly.value
	val artifactTargetPath = s"/app/${artifact.name}"

	new Dockerfile {
		from("java")
		add(artifact, artifactTargetPath)
		entryPoint("java", "-jar", artifactTargetPath)
		expose(8299)
	}
}
