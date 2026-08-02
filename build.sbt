name := "SCache"

organization := "org.scache"

// Keep the daemon ABI aligned with Spark 3.5 and Celeborn's published Spark 3 client.
scalaVersion := "2.12.18"

// Prefer local Maven cache (helps when working offline / in restricted-network environments).
resolvers += Resolver.mavenLocal

// Scaladoc may warn about unresolved @link targets (especially from Java sources).
// Suppress those link warnings so `sbt doc` output is clean.
Compile / doc / scalacOptions ++= Seq("-no-link-warnings")

libraryDependencies ++= Seq(
  "log4j" % "log4j" % "1.2.17",
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "org.slf4j" % "slf4j-log4j12" % "1.7.21",
  "com.typesafe" % "config" % "1.2.1",
  "com.google.guava" % "guava" % "17.0",
  "com.google.code.findbugs" % "jsr305" % "3.0.0",
  // Keep Netty aligned with modern runtimes (e.g., Spark 3.x) to avoid
  // binary incompatibilities like AbstractMethodError in ReferenceCounted.
  "io.netty" % "netty-all" % "4.1.96.Final",
  "com.esotericsoftware.kryo" % "kryo" % "2.21",
  "org.apache.avro" % "avro" % "1.7.7",
  "commons-io" % "commons-io" % "2.4",
  "com.ning" % "compress-lzf" % "1.0.3",
  "net.jpountz.lz4" % "lz4" % "1.3.0",
  "org.eclipse.jetty" % "jetty-util" % "9.2.16.v20160414",
  "com.twitter" % "chill_2.10" % "0.5.0",
  "com.twitter" % "chill-java" % "0.5.0",
  "org.roaringbitmap" % "RoaringBitmap" % "0.5.11",
  "org.apache.hadoop" % "hadoop-common" % "2.4.0",
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.4.0"
)


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.first
  case PathList("org", "objenesis", xs @ _*) => MergeStrategy.first
  case PathList("org", "objectweb", xs @ _*) => MergeStrategy.first
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
