import sbtassembly.MergeStrategy

organization := "ch.grafblutwurst"
name := "anglerfish"

version := "0.1.3"

crossScalaVersions := Seq("2.11.8", "2.12.4")

resolvers += Resolver.sonatypeRepo("releases")


val refinedVersion = "0.9.2"
val circeVersion = "0.9.3"

libraryDependencies ++= Seq(
  "org.apache.avro"     % "avro"                        % "1.8.2",
  "org.typelevel"       %% "cats-effect"                % "1.0.0-RC2",
  "io.circe"            %% "circe-core"                 % circeVersion,
  "io.circe"            %% "circe-parser"               % circeVersion,
  "io.circe"            %% "circe-generic"              % circeVersion,
  "io.circe"            %% "circe-refined"              % circeVersion,
  "eu.timepit"          %% "refined"                    % refinedVersion,
  "org.apache.avro"     %  "avro-mapred"                % "1.8.2",
  "com.slamdata"        %% "matryoshka-core"            % "0.21.3",
  "org.scalacheck"      %% "scalacheck"                 % "1.14.0" % "test",
  "com.sksamuel.avro4s" %% "avro4s-core"                % "1.9.0" % "test",
  "com.sksamuel.avro4s" %% "avro4s-json"                % "1.9.0" % "test",
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.6" % "test",
  "com.codecommit" %% "shims" % "1.4.0"
)

scalacOptions in Test ++= Seq("-Yrangepos")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")


parallelExecution in Test := false
