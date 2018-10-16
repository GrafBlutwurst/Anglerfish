
organization := "ch.grafblutwurst"
name := "anglerfish"

version := "0.1.10"

crossScalaVersions := Seq("2.11.8", "2.12.4")

resolvers += Resolver.sonatypeRepo("releases")


val refinedVersion = "0.9.2"
val derivingVersion = "1.0.0"
val circeVersion = "0.10.0"

libraryDependencies ++= Seq(
  "org.apache.avro"     % "avro"                        % "1.8.2",
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
  "com.codecommit"             %% "shims"               % "1.4.0",
  "org.scalaz" %% "deriving-macro" % derivingVersion,
  compilerPlugin("org.scalaz" %% "deriving-plugin" % derivingVersion),

  // the scalaz-deriving Altz / Decidablez / Deriving API and macros
  "org.scalaz" %% "scalaz-deriving" % derivingVersion,

  // instances for Show and Arbitrary
  "org.scalaz" %% "scalaz-deriving-magnolia" % derivingVersion,
  "org.scalaz" %% "scalaz-deriving-scalacheck" % derivingVersion,

)

scalacOptions in Test ++= Seq("-Yrangepos")

addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.7")


parallelExecution in Test := false
