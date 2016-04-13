sbtResolver <<= (sbtResolver) { r =>
  Option(System.getenv("SBT_PROXY_REPO")) map { x =>
    Resolver.url("proxy repo for sbt", url(x))(Resolver.ivyStylePatterns)
  } getOrElse r
}

resolvers <<= (resolvers) { r =>
  (Option(System.getenv("SBT_PROXY_REPO")) map { url =>
    Seq("proxy-repo" at url)
  } getOrElse {
    r
  }) ++ Seq("local" at ("file:" + System.getProperty("user.home") + "/.m2/repo/"))
}

externalResolvers <<= (resolvers) map identity

addSbtPlugin("com.twitter" %% "sbt-package-dist" % "1.1.0")

addSbtPlugin("com.twitter" %% "scrooge-sbt-plugin" % "3.1.5")