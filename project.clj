(defproject storm-realtime "0.0.0-SNAPSHOT"
  :description "NodeJS marshmallow"
  :dependencies [[marshmallow                             "0.4.11"]]
  :repositories [["rally"     "http://alm-build.f4tech.com:8080/nexus/content/groups/public"]
                 ["releases"  {:url "http://alm-build:8080/nexus/content/repositories/releases"
                               :sign-releases false
                               :username "admin"
                               :password "admin123"}]
                 ["snapshots" {:url "http://alm-build:8080/nexus/content/repositories/snapshots"
                               :username ""
                               :password ""}]]
  :min-lein-version "2.0.0"
)
