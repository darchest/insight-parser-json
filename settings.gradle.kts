plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}
rootProject.name = "insight-parser-json"

include(":insight")
project(":insight").projectDir = file("../insight")

include(":insight-vendor-postgresql")
project(":insight-vendor-postgresql").projectDir = file("../insight-vendor-postgresql")