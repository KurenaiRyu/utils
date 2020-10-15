import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.4.10"
    application
}
group = "io.github.natsusai"
version = "0.0.1"

repositories {
    mavenCentral()
}
dependencies {
    api("com.esotericsoftware:kryo-shaded:4.0.2")
    api("commons-codec:commons-codec:1.11")
    implementation("org.slf4j:slf4j-api:1.7.30")
    implementation("org.springframework.amqp:spring-rabbit:2.2.0.RELEASE")
    testImplementation(kotlin("test-junit"))
}
tasks.withType<KotlinCompile>() {
    kotlinOptions.jvmTarget = "11"
}
application {
    mainClassName = "MainKt"
}