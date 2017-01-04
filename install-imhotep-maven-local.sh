#!/bin/bash
AWS="https://s3-us-west-2.amazonaws.com/indeedeng-imhotep-build"
VERSION=`curl -s $AWS | egrep -o "imhotep-server-[0-9\.]+(-\w+)?.jar" | head -1 | egrep -o "([0-9]+\.?)+(-\w+)"`

function install_maven {
	artifact=$1
	type=$2

	jarfile=$artifact-$VERSION.$type
	pomfile=$artifact.pom
	
	wget $AWS/$jarfile
	wget $AWS/$pomfile

	mvn install:install-file -Dfile=$jarfile -DgroupId=com.indeed -DartifactId=$artifact -Dversion=$VERSION -Dpackaging=$type -DpomFile=$pomfile
}

install_maven imhotep-server jar
install_maven imhotep-client jar
install_maven imhotep-archive jar

# install parent pom
wget $AWS/imhotep.pom
mvn install:install-file -Dfile=imhotep.pom -DgroupId=com.indeed -DartifactId=imhotep -Dversion=$VERSION -Dpackaging=xml -DpomFile=imhotep.pom

