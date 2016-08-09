FROM java:7-jdk
MAINTAINER Darren Kastelic <darren@indeed.com>

# --------- protoc install ---------------
RUN apt-get update && apt-get install -y protobuf-compiler libsnappy-dev

# --------- maven install ---------------
ENV MAVEN_VERSION 3.1.1

RUN curl -fsSL http://archive.apache.org/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz | tar xzf - -C /usr/share \
  && mv /usr/share/apache-maven-$MAVEN_VERSION /usr/share/maven \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven

# ------------ build indeed/util -------------
RUN mkdir /src
WORKDIR /src
RUN git clone https://github.com/indeedeng/util.git
WORKDIR /src/util
RUN mvn -DskipTests clean install

# ------------ build indeed/lsmtree ----------
WORKDIR /usr/src
RUN git clone https://github.com/indeedeng/lsmtree.git
WORKDIR /usr/src/lsmtree
RUN mvn -DskipTests clean install 

# --------------------------------------------
VOLUME /root/.m2

# --------------------------------------------
#CMD ["mvn", "clean", "install", "package"]
