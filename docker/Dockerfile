FROM openjdk:7

#from https://hub.docker.com/r/hseeberger/scala-sbt/~/dockerfile/

ENV SCALA_VERSION 2.9.2
ENV SBT_VERSION 0.12.4

RUN \
  curl -fsL http://www.scala-lang.org/files/archive/scala-$SCALA_VERSION.tgz | tar xfz - -C /root/ && \
  echo >> /root/.bashrc && \
  echo 'export PATH=~/scala-$SCALA_VERSION/bin:$PATH' >> /root/.bashrc

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  sbt sbt-version


RUN apt-get install git


CMD ["bash"]