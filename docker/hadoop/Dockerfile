# HDFS image built from the official Hadoop release. The apache/hadoop image is
# amd64-only and crashes under emulation on Apple Silicon; this one runs natively
# on both amd64 and arm64.
FROM eclipse-temurin:11-jre

ARG HADOOP_VERSION=3.4.1
ARG HADOOP_TGZ=hadoop-${HADOOP_VERSION}-lean.tar.gz
# the CDN is fast but only keeps current releases; the archive keeps all of them but is slow
ARG CDN=https://dlcdn.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}
ARG ARCHIVE=https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}

RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && cd /tmp \
 && (curl -fsSLO "${CDN}/${HADOOP_TGZ}" || curl -fsSLO "${ARCHIVE}/${HADOOP_TGZ}") \
 && curl -fsSLO "${ARCHIVE}/${HADOOP_TGZ}.sha512" \
 && sha512sum -c "${HADOOP_TGZ}.sha512" \
 && tar -xzf "${HADOOP_TGZ}" -C /opt \
 && mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
 && rm -rf /opt/hadoop/share/doc /tmp/hadoop-*

ENV HADOOP_HOME=/opt/hadoop
ENV PATH="/opt/hadoop/bin:${PATH}"
COPY core-site.xml hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
