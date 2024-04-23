# このDockerfileの使用用途：
# ①Lambda Layerの作成用
# コンテナ検証用
FROM amazonlinux:2

ARG PYTHON_VERSION=3.9.6

RUN yum update -y && yum install -y tar gzip make gcc openssl-devel bzip2-devel libffi-devel zip \
    && curl https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz | tar xz \
    && cd Python-${PYTHON_VERSION} && ./configure && make && make install \
    && cd - && rm -rf Python-${PYTHON_VERSION} \
    # amazon linux 2ではデフォルトのPythonバージョンが2.7.18になっている。
    # Python3に変更するためにはこの処理が必要になる
    && alternatives --install /usr/bin/python python /usr/local/bin/python3 1 \
    && alternatives --install /usr/bin/pip pip /usr/local/bin/pip3 1