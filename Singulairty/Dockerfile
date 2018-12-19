FROM ubuntu:16.04
LABEL maintainer "xin.zhang@longrunweather.com"

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV NETCDF=/usr
ENV PATH=.:$PATH

# install basic tools
RUN apt-get update \
    && apt-get install -y --no-install-recommends software-properties-common \
    && apt-add-repository ppa:ubuntu-toolchain-r/test \
    && apt-add-repository ppa:george-edison55/cmake-3.x \
    && apt-get update \
    && buildDeps='ssh automake autoconf locales pkg-config git time cmake csh ksh vim file curl wget texinfo flex bison libpng-dev libjasper-dev libnetcdf-dev libnetcdff-dev netcdf-bin ncl-ncarg mlocate gcc gfortran g++ git-flow python-dev libtool bc' \
    && apt-get install -y --no-install-recommends $buildDeps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -fs /usr/lib/x86_64-linux-gnu/libnetcdf.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libnetcdff.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libnetcdff.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libnetcdff.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjasper.a usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjasper.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjasper.so.1 usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjasper.so.1.0.0 usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libpng.a usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libpng.so usr/lib/.  \
    && ln -fs /usr/lib/x86_64-linux-gnu/libpng12.a usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libpng12.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libpng12.so.0 usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjpeg.a usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjpeg.so usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjpeg.so.8 usr/lib/. \
    && ln -fs /usr/lib/x86_64-linux-gnu/libjpeg.so.8.0.2 usr/lib/. \
    && git clone https://github.com/open-mpi/ompi.git \
    && cd ompi \
    && ./autogen.pl \
    && ./configure --prefix=/usr/local \
    && make \
    && make install \
    && /usr/local/bin/mpicc examples/ring_c.c -o /usr/bin/mpi_ring \
    && rm -rf /ompi

CMD ["/bin/bash" , "-l"]
