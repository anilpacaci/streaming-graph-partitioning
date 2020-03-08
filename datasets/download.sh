#!/bin/bash

REPO_HOME="https://vault.cs.uwaterloo.ca/s/7SEPzsQqybDyCTR"
URL_PREFIX="${REPO_HOME}""/download?path=%2F&files="

TWITTER_ARCHIVE="twitter_compressed.tar.gz"
UK2007_ARCHIVE="uk2007_compressed.tar.gz"
USROAD_ARHIVE="usroad_compressed.tar.gz"
LDBC_ARCHIVE="sf1000_friendship_adjacency.tar.gz"

echo "Downloading twitter"
wget "$URL_PREFIX$TWITTER_ARCHIVE" -O "${TWITTER_ARCHIVE}"

echo "Decompressing twitter"
tar -xzvf "${TWITTER_ARCHIVE}"

echo "Remove compressed archive"
rm -rf "${TWITTER_ARCHIVE}"

echo "Downloading uk2007-05"
wget "$URL_PREFIX$UK2007_ARCHIVE" -O "${UK2007_ARCHIVE}"

echo "Decompressing uk2007-05"
tar -xzvf "${UK2007_ARCHIVE}"

echo "Remove compressed archive"
rm -rf "${UK2007_ARCHIVE}"

echo "Downloading us-road"
wget "$URL_PREFIX$USROAD_ARCHIVE" -O "${USROAD_ARCHIVE}"

echo "Decompressing us-road"
tar -xzvf "${USROAD_ARCHIVE}"

echo "Remove compressed archive"
rm -rf "${USROAD_ARCHIVE}"

echo "Downloading dbc-sf1000"
wget "$URL_PREFIX$LDBC_ARCHIVE" -O "${LDBC_ARCHIVE}"

echo "Decompressing ldbc-sf1000"
tar -xzvf "${LDBC_ARCHIVE}"

echo "Remove compressed archive"
rm -rf "${LDBC_ARCHIVE}"

echo "Download complete !!!"
