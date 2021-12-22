#!/bin/bash

echo "Extrayendo video_price"
tar xjvf datos/video_price.tbz --directory datos --wildcards --strip=1 > /dev/null
echo "Extrayendo video"
tar xjvf datos/video.tbz       --directory datos --wildcards --strip=1 > /dev/null

rm -rf   "datos/video_split" "datos/video_price_split"
mkdir -p "datos/video_split" "datos/video_price_split"

split --verbose --numeric-suffixes --suffix-length=3 --lines=100000 --separator=$'\2' "datos/video"       "datos/video_split/video_"
split --verbose --numeric-suffixes --suffix-length=3 --lines=100000 --separator=$'\2' "datos/video_price" "datos/video_price_split/video_price_"
