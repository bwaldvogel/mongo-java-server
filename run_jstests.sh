#!/bin/zsh

for file in jstests/*.js
do
	echo "running $file"
	mongo localhost:27017/test $file || exit
done

echo "successfully finished"
