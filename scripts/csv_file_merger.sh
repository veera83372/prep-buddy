#!/usr/bin/env bash

folder=$1

cd $folder
rm merged.csv 2>/dev/null 
files=$(ls | grep 'part')

echo "Files ready to be merged ~>"
echo "$files"
for file in $files; do
	cat $file >> merged.csv;
done

echo "________________________________________________________________________________________________________________________________________"
echo "Files successfully merged. Check 'merged.csv' in '$folder' folder."
echo "________________________________________________________________________________________________________________________________________"