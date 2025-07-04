#!/bin/bash

# Set the source folder and the number of quarters
SRC_FOLDER="worker8501/pt-crawl-test"
QUARTERS=8

# Calculate the number of files per quarter
NUM_FILES=$(ls "$SRC_FOLDER" | wc -l)
FILES_PER_QUARTER=$((NUM_FILES / QUARTERS))

# Create the quarter folders
for i in $(seq 1 $QUARTERS); do
  mkdir -p "${SRC_FOLDER}-${i}"
done

# Move files to quarter folders with progress indicator
i=1
file_count=0
total_files=$NUM_FILES
for file in "$SRC_FOLDER"/*; do
  if [ -f "$file" ]; then
    mv "$file" "${SRC_FOLDER}-${i}"
    ((file_count++))
    printf "\rMoving files... (%d/%d) %d%% complete" $((file_count + (i-1)*FILES_PER_QUARTER)) $total_files $(( (file_count + (i-1)*FILES_PER_QUARTER) * 100 / total_files ))
    if [ $file_count -eq $FILES_PER_QUARTER ]; then
      ((i++))
      file_count=0
    fi
  fi
done
printf "\nDone!\n"
