#!/bin/bash

SOURCE=../split-dataset
DEST=../split-dataset/test
#CMD=hdfs dfs -mv
CMD=mv
i=`ls $SOURCE | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
#i=1
while [ "$i" -ne 0 ]
do
    file=$i.csv
    echo "Moving file: "$file
    $CMD $SOURCE/$file $DEST/$file
    i=`ls $SOURCE | grep '.csv' | sort -n | head -1 | cut -d '.' -f1`
    sleep 5
done
