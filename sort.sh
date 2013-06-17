declare -i maxheap
maxheap=8
maxheap+=$2
java -Xmx${maxheap}m -XX:MaxPermSize=8m -jar target/parallel-sort-1.0-jar-with-dependencies.jar $1 $2 2>&1
