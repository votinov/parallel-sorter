set /a maxheap=8+%2
echo %maxheap%
java -Xmx%maxheap%m -XX:MaxPermSize=8m -jar target/parallel-sort-1.0-jar-with-dependencies.jar %1 %2 2>&1
