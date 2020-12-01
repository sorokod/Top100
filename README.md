# Top100

Find the top 100 most frequent long values in a large (4 billion) data file

---
For looking at the code, a good starting point is `src/main/kotlin/org/xor/top100/Main.kt`. The approach is as follows:

1. Generated a data file
2. Sort and merge. Since the file would not fit in the memory, we are doing an external sort. Chunks of the data file
are sorted individually (using [`MappedByteBuffer`s](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/MappedByteBuffer.html))    
and then merged into a fully sorted file.
3. The top 100 most frequent values are then extracted. Because the data is sorted, the distinct values are contiguous 
and counting them while traversing the data once, gives us the global frequency.
 
### Execution times

Running with 8 gigabytes of memory ( `-Xmx8G -Xms8G` ) we get:

#### 1 billion values
1. Generated a data file: 7.5G in 33 sec.
2. Sort and merge: 160 sec.
3. Top 100: 25 sec.

Total time: 220 sec. 
 
#### 4 billion values
1. Generated a data file: 30G in 140 sec.
2. Sort and merge: ~ 25 min.
3. Top 100: 100 sec.

Total time: ~ 30 min 
 
 
 