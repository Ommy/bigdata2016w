Assignment 3
===

## Question 1

Running `du -h` on the output from the following command:

```
$ hadoop jar target/bigdata2016w-0.1.0-SNAPSHOT.jar \
   ca.uwaterloo.cs.bigdata2016w.Ommy.assignment3.BuildInvertedIndexCompressed \
   -input data/Shakespeare.txt -output idx -reducers 4
```

gives me the following:

```
900K	idx/part-r-00000
1004K	idx/part-r-00001
852K	idx/part-r-00002
1.1M	idx/part-r-00003
3.8M	idx
```

## Question 2

Running `du -h` on the Wikipedia collection index gives me the following:

```
107M	idx/part-r-00000
105M	idx/part-r-00002
112M	idx/part-r-00001
112M	idx/part-r-00003
434M	idx
```

## Question 3

1. Eurostar
2. Railway platform
3. Andy Bechtolsheim
4. List of civil parishes in Hampshire
5. Institute for Quantum Computing
6. List of University of Wisconsin–Madison people in academics

## Question 4

1. Amazon.com
2. Criticism of Facebook


