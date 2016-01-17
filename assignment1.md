Assignment 1
===

## Question 1

#### Pairs

In the pairs implementation, there were 2 MapReduce jobs. In order to calculate PMI, I would need the word count of `x` in order to calculate `p(x)`. So the first MapReduce job did two things. One, it did a normal word count like we had in assignment 0. The other task it did was a line count of the input passed in. This was done through the use of counters through the Hadoop context. Once the job had finished, the counter is retreived from the job and passed onto the configuration for the second job to read.

The second MapReduce job would then do the mapping of each word to its pair. The map job would go through the list of words and pair them up into the `PairOfString` object. In the reduce job, in the setup function, it would load up the result of the word count job and serialize the file into a HashMap. Then the reduce job would sum up the remaining values for the specific key and calculate the PMI, utilizing the word count loaded in.

#### Stripes

The stripes implementation was done very similarily to the pairs implementation. It had 2 MapReduce jobs, and the first one was the word count. The first job, similarily, calculated the total number of lines in the input file via counters. This was then saved to a temporary directory in HDFS. The counter is retreived once again and passed to the second job.

The second MapReduce job would then create a HashMap of String to Float using the `HMapStF` object. Here the mapper would go through each word and either create a new entry in the map if no key existed or increment the specific key within the map. Basically it paired each word with words found on the same line. The combiner would create a new map and sum up all the KVPs it got and store them into the new map. This is then sent off to the reducer where it does the final summation and writes to disk. 

## Question 2

#### On my Laptop

The Pairs PMI implementation runs in 34.128 seconds using the Shakespeare data.

The Stripes PMI implementation runs in 18.111 seconds using the Shakespeare data.

#### On the CS Environment

The Pairs PMI implementation runs in 60.048 seconds using the Shakespeare data.

The Stripes PMI implementation runs in 24.023 seconds using the Shakespeare data.

## Question 3

### On my Laptop

The Pairs PMI implemenation with no combiners runs in 39.927 seconds.

The Stripes PMI implementation with no combiners runs in 19.989 seconds

### On the CS Environment

The Pairs PMI implementation with no combiners runs in 73.974 seconds.

The Stripes PMI implementation with no combiners runs in 29.028 seconds.

## Question 4

There ends up being `77,198` unique pairs. If we don't consider the symmetric case since `PMI(x,y) = PMI(y,x)`, then we have `38,599` unique pairs.

## Question 5

The pair `(anjou, maine)` was the highest PMI at `3.633` and `(milford, haven)` was extremely close to it with a PMI of `3.620`.

The reason the pair `(anjou, maine)` got such a high PMI is because the two words are usually together in once sentence. Also, in Shakespeare and early England, King Charles VII of France received two lands for marrying Margaret, which were Maine and Anjou. So, whenever Maine is mentioned in text, it is usually accomanied by Anjou.

## Question 6

#### Tears

`shed` with PMI `2.111`

`salt` with PMI `2.0528`

`eyes` with PMI `1.165`

### Death

`father's` with PMI `1.12025`

`die` with PMI `0.754159`

`life` with PMI `0.73813`

## Question 7

#### Waterloo

`kitchener` with PMI `2.614997`

`napoleon` with PMI `1.908439`

`napoleonic` with PMI `1.7866189`

#### Toronto

`marlboros` with PMI `2.353996`

`spadina` with PMI `2.3126`

`leafs` with PMI `2.31089`
