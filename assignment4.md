Assignment 4
==

I was able to get single source node working with a multiple node implementation. This means that I have all the code there to support multiple nodes, and it will run through multiple nodes, but I couldn't ultimately get it working. The problem I'm seeing is that as I add more nodes, the numbers start to skew. At 3 nodes, the masses being written to disk from `phase1` were all becoming `NaN`. There is some bug in my code that is influencing the rest of the values, but I am unable to determine where the bug is.

The main reason why I wasn't fully able to complete it is that while we were given an extension, my other courses all packed together their tests, quizzes, and assignments, and so I only had 1 full day to work on this. 

I have not been able to test on Altiscale for the single node implementation.

# Assignment 4 Marks

| SL  |  ML | SA | SA |
| --- | --- | --- | --- |
| 5 | 0 | 0 | 0 |


<!--* Penalty: %-->
* Total: 5/55


* SL (15 points)
 * A correct implementation of single-source personalized PageRank is worth 10 points.
 * That we are able to run the single-source personalized PageRank implementation in the Linux Student CS environment is worth 5 points.
* ML (20 points)
 * A correct implementation of multiple-source personalized PageRank is worth 15 points.
 * That we are able to run the multiple-source personalized PageRank implementation in the Linux Student CS environment is worth 5 points.
* SA (10 points)
 * Scaling the single-source personalized PageRank implementation on Altiscale is worth 10 points.
* MA (10 points)
 * Scaling the multiple-source personalized PageRank implementation on Altiscale is worth 10 points.



## Deducted Points Detail
prints only NaN values

```
> Source: 6139
> NaN 7
> NaN 8
> NaN 9
> NaN 10
> NaN 11
> NaN 12
> NaN 13
> NaN 14
> NaN 15
> NaN 6300
```
