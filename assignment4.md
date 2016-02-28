Assignment 4
==

I was able to get single source node working with a multiple node implementation. This means that I have all the code there to support multiple nodes, and it will run through multiple nodes, but I couldn't ultimately get it working. The problem I'm seeing is that as I add more nodes, the numbers start to skew. At 3 nodes, the masses being written to disk from `phase1` were all becoming `NaN`. There is some bug in my code that is influencing the rest of the values, but I am unable to determine where the bug is.

The main reason why I wasn't fully able to complete it is that while we were given an extension, my other courses all packed together their tests, quizzes, and assignments, and so I only had 1 full day to work on this. 

I have not been able to test on Altiscale for the single node implementation.
