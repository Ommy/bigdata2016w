# Assignment 6

## Question 1

| Trained On | 1-ROCA%|
|------------|--------|
| `group_x`  | 17.25  |
| `group_y`  | 12.82  |
| `britney`  | 15.03  |

## Question 2

1-ROCA% for the average method was 13.03

## Question 3

1-ROCA% for the voting method was 12.20

## Question 4

| Classifier            | 1-ROCA% |
| ----------------------|---------|
| ApplySpamClassifier   | 16.62   |

## Question 5

| Iteration | 1-ROCA%|
|-----------|--------|
|     0     | 15.33  |
|     1     | 14.05  |
|     2     | 15.28  |
|     3     | 15.70  |
|     4     | 15.83  |
|     5     | 14.79  |
|     6     | 17.57  |
|     7     | 17.51  |
|     8     | 16.10  |
|     9     | 15.12  |

The average 1-ROCA% of the 10 iterations is 15.728


Marks:
Compilation: 4/4
TrainSpamClassifier: 14/15
ApplySpamClassifier: 5/5
ApplyEnsembleClassifier: 6/6
Shuffle implementation: 5/5
Question Answers: 15/15
Runs on Altiscale: 10/10
Total: 59/60

- Curious that voting achieved a better score for your answers than average
- When training, the weight map should really just be in the flatMap/mapPartitions would not work as intended if program was more complex
  - You end up copying the map rather than directly accessing it
