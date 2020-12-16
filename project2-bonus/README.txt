Project 2 – Gossip Simulator Report

1. Group members
Name: Zixun Wang             UFID: 3725-9823
Name: Yixin Wei              UFID: 5114-6181

2. What is working

We have implemented all the four topologies and two algorithms and tested the convergence for both algorithms and topologies.
In addition, we have implemented the bonus part and tested the failure model with parameter (percent of killed nodes) for all the four topologies and two algorithms.
The gossip algorithm and push-sum algorithm can reach 100% convergence for all the four topologies except some special cases.

3. What is the largest network you managed to deal with for each type of topology and algorithm?

               | LINE          | FULL          | 2D            |  IMP 2D     |
 GOSSIP        | 20000 nodes   | 10000 nodes   | 20000 nodes   | 20000 nodes |
    time       | 586043 ms     | 466966 ms     | 493217 ms     | 556421 ms   | 
 PUSH-SUM      | 1500  nodes   | 10000 nodes   |  3000 nodes   | 100000 nodes|
    time       | 1039055 ms    | 123511 ms     |  2077 ms      | 19677 ms    |

We have limited the maximum time to be 10 minute to calculate the maximum nodes int gossip algorithm.
And we have limited the maximum time to be 20 minute to calculate the maximum nodes int push-sum algorithm.