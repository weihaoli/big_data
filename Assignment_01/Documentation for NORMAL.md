############################################################

### Documentation for NORMAL.py

###########################################################


#### gathering data
1. I used each process to gather data from signal that we output from last program. And select the the first data in each second interval  
2. We then calculate the second return, and gather them into prcess 0    

#### Statistical calculation
1. Calculate the descritive statists and do normal test.  

####  trade-off and improvement
1. most problems are the same as the previous question
2. looking for new idea to do parrallel programing on statistical analysis.
3. I use mpi.Barrier() here, this slower the speed. I will try some way to get around it.

##### bug:
1. bugs might happens due to number of processes and chunke read each time.  
how to fix:    
1. with same parameter and try 1-2 more times.  
2. change number of processes or chunk read per time will work.  
3. TODO, will find some automatic solution later.   
