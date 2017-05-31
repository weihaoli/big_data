############################################################

### Documentation for Scrub.py

###########################################################
##### reading 
1. I read the size of the file. If it is larger than 1/32 of the free memory, then I cut the file in to pieces,whose size is of 1/32 free memory(name it read size). I used file_read_Order()  
2. record the number of process(name it num_per_process), and each process should read a pieces as read_size/num_per_process  

##### parsing   
3. split the buffer into python list of list, seperated as three elements: "time stamp","price","volumn"  

##### scrubing
4. scrub the data base on :  
(1) if volumn/ price is 0 
(2) if price/volumn become unexpected large than before, >1000 times for volumn, >100 for price  
        
(3) if price/volumn is negtive, if data is duplicated  
      

(4) if wierd characters happens in dates  

       
(5) if data from other time windows appears   

 And I append them into string  
##### outputing  
 output it to the signal.txt,noise.txt, LogFIle1.txt  

##### trade-off and things need tobe improve:
1. I should record the remaining file size to decide the program determination condition, rather than iteration times. The previous one will be more precise.  
2. there is a good number for the size of chunk I can read each time, here I decide the number based on 1/32 of the free memory. But the good number should be , more precisly, based on CPU performance  
free memroy, and file size etc 1/32 runs safer  
3. because the number I chose, memory overflow problem will occur on some PC  

##### bug:
1. bugs might happens due to number of processes and chunke read each time.  
how to fix:    
1. with same parameter and try 1-2 more times.  
2. change number of processes or chunk read per time will work.  
3. TODO, will find some automatic solution later.  
