from mpi4py import MPI
import numpy as np
import psutil
import time
import gc
from scipy import stats

#set up variables and open files
np.set_printoptions(threshold='nan')
i_t,c_t = 0,0
i_total = 0
start = time.time()
i_time, process_time, o_time = 0, 0, 0

procss_time_start = time.time()
comm = MPI.COMM_WORLD
file = MPI.File.Open(comm, 'signal.txt',MPI.MODE_RDONLY, MPI.INFO_NULL)
file3 =  MPI.File.Open(comm, 'LogFIle2.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)

size = file.Get_size()
rank = comm.Get_rank()



remain = size

num_process = comm.Get_size()


#get the size of free memory
read_size = size if size < (psutil.virtual_memory().free)/32 else (psutil.virtual_memory().free)/32

size_per_process = int(float(read_size)/num_process)


#logging
if rank == 0:
    
    log = ''
    log += "session start : \n file size: "+str(size) +"\n memory free : " +str(psutil.virtual_memory().free) + "\n memory read each time : " + str(read_size)
    



price_per_second = []


while remain > 0:
    if rank ==0:
#read file
        i_t = time.time()
    buffer_per_process = bytearray(size_per_process)
    offset_per_process = rank * size_per_process
    print float(remain)/size,"remaining ..."
    #if float(remain)/size < 0.005:
     #   break

   
    file.Read_ordered(buffer_per_process)
    remain -= num_process * size_per_process
#parsing and sorting    
    records = buffer_per_process.decode().split('\n')
    del buffer_per_process
    gc.collect()
    records = [records[i].split(',') for i in xrange(0, len(records))]
    sorted(records)
    



    


   
    

    # be careful of index i, it start from 0!
    for i, row in enumerate(records[1:-2]):
#broken line handling        
        if len(row) !=3 or len(records[i]) !=3 or len(records[i+2]) !=3:
            continue

#get the first record in 1 second interval
        elif row[0][:17] != records[i][0][:17]:
            price_per_second.append(float(row[1]))
    
    
    if rank == 0:
        i_total +=  time.time() - i_t

        

    del records
    


    im = psutil.virtual_memory().free

#gather informations
#comm.Barrier()
collection = comm.gather(price_per_second,root =0)

#calculate returnes
if rank == 0:
    c_time = time.time()
    test_set = []
    for i in collection:
        test_set += i
    test_set = np.array(test_set[1:])
    test_set = (test_set[1:]-test_set[:-1])/test_set[:-1]
     
 #compute nessesary statistics
   
    describe = stats.describe(test_set)
    log += "\n running time memory is : " + str(im)+"\n"
    log += "\n the number of observations is : " + str(describe[0])
    log += "\n the minimum number of observations is : " + str(describe[1][0])
    log += "\n the maximum number of observations is : " + str(describe[1][1])
    log += "\n the mean of observations is : " + str(describe[2])
    log += "\n the variance of observations is : " + str(describe[3])
    log += "\n the skewness of observations is : " + str(describe[4])
    log += "\n the kurtosis of observations is : " + str(describe[5])     

#normal test
    k,p = stats.mstats.normaltest(test_set)
    log += "\n z-score from kurtosis test is : " +str(k)
    log += "\n 2-sided chi squared probability for the hypothesis test is : " +str(float(p))

    if p < 0.05:
        log +='\n under 95% confident interval, the sample is not nomal\n '
    else:
        log +='\n under 95% confident interval, the sample is not nomal\n '

prcoess_time_end = time.time()

process_time = prcoess_time_end-procss_time_start


#logging
if rank == 0:
   c_end = time.time()-c_time
   log += "\nTotal time of  IO,pre-calculation of returns: {} ms ".format(str(i_total*1000)) 
   log += "\nTotal time of  statistical calculation: {} ms ".format(str(c_end*1000)) 
   log += "\nTotal time of processing data: {} ms".format(str(process_time*1000))  
   file3.Iwrite(log.encode(encoding='UTF-8',errors='strict')) 

            

comm.Barrier()
file.Close()
file3.Close()




