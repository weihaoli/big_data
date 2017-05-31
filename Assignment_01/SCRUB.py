from mpi4py import MPI
import numpy as np
import psutil
import time
import gc
import argparse
# import matplotlib.pyplot as plt
np.set_printoptions(threshold='nan')

#setting parser
parser = argparse.ArgumentParser()
parser.add_argument("filename")
args = parser.parse_args()

start = time.time()
i_time, p_time,s_time,os_time,on_time =0, 0, 0, 0,0


#open file and record rank,size of file
comm = MPI.COMM_WORLD
file = MPI.File.Open(comm, args.filename,MPI.MODE_RDONLY, MPI.INFO_NULL)
file1 = MPI.File.Open(comm, 'noise.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)
file2 = MPI.File.Open(comm, 'signal.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)
file3 =  MPI.File.Open(comm, 'LogFIle1.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)
size = file.Get_size()
rank = comm.Get_rank()
remain = size
num_process = comm.Get_size()


#get the size of free memory
read_size = size if size < (psutil.virtual_memory().free)/32 else (psutil.virtual_memory().free)/32
#TODO need to modify here
size_per_process = int(float(read_size)/num_process)


#record the first line of log

if rank == 0:
    
    log = ''
    log += "session start : \n file size: "+str(size) +"\n memory free : " +str(psutil.virtual_memory().free) + " byte"+ "\n size read each time : " + str(read_size) +" byte"




while remain > 0:
#read in file
    buffer_per_process = bytearray(size_per_process)
    offset_per_process = rank * size_per_process
    print rank,float(remain)/size,"remaining ..."
    str(psutil.virtual_memory().free)
    if rank ==0:
        i_time_start = time.time()
         

    file.Read_ordered(buffer_per_process)
    


    if rank ==0:
        
        i_time += (time.time()-i_time_start)
    remain -= num_process * size_per_process
    


# parsing the lines into list,and sorted
    if rank ==0:
        p_time_start = time.time()

    records = buffer_per_process.decode().split('\n')
    del buffer_per_process
    gc.collect()
    im = str(psutil.virtual_memory().free)
    records = [records[i].split(',') for i in xrange(0, len(records))]
#    sorted(records)
    if rank ==0:
        p_time += (time.time()-p_time_start)
    

    
       
    

   


#begining of scrubing
    
    if rank ==0:
        
        s_time_start = time.time()


   
   
    noise = []
    signal = []

    # be careful of index i, it start from 0!
    for i, row in enumerate(records[1:-2]):
       
#broken line identification
        if len(row) !=3 or len(records[i]) !=3 or len(records[i+2]) !=3:
            continue
#if volumn/ price is 0

	elif row[2]=='0' or row[1]=='0.00':
            noise.append(row)
#if price/volumn become unexpected large than before, >1000 times for volumn, >100 for price
        elif len(row[2]) >len(records[i][2])+2 or len(row[1]) >len(records[i][1])+1:
            noise.append(row)
#if price/volumn is negtive, if data is duplicated
        elif '-'in row[2] or '-'in row[1] or row[0] == records[i][0]:

            noise.append(row)

#if string character happens in dataes

        elif 'o' in row[0] or 'O' in row[0]:
#if data from other time windows happens 
            noise.append(row)
        elif row[0][:17] != records[i][0][:17] and row[0][:17] != records[i+2][0][:17]:
            noise.append(row)
#else append to signal
        else:

            signal.append(row)
    if rank ==0:
        
        s_time += (time.time()-s_time_start)
	
    del records
    
    
    




#processing outputting
    noise_data = '\n'.join(','.join(i) for i in noise)+'\n'
    signal_data = '\n'.join(','.join(i) for i in signal)+'\n'


    if rank ==0:
        on_time_start = time.time()
    file1.Write_ordered(noise_data.encode(encoding='UTF-8',errors='strict'))
    if rank ==0:
        
        on_time += (time.time()-on_time_start)
    if rank ==0:
        os_time_start = time.time()
    if rank ==0:
        
        os_time_start = time.time()
    file2.Write_ordered(signal_data.encode(encoding='utf-8',errors='strict'))
    
    
    if rank ==0:
        
        os_time += (time.time()-os_time_start)




          

comm.Barrier()
#record important time and output them to log file, close all file
if rank == 0:
   log += "\ncumulative time of inputting parsing and outputting data: {} ms".format(str(i_time*1000+p_time*1000+on_time*1000+os_time*1000))
   log += "\nTotal time of inputting data: {} ms".format(str(i_time*1000)) 
   log += "\nTotal time of parsing data: {} ms".format(str(p_time*1000))
   log += "\nTotal time of scrubing data: {} ms".format(str(s_time*1000)) 
   log += "\nTotal time of outputting noise data: {} ms".format(str(on_time*1000))
   log += "\nTotal time of outputting signal data: {} ms".format(str(os_time*1000))  +  "\n free memory in run time: "+im + " byte" + "\n all done"
   file3.Iwrite(log.encode(encoding='UTF-8',errors='strict'))
   file3.Close() 
   file.Close()
   file1.Close()
   file2.Close()




