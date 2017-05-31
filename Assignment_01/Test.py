from mpi4py import MPI
import numpy as np
import psutil
import time
# import matplotlib.pyplot as plt
np.set_printoptions(threshold='nan')

start = time.time()
i_time, process_time, o_time = 0, 0, 0

procss_time_start = time.time()
comm = MPI.COMM_WORLD
file = MPI.File.Open(comm, 'data-big.txt',MPI.MODE_RDONLY, MPI.INFO_NULL)
file1 = MPI.File.Open(comm, 'noise.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)
file2 = MPI.File.Open(comm, 'signal.txt', amode = MPI.MODE_RDWR+MPI.MODE_CREATE)
import gc
size = file.Get_size()
rank = comm.Get_rank()



remain = size

num_process = comm.Get_size()


#get the size of free memory
read_size = size if size < (psutil.virtual_memory().free)/16 else (psutil.virtual_memory().free)/16
# need to modify here
size_per_process = int(float(read_size)/num_process)








while remain > 0:
    buffer_per_process = bytearray(size_per_process)
    offset_per_process = rank * size_per_process

    print float(remain)/size,"remaining ..."
    print rank, "begin to read",remain
    file.Read_ordered(buffer_per_process)
    remain -= num_process * size_per_process
    print rank, "begin decoding"
    records = buffer_per_process.decode().replace(':', ',').split('\n')
    del buffer_per_process
    gc.collect()
    print rank, "finish spling "
    
    
    records = [records[i].split(',') for i in xrange(0, len(records))]

    

    print rank, "finish parsing",len(records[1:-2])

    # identify broken line



    # here is a magic I dont understand: Array.size!= len(array)
    #code does not work
    for row in records:
        if len(row) != 6:
            del row

    #records = records[1:-2]
    gc.collect()
    noise = ''
    signal = ''

    # be careful of index i, it start from 0!
    for i, row in enumerate(records[1:-2]):

        if len(row) !=6 or len(records[i]) !=6 or len(records[i+2]) !=6:
            continue

        elif float(row[5]) < 10 or float(row[5]) < 10 or row[3] == records[i][3]:

            noise += '\n'+ str(row)



        elif row[0]!= records[i][0] and row [0] != records[i + 2][0]:

            noise += '\n'+ str(row)

        elif row[1]!= records[i][1] and row [1] != records[i + 2][1]:

            noise +='\n'+ str(row)

        elif row[2]!= records[i][2] and row [2] != records[i + 2][2]:

            noise += str(row)

        else:

            signal +='\n'+ str(row)
    del records
    print rank, "finish processing"
    gc.collect()



    #noise = np.array_str(records[[not i for i in count]])





    file1.Write_ordered(noise.encode(encoding='UTF-8',errors='strict'))
    file2.Write_ordered(signal.encode(encoding='utf-8',errors='strict'))
    print rank, "finish writing"



            


file.Close()
file1.Close()
file2.Close()

prcoess_time_end = time.time()

process_time = prcoess_time_end-procss_time_start
print "Total time of processing data: {} seconds".format(str(process_time))