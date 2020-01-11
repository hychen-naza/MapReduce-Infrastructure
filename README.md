# MapReduce-Infrastructure

It's a project of Advaced OS (6210) course from Gatech

## Install

  1. `grpc` [How to Install](https://github.com/grpc/grpc/blob/master/INSTALL.md)
  2. `protocol buffer` [How to Install](https://github.com/google/protobuf/blob/master/src/README.md) 
    - You need version 3.0 of protoc to be able to generate code from the given proto files.
    - I would suggest the best place to install this version is from the grpc repository itself. (`grpc/third_party/protobuf/`)
    - In the instructions for installing protoc 3.0, `make check` might fail. continue installing even after that. If compilation works fine with this new protoc, all set for the project.
  3. You need to be able to compile c++11 code on your Linux system
  
## One way to get installed
1. [sudo] apt-get install build-essential autoconf libtool
2. Create a new directory somewhere where you will pull code from github to install grpc and protobuf.
     Then: `cd  $this_new_dir`
2. git clone --recursive `-b $(curl -L http://grpc.io/release)` https://github.com/grpc/grpc
3. cd  grpc/third_party/protobuf
4. sudo apt-get install autoconf automake libtool curl make g++ unzip
5. ./autogen.sh (it might fail, try further steps anyhow, might not create problems)
6. ./configure
7. sudo make
8. make check (it might fail, try further steps anyhow, might not create problems)
9. sudo make install
10. sudo ldconfig
11. cd ../../
12. make
13. sudo make install 

## Design

### Master:

To handle straggler problem, the master keeps a task scoreboard, indicating the status of a task - not start, in progress or complete. When the map phase or the reduce phase is close to end, there might be several in progress tasks due to straggler problem, and no unstart tasks. At this point, master will keep picking an in progress task randomly and sending to idle worker until all tasks are finished. 

To handle worker failures, as I find that the returned worker address from failed worker is invalid, so I just check the validity of the worker address from the reply. If it is invalid, I will reduce the number of live workers, increase the number of failed workers and reject this reply. Because this worker address never get back to ready_worker_vector, master will never assign task to it anymore. But now, I don't know which worker failed. To solve it, at the beginning of mapreduce, I start a child thread to check the current and previous tasks of every worker. If the current and previous tasks of a worker are different, I can ensure this worker is still alive; If tasks are identical, I can't say for sure this worker is failed maybe it is doing this one task during the time period. I can determine the failed workers only when the number of failed workers is equal to the number of workers having same tasks during the check (because failed workers always do same tasks). The value of time interval between two check is critical, if the value is too small, it will be hard to discover the failed workers since a normal live worker may still do the same task; if the value is large, yes I can easily find failed workers if time allowed, but if the entire mapreduce process is really short, I have no enough time to check (like in our project).

So I test my code in two situations. Firstly, I test code when each task assigned to worker requires short time to proceed. I set the time interval 1ms (line 411 in master.h) in master and let worker automatically exits (line 51 in worker.h). Secondly, I test code when each task assigned to worker needs quiet long time to proceed. I set the time interval 100ms (line 410 in master.h) in master and let worker sleep 2s after finishing work (line 105 in worker.h). The results prove it works fine, so the value of time interval is depending on the proceeding time of each task. Now I have commented these testing codes and only leave "usleep(1000);" in line 411 in master.h.

To manage worker pool and get the status of workers, the master maintain a ready_worker_vector containing all idle workers, and failed_worker_vector containing failed workers. If the worker not in these vectors, we know the worker is doing map work if we are in map phase, or doing reduce work if we are in reduce phase. Once a worker finishes its work, it returns its address to master, and master can push this worker address back to ready_worker_vector. 

When assigning task, the master will first pick a unfinished task according to the task scoreboard and then pick a idle worker from ready_worker_vector. The master starts new thread to receive grpc reply from master. When receiving reply from the worker, the child thread will update the task scoreboard and then put the worker back to ready_worker_vector. All these operations are atomic because both master and child thread will update scoreboard and ready_worker_vector.

### Mapper:
	
For every shard, the worker will yield N interim files, N is equal to the number of final output files. Every word will be hashed by its first character (denoted as c), and according to the hash value (hash value = c % N), each word will be written to its corresponding file. In this way, interim files are also grouped by the hash value of words.

### Reducer:
	
Master will send all interim files in the same group to one reducer (in our project, we have 8 final output files, thus 8 groups of interim files). The reducer does the reducing work and returns the output file. Master will check if the output file is duplicated, if not, master will rename it and put it into output dir.


### Output:
	
My counting result matches the expected counting result using the test script provided in piazza.


P.S
	In my machine, I use 127.0.0.1 instead of localhost. Otherwise, I will have "Address family not supported by protocol" error. Maybe in your test machine and environment, it will not be a problem.





