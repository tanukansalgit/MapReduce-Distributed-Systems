# MapReduce-Distributed-Systems
## OVERVIEW

It contains an implementation of the MapReduce model. It divides the task into two functions: Mapper and Reducer, and uses a key-value store for efficient communication between the master and Mapper/Reducer functions. The end-user provides input/output locations, as well as the number of mappers and reducers to distribute the processing load.
 Here, the end user is obliged to provide the input location, number of mappers, number of reducers  and output location.

## IMPLEMENTATION
The bodies that are contributing to this model are:

1. MAP-REDUCE : It is a user interface, where we initialize the master to start the mapReduce model. It is the one that takes in the user-inputs( number of mappers, number of reducers, input files Array, output files). It also provides the maximum chunk size for each file to the master.

2. INPUT FILES: It is the array of path of the file that user want to process, to generate the output

3. MASTER :The Master is a critical component of the MapReduce model as it serves as the controller or the conductor of the entire process. It manages all the functions required to implement the MapReduce functionality efficiently.
- To begin with, the Master splits all the input files into fixed size chunks, typically 64KB. It then initializes n number of mappers and performs pre-processing for mapper management, such as available mappers, mapper-status, and mapper-output keys.
- Once the initialization is complete, the Master assigns jobs to all the available mappers. It iterates through all the input files that need to be processed and waits for the available mappers to process them. The Master also keeps track of all the mapper status by connecting with the key-value server.
- Once all the input files are processed by the mappers, the Master initializes the reducers and manages them. It also keeps track of the reducer status with the help of the key-value server.
- When the reducers are done processing the data, the Master processes the reducer output generated in the key-value store. It then stores the count-output and inverted-index-output in the output files, providing the final results to the end-user.
- Throughout the entire MapReduce process, the Master plays a crucial role in coordinating and managing the mappers and reducers. It also monitors the progress of the tasks, handles any errors or failures, and ensures the successful completion of the MapReduce job.

4. MAPPER : This function is taking the file as input, which then processes it and stores its word-existence and word-with-filename in the key-value storage. It stores in key-value storage as keys provided from the master, and value which was computed from the file provided.
Here, we are creating multiple processes for mappers, which ensures a distributed environment.
Also, when a mapper fails, we are adding a job for that mapper in re-processing, which is maintained by a jobsMapping and reProcessing array.

5. REDUCER : This function is using hash() to divide the words between different reducers. It processes all the mapper-generated data in the key-value storage, and processes the word assigned to it( decided by the hash function). The hash function ensures that the same word is processed by the same reducer, irrespective of its location and existence.
Here, we are creating multiple processes for reducers, which ensures a distributed environment.
When a reducer fails, we are re-processing that reducer, in order to maintain the use-case.

6. KEY-VALUE STORAGE: It is managed by a key-value server hosted on some network, which is being used for the intermediate data storage. Also, master uses it to keep track of idle/failed/completed mappers and reducers

7. OUTPUT FILES: “count-output.txt” and “inverted-index-output.txt” are the default files used for providing final output to the user. Users are also obliged to provide these file locations.

## FAULT TOLERANCE
Fault tolerance support is provided for Mapper and Reducers.
When a mapper/reducer is failed, their jobs are re-processed, to maintain the integrity among data and serve the use-case.
When a mapper fails, the job it was performing is again added to the filePaths(for re-processing).
When a reducer fails, it is re-initialised to re-processes the keys assigned to it.
This functionality is tested by multiple use-cases.

## DISTRIBUTED ENVIRONMENT
Here, we are creating multiple processes for multiple mappers.
Multiple Processes are created for multiple reducers.
No direct communication between master and other entities.
Key-Value is hosted in a different environment.

## API_EXPOSED
- mappers - number of mappers
- reducers - number of reducers
- filePaths - Array of all the filePaths for processing
- outputCountFilePath - File-Path for Word-Count
- outputInvertedIndexFilePath - File-Path for Inverted-Index

## ASSUMPTION
1. MapReduce assumes that the key-value store is a reliable shared resource that can be accessed by both mappers and reducers to communicate and exchange data.
2. The model assumes that the input files will be in a specified array format.
3. The model assumes that a key-value store will be available and hosted on another network for intermediate data storage
4. The model assumes that a hash function will be used to divide the words between different reducers.
5. The model assumes that the environment is homogeneous, i.e., all nodes have the same processing capabilities and memory capacity.

## LIMITATIONS
1. Scalability Limitations: The model relies heavily on the availability of a large number of independent, identical processing nodes, which may not always be available, particularly for small-scale systems. Moreover, the model may not scale well for real-time or near-real-time processing needs.
2. Communication Overhead: The use of a key-value store as a communication mechanism between the Master, Mapper and Reducer functions can result in significant communication overhead. This overhead can impact performance, particularly when dealing with large datasets.
3. Data Skew: The hash function used to divide the data between different reducers may not always result in an even distribution of data, which can lead to data skew. This can result in some reducers processing more data than others, leading to performance issues.
4.Single Point of Failure: The Master node acts as the central coordinator for the entire MapReduce process, which means that if the Master fails, the entire process comes to a halt. This can be a significant limitation for large-scale systems where high availability is critical.
5. Lack of Flexibility: The MapReduce model is designed to work with specific types of data processing tasks, which means that it may not be suitable for all types of data processing needs. This lack of flexibility can be a limitation for some applications.






