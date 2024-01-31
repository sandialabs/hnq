# HNQ Version 0.1

HNQ is a low level benchmark code used to characterize storage hardware performance. The software generates a stream of write or read requests, in a variety of combinations, to be delivered to a standard off the shelf hard drive or raid controller, or any collection of raw devices capable of accepting such requests. The internal buffers used are filled with random data for write requests. The transfer requests are issued in non-deterministic order, while maintaining a certain amount of data locality thought to be typical of an I/O request queue in a parallel file system. The arguments can be varied to control transfer sizes, number of simultaneous requests, concurrent reading and writing, and sequential or random offsets on the device. The resulting output is a report of the measured time to complete the requests on each target device. HNQ can be used to establish the performance characteristics of storage hardware. Differing mixes of I/O traffic can be modeled by appropriate selection of input arguments, and the response of the target hardware is quantified. One possible use mode is for verification of contract specified performance requirements for a hardware purchase.

Ward, Lee

SCR# 1330
