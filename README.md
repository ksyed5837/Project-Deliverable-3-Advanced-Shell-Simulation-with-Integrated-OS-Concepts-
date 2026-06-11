MyShell Deliverable 3 – Memory Management and Process Synchronization
Overview
This project extends the custom shell developed in previous deliverables by integrating memory management and process synchronization concepts commonly found in modern operating systems.
The implementation demonstrates paging, page replacement algorithms (FIFO and LRU), page fault handling, memory allocation/deallocation, and process synchronization using mutexes and semaphores.
________________________________________
Features
Memory Management
•	Paging simulation
•	Fixed-size page frames
•	Page allocation and deallocation
•	Page fault handling
•	FIFO page replacement algorithm
•	LRU page replacement algorithm
•	Memory usage tracking
•	Page hit and page fault statistics
Process Synchronization
•	Producer-Consumer problem simulation
•	Mutex-based synchronization
•	Semaphore implementation
•	Shared buffer protection
•	Race condition prevention
________________________________________
Project Structure
Project_Deliverable3/
│
├── myshell_d3.py
├── Deliverable3_Report.docx
├── README.md
└── screenshots/
________________________________________
Requirements
•	Python 3.8 or higher
Verify Python installation:
python --version
________________________________________
Running the Project
Launch the shell:
python myshell_d3.py
________________________________________
Available Commands
Memory Management Commands
Initialize memory:
meminit <frames>
Example:
meminit 3
Allocate pages:
alloc <process> <page1,page2,...>
Example:
alloc P1 1,2,3
Access a page using FIFO:
access <process> <page> fifo
Example:
access P2 4 fifo
Access a page using LRU:
access <process> <page> lru
Example:
access P2 4 lru
Display memory status:
memstatus
Free process memory:
free <process>
Example:
free P1
Clear memory simulation:
memclear
________________________________________
Demonstration Commands
FIFO page replacement demo:
fifo_demo
LRU page replacement demo:
lru_demo
Producer-Consumer synchronization demo:
sync_demo
________________________________________
Sample Workflow
meminit 3

alloc P1 1,2,3

access P2 4 fifo

memstatus

free P1

sync_demo
________________________________________
Concepts Demonstrated
Memory Management
•	Paging
•	Page Frames
•	Page Faults
•	FIFO Page Replacement
•	LRU Page Replacement
•	Memory Allocation
•	Memory Deallocation
Process Synchronization
•	Producer-Consumer Problem
•	Mutexes
•	Semaphores
•	Shared Resource Protection
•	Race Condition Prevention
________________________________________
Learning Outcomes
This project demonstrates how operating systems manage memory and coordinate concurrent processes. It provides hands-on experience with paging systems, page replacement algorithms, synchronization mechanisms, and resource management concepts.
________________________________________
Author
Kashif Ali Syed
MSCS-630 Advanced Operating Systems
University of the Cumberlands
