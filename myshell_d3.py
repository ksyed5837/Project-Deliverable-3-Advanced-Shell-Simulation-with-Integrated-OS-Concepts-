#!/usr/bin/env python3
"""
Project Deliverable 3: Advanced Shell Simulation with Memory Management and Synchronization
Builds on Deliverable 2 shell and adds:
  - Paging memory simulation
  - FIFO and LRU page replacement
  - Page fault tracking
  - Memory allocation/deallocation
  - Producer-Consumer synchronization demo using semaphores and a mutex

Run: python myshell_d3.py
"""

import heapq
import os
import shlex
import shutil
import signal
import subprocess
import threading
import time
from collections import deque, OrderedDict
from dataclasses import dataclass, field
from typing import Deque, Dict, List, Optional, Tuple

SIM_SLEEP = 0.10


@dataclass
class Job:
    job_id: int
    process: subprocess.Popen
    command: str
    status: str = "Running"


@dataclass
class SimProcess:
    pid: int
    name: str
    burst_time: int
    priority: int
    arrival_time: int = 0
    remaining_time: int = field(init=False)
    start_time: Optional[int] = None
    completion_time: Optional[int] = None

    def __post_init__(self) -> None:
        self.remaining_time = self.burst_time

    def clone(self) -> "SimProcess":
        return SimProcess(self.pid, self.name, self.burst_time, self.priority, self.arrival_time)


@dataclass
class PageFrame:
    frame_id: int
    process_name: str
    page_id: int
    loaded_at: int
    last_used: int

    def label(self) -> str:
        return f"{self.process_name}:Page{self.page_id}"


class MiniShell:
    def __init__(self) -> None:
        self.jobs: Dict[int, Job] = {}
        self.next_job_id = 1
        self.running = True

        # Deliverable 2 scheduling state
        self.sim_processes: List[SimProcess] = []
        self.next_sim_pid = 1

        # Deliverable 3 memory state
        self.frame_count = 4
        self.frames: List[Optional[PageFrame]] = [None] * self.frame_count
        self.process_pages: Dict[str, set] = {}
        self.clock = 0
        self.page_faults = 0
        self.page_hits = 0

    def prompt(self) -> str:
        return f"myshell-d3:{os.getcwd()}$ "

    def cleanup_jobs(self) -> None:
        for job in list(self.jobs.values()):
            code = job.process.poll()
            if code is not None and job.status == "Running":
                job.status = f"Done ({code})"

    def run(self) -> None:
        print("Welcome to MyShell Deliverable 3. Type 'help' for commands or 'exit' to quit.")
        while self.running:
            self.cleanup_jobs()
            try:
                line = input(self.prompt()).strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not line:
                continue
            self.execute_line(line)

    def execute_line(self, line: str) -> None:
        background = False
        if line.endswith("&"):
            background = True
            line = line[:-1].strip()
        try:
            args = shlex.split(line)
        except ValueError as exc:
            print(f"Error: {exc}")
            return
        if not args:
            return
        command = args[0]
        builtins = {
            "help": self.cmd_help,
            "cd": self.cmd_cd,
            "pwd": self.cmd_pwd,
            "exit": self.cmd_exit,
            "echo": self.cmd_echo,
            "clear": self.cmd_clear,
            "ls": self.cmd_ls,
            "cat": self.cmd_cat,
            "mkdir": self.cmd_mkdir,
            "rmdir": self.cmd_rmdir,
            "rm": self.cmd_rm,
            "touch": self.cmd_touch,
            "kill": self.cmd_kill,
            "jobs": self.cmd_jobs,
            "fg": self.cmd_fg,
            "bg": self.cmd_bg,
            "addproc": self.cmd_addproc,
            "listproc": self.cmd_listproc,
            "clearproc": self.cmd_clearproc,
            "rr": self.cmd_round_robin,
            "priority": self.cmd_priority,
            "sched_demo": self.cmd_sched_demo,
            # Deliverable 3 commands
            "meminit": self.cmd_meminit,
            "alloc": self.cmd_alloc,
            "access": self.cmd_access,
            "memstatus": self.cmd_memstatus,
            "free": self.cmd_free,
            "memclear": self.cmd_memclear,
            "fifo_demo": self.cmd_fifo_demo,
            "lru_demo": self.cmd_lru_demo,
            "sync_demo": self.cmd_sync_demo,
        }
        if command in builtins:
            if background:
                print("Error: built-in commands are handled directly and should not be run with &.")
                return
            builtins[command](args[1:])
        else:
            self.run_external(args, line, background)

    def cmd_help(self, args: List[str]) -> None:
        print("Built-in commands: cd, pwd, exit, echo, clear, ls, cat, mkdir, rmdir, rm, touch, kill")
        print("Job control: command &, jobs, fg <job_id>, bg <job_id>")
        print("Scheduling simulation: addproc, listproc, clearproc, rr <quantum>, priority, sched_demo")
        print("Memory management simulation:")
        print("  meminit <frames>                  initialize physical memory frame count")
        print("  alloc <process> <page1,page2,...> allocate pages to a process")
        print("  access <process> <page> <fifo|lru> access a page using FIFO or LRU replacement")
        print("  memstatus                         show frames, memory usage, page faults")
        print("  free <process>                    deallocate all pages for a process")
        print("  memclear                          reset memory simulation")
        print("  fifo_demo                         run a FIFO page replacement demo")
        print("  lru_demo                          run an LRU page replacement demo")
        print("Synchronization simulation:")
        print("  sync_demo                         run Producer-Consumer demo using semaphores and mutex")

    # Deliverable 1 commands
    def cmd_cd(self, args: List[str]) -> None:
        target = args[0] if args else os.path.expanduser("~")
        try:
            os.chdir(os.path.expanduser(target))
        except FileNotFoundError:
            print(f"cd: no such directory: {target}")
        except NotADirectoryError:
            print(f"cd: not a directory: {target}")
        except PermissionError:
            print(f"cd: permission denied: {target}")

    def cmd_pwd(self, args: List[str]) -> None:
        print(os.getcwd())

    def cmd_exit(self, args: List[str]) -> None:
        print("Exiting MyShell.")
        self.running = False

    def cmd_echo(self, args: List[str]) -> None:
        print(" ".join(args))

    def cmd_clear(self, args: List[str]) -> None:
        os.system("cls" if os.name == "nt" else "clear")

    def cmd_ls(self, args: List[str]) -> None:
        path = args[0] if args else "."
        try:
            for name in sorted(os.listdir(path)):
                print(name)
        except FileNotFoundError:
            print(f"ls: cannot access '{path}': No such file or directory")
        except NotADirectoryError:
            print(path)
        except PermissionError:
            print(f"ls: permission denied: {path}")

    def cmd_cat(self, args: List[str]) -> None:
        if not args:
            print("cat: missing file operand")
            return
        for filename in args:
            try:
                with open(filename, "r", encoding="utf-8") as file:
                    print(file.read(), end="")
            except FileNotFoundError:
                print(f"cat: {filename}: No such file or directory")
            except IsADirectoryError:
                print(f"cat: {filename}: Is a directory")
            except PermissionError:
                print(f"cat: {filename}: Permission denied")

    def cmd_mkdir(self, args: List[str]) -> None:
        if not args:
            print("mkdir: missing directory name")
            return
        for dirname in args:
            try:
                os.mkdir(dirname)
                print(f"Directory created: {dirname}")
            except FileExistsError:
                print(f"mkdir: cannot create directory '{dirname}': File exists")
            except PermissionError:
                print(f"mkdir: permission denied: {dirname}")

    def cmd_rmdir(self, args: List[str]) -> None:
        if not args:
            print("rmdir: missing directory name")
            return
        for dirname in args:
            try:
                os.rmdir(dirname)
                print(f"Directory removed: {dirname}")
            except FileNotFoundError:
                print(f"rmdir: failed to remove '{dirname}': No such file or directory")
            except OSError:
                print(f"rmdir: failed to remove '{dirname}': Directory not empty or invalid")

    def cmd_rm(self, args: List[str]) -> None:
        if not args:
            print("rm: missing file name")
            return
        for filename in args:
            try:
                os.remove(filename)
                print(f"File removed: {filename}")
            except FileNotFoundError:
                print(f"rm: cannot remove '{filename}': No such file or directory")
            except IsADirectoryError:
                print(f"rm: cannot remove '{filename}': Is a directory")
            except PermissionError:
                print(f"rm: permission denied: {filename}")

    def cmd_touch(self, args: List[str]) -> None:
        if not args:
            print("touch: missing file name")
            return
        for filename in args:
            try:
                with open(filename, "a", encoding="utf-8"):
                    os.utime(filename, None)
                print(f"Touched file: {filename}")
            except PermissionError:
                print(f"touch: permission denied: {filename}")

    def cmd_kill(self, args: List[str]) -> None:
        if not args:
            print("kill: missing pid")
            return
        try:
            pid = int(args[0])
            os.kill(pid, signal.SIGTERM)
            print(f"Sent termination signal to process {pid}")
        except ValueError:
            print("kill: pid must be a number")
        except ProcessLookupError:
            print(f"kill: process not found: {args[0]}")
        except PermissionError:
            print(f"kill: permission denied: {args[0]}")
        except AttributeError:
            print("kill: not supported on this system")

    def cmd_jobs(self, args: List[str]) -> None:
        self.cleanup_jobs()
        if not self.jobs:
            print("No background jobs.")
            return
        for job in self.jobs.values():
            print(f"[{job.job_id}] PID={job.process.pid} {job.status} - {job.command}")

    def cmd_fg(self, args: List[str]) -> None:
        if not args:
            print("fg: missing job id")
            return
        job = self.get_job(args[0], "fg")
        if not job:
            return
        print(f"Bringing job [{job.job_id}] to foreground: {job.command}")
        try:
            job.process.wait()
            job.status = "Done"
            del self.jobs[job.job_id]
        except KeyboardInterrupt:
            print("Foreground job interrupted.")

    def cmd_bg(self, args: List[str]) -> None:
        if not args:
            print("bg: missing job id")
            return
        job = self.get_job(args[0], "bg")
        if not job:
            return
        if job.process.poll() is None:
            job.status = "Running"
            print(f"Job [{job.job_id}] is running in background: {job.command}")
        else:
            print(f"Job [{job.job_id}] has already completed.")

    def get_job(self, value: str, command_name: str) -> Optional[Job]:
        try:
            job_id = int(value)
        except ValueError:
            print(f"{command_name}: job id must be a number")
            return None
        job = self.jobs.get(job_id)
        if not job:
            print(f"{command_name}: no such job: {job_id}")
            return None
        return job

    def run_external(self, args: List[str], command_line: str, background: bool) -> None:
        if shutil.which(args[0]) is None:
            print(f"Error: command not found: {args[0]}")
            return
        try:
            process = subprocess.Popen(args)
            if background:
                job_id = self.next_job_id
                self.next_job_id += 1
                self.jobs[job_id] = Job(job_id, process, command_line)
                print(f"Started background job [{job_id}] PID={process.pid}: {command_line}")
            else:
                process.wait()
        except Exception as exc:
            print(f"Error running command: {exc}")

    # Deliverable 2 commands
    def cmd_addproc(self, args: List[str]) -> None:
        if len(args) not in (3, 4):
            print("Usage: addproc <name> <burst_time> <priority> [arrival_time]")
            return
        try:
            burst = int(args[1]); priority = int(args[2]); arrival = int(args[3]) if len(args) == 4 else 0
        except ValueError:
            print("Error: burst_time, priority, and arrival_time must be integers")
            return
        if burst <= 0 or arrival < 0:
            print("Error: burst_time must be > 0 and arrival_time cannot be negative")
            return
        p = SimProcess(self.next_sim_pid, args[0], burst, priority, arrival)
        self.next_sim_pid += 1
        self.sim_processes.append(p)
        print(f"Added process PID={p.pid}, name={p.name}, burst={p.burst_time}, priority={p.priority}, arrival={p.arrival_time}")

    def cmd_listproc(self, args: List[str]) -> None:
        if not self.sim_processes:
            print("No simulated processes. Use addproc or sched_demo first.")
            return
        print("PID  Name       Burst  Priority  Arrival")
        print("---  ---------  -----  --------  -------")
        for p in self.sim_processes:
            print(f"{p.pid:<3}  {p.name:<9}  {p.burst_time:<5}  {p.priority:<8}  {p.arrival_time:<7}")

    def cmd_clearproc(self, args: List[str]) -> None:
        self.sim_processes.clear(); self.next_sim_pid = 1
        print("Cleared all simulated processes.")

    def cmd_sched_demo(self, args: List[str]) -> None:
        self.sim_processes.clear(); self.next_sim_pid = 1
        for name, burst, priority, arrival in [("P1",6,3,0),("P2",4,1,1),("P3",5,2,2),("P4",3,1,4)]:
            self.sim_processes.append(SimProcess(self.next_sim_pid, name, burst, priority, arrival)); self.next_sim_pid += 1
        print("Loaded demo processes:"); self.cmd_listproc([])

    def cmd_round_robin(self, args: List[str]) -> None:
        if not self.sim_processes:
            print("No simulated processes. Add processes using addproc or run sched_demo."); return
        if len(args) != 1:
            print("Usage: rr <time_quantum>"); return
        try:
            quantum = int(args[0])
        except ValueError:
            print("Error: time quantum must be an integer"); return
        if quantum <= 0:
            print("Error: time quantum must be greater than 0"); return
        self.run_round_robin(quantum)

    def cmd_priority(self, args: List[str]) -> None:
        if not self.sim_processes:
            print("No simulated processes. Add processes using addproc or run sched_demo."); return
        self.run_priority_scheduler()

    def run_round_robin(self, quantum: int) -> None:
        processes = sorted([p.clone() for p in self.sim_processes], key=lambda p:(p.arrival_time,p.pid))
        time_now = 0; index = 0; ready: Deque[SimProcess] = deque(); completed: List[SimProcess] = []
        print("\n=== Round-Robin Scheduling ==="); print(f"Configured time quantum: {quantum}")
        while index < len(processes) or ready:
            while index < len(processes) and processes[index].arrival_time <= time_now:
                ready.append(processes[index]); print(f"Time {time_now}: {processes[index].name} arrived and entered ready queue"); index += 1
            if not ready:
                time_now = processes[index].arrival_time; continue
            p = ready.popleft();
            if p.start_time is None: p.start_time = time_now
            run_time = min(quantum, p.remaining_time)
            print(f"Time {time_now}: Running {p.name} for {run_time} unit(s); remaining before run={p.remaining_time}")
            time.sleep(SIM_SLEEP); time_now += run_time; p.remaining_time -= run_time
            while index < len(processes) and processes[index].arrival_time <= time_now:
                ready.append(processes[index]); print(f"Time {time_now}: {processes[index].name} arrived and entered ready queue"); index += 1
            if p.remaining_time == 0:
                p.completion_time = time_now; completed.append(p); print(f"Time {time_now}: {p.name} completed")
            else:
                ready.append(p); print(f"Time {time_now}: {p.name} time slice expired; moved to back of queue")
        self.print_metrics(completed)

    def run_priority_scheduler(self) -> None:
        processes = sorted([p.clone() for p in self.sim_processes], key=lambda p:(p.arrival_time,p.pid))
        time_now = 0; index = 0; ready_heap: List[Tuple[int,int,SimProcess]] = []; completed: List[SimProcess] = []; current = None
        print("\n=== Priority-Based Scheduling ==="); print("Lower priority number means higher priority. Same priority uses FCFS order.")
        while index < len(processes) or ready_heap or current:
            while index < len(processes) and processes[index].arrival_time <= time_now:
                p = processes[index]; heapq.heappush(ready_heap,(p.priority,p.arrival_time,p)); print(f"Time {time_now}: {p.name} arrived with priority {p.priority}")
                if current and p.priority < current.priority:
                    print(f"Time {time_now}: Preemption - {p.name} has higher priority than {current.name}"); heapq.heappush(ready_heap,(current.priority,current.arrival_time,current)); current = None
                index += 1
            if current is None:
                if ready_heap:
                    _,_,current = heapq.heappop(ready_heap)
                    if current.start_time is None: current.start_time = time_now
                    print(f"Time {time_now}: Running {current.name} with priority {current.priority}")
                else:
                    if index < len(processes): time_now = processes[index].arrival_time
                    continue
            time.sleep(SIM_SLEEP); current.remaining_time -= 1; time_now += 1
            if current.remaining_time == 0:
                current.completion_time = time_now; print(f"Time {time_now}: {current.name} completed"); completed.append(current); current = None
        self.print_metrics(completed)

    def print_metrics(self, completed: List[SimProcess]) -> None:
        completed.sort(key=lambda p:p.pid)
        print("\nPerformance Metrics")
        print("PID  Name       Waiting  Turnaround  Response")
        print("---  ---------  -------  ----------  --------")
        tw=tt=tr=0
        for p in completed:
            turnaround=(p.completion_time or 0)-p.arrival_time; waiting=turnaround-p.burst_time; response=(p.start_time or 0)-p.arrival_time
            tw+=waiting; tt+=turnaround; tr+=response
            print(f"{p.pid:<3}  {p.name:<9}  {waiting:<7}  {turnaround:<10}  {response:<8}")
        if completed:
            n=len(completed); print("Averages:"); print(f"  Average waiting time: {tw/n:.2f}"); print(f"  Average turnaround time: {tt/n:.2f}"); print(f"  Average response time: {tr/n:.2f}")
        print()

    # Deliverable 3 Memory Management
    def cmd_meminit(self, args: List[str]) -> None:
        if len(args) != 1:
            print("Usage: meminit <number_of_frames>"); return
        try:
            frames = int(args[0])
        except ValueError:
            print("Error: number_of_frames must be an integer"); return
        if frames <= 0:
            print("Error: number_of_frames must be greater than 0"); return
        self.frame_count = frames
        self.frames = [None] * frames
        self.process_pages.clear()
        self.clock = self.page_faults = self.page_hits = 0
        print(f"Memory initialized with {frames} physical page frames.")

    def cmd_memclear(self, args: List[str]) -> None:
        self.frames = [None] * self.frame_count
        self.process_pages.clear()
        self.clock = self.page_faults = self.page_hits = 0
        print("Memory simulation cleared. Frames are empty and counters reset.")

    def parse_pages(self, pages_arg: str) -> Optional[List[int]]:
        try:
            pages = [int(x.strip()) for x in pages_arg.split(',') if x.strip() != '']
        except ValueError:
            print("Error: pages must be comma-separated integers, for example 1,2,3")
            return None
        if not pages:
            print("Error: at least one page must be provided")
            return None
        if any(p < 0 for p in pages):
            print("Error: page numbers cannot be negative")
            return None
        return pages

    def cmd_alloc(self, args: List[str]) -> None:
        if len(args) != 2:
            print("Usage: alloc <process> <page1,page2,...>"); return
        process = args[0]
        pages = self.parse_pages(args[1])
        if pages is None: return
        print(f"Allocating pages {pages} for process {process} using FIFO replacement when memory is full.")
        for page in pages:
            self.load_or_access_page(process, page, "fifo")
        self.cmd_memstatus([])

    def cmd_access(self, args: List[str]) -> None:
        if len(args) != 3:
            print("Usage: access <process> <page> <fifo|lru>"); return
        process = args[0]
        try:
            page = int(args[1])
        except ValueError:
            print("Error: page must be an integer"); return
        algo = args[2].lower()
        if algo not in ("fifo", "lru"):
            print("Error: replacement algorithm must be fifo or lru"); return
        if page < 0:
            print("Error: page cannot be negative"); return
        self.load_or_access_page(process, page, algo)
        self.cmd_memstatus([])

    def find_page(self, process: str, page: int) -> Optional[int]:
        for i, frame in enumerate(self.frames):
            if frame and frame.process_name == process and frame.page_id == page:
                return i
        return None

    def load_or_access_page(self, process: str, page: int, algorithm: str) -> None:
        self.clock += 1
        existing = self.find_page(process, page)
        if existing is not None:
            frame = self.frames[existing]
            assert frame is not None
            frame.last_used = self.clock
            self.page_hits += 1
            print(f"PAGE HIT: {process}:Page{page} is already in Frame {existing}. Updated last-used time.")
            return
        self.page_faults += 1
        print(f"PAGE FAULT: {process}:Page{page} is not in memory.")
        free_index = next((i for i, frame in enumerate(self.frames) if frame is None), None)
        new_frame = PageFrame(free_index if free_index is not None else -1, process, page, self.clock, self.clock)
        if free_index is not None:
            new_frame.frame_id = free_index
            self.frames[free_index] = new_frame
            self.process_pages.setdefault(process, set()).add(page)
            print(f"Loaded {process}:Page{page} into free Frame {free_index}.")
            return
        victim_index = self.select_victim(algorithm)
        victim = self.frames[victim_index]
        assert victim is not None
        print(f"Memory full. {algorithm.upper()} replacement selected Frame {victim_index} containing {victim.label()}.")
        if victim.process_name in self.process_pages:
            self.process_pages[victim.process_name].discard(victim.page_id)
        new_frame.frame_id = victim_index
        self.frames[victim_index] = new_frame
        self.process_pages.setdefault(process, set()).add(page)
        print(f"Replaced {victim.label()} with {process}:Page{page} in Frame {victim_index}.")

    def select_victim(self, algorithm: str) -> int:
        if algorithm == "fifo":
            return min(range(len(self.frames)), key=lambda i: self.frames[i].loaded_at if self.frames[i] else 10**9)
        return min(range(len(self.frames)), key=lambda i: self.frames[i].last_used if self.frames[i] else 10**9)

    def cmd_memstatus(self, args: List[str]) -> None:
        print("\n=== Memory Status ===")
        print(f"Total frames: {self.frame_count}")
        used = sum(1 for f in self.frames if f is not None)
        print(f"Used frames: {used}/{self.frame_count}")
        print("Frame  Contents       LoadedAt  LastUsed")
        print("-----  -------------  --------  --------")
        for i, frame in enumerate(self.frames):
            if frame is None:
                print(f"{i:<5}  EMPTY")
            else:
                print(f"{i:<5}  {frame.label():<13}  {frame.loaded_at:<8}  {frame.last_used:<8}")
        print(f"Page hits: {self.page_hits}")
        print(f"Page faults: {self.page_faults}")
        print("Process memory usage:")
        if not self.process_pages:
            print("  No process pages are loaded.")
        else:
            for proc, pages in sorted(self.process_pages.items()):
                loaded_pages = sorted(pages)
                print(f"  {proc}: {len(loaded_pages)} page(s) loaded -> {loaded_pages}")
        print()

    def cmd_free(self, args: List[str]) -> None:
        if len(args) != 1:
            print("Usage: free <process>"); return
        process = args[0]
        removed = 0
        for i, frame in enumerate(self.frames):
            if frame and frame.process_name == process:
                print(f"Deallocating {frame.label()} from Frame {i}.")
                self.frames[i] = None
                removed += 1
        self.process_pages.pop(process, None)
        if removed == 0:
            print(f"No pages found for process {process}.")
        else:
            print(f"Freed {removed} frame(s) for process {process}.")
        self.cmd_memstatus([])

    def cmd_fifo_demo(self, args: List[str]) -> None:
        print("\n=== FIFO Page Replacement Demo ===")
        self.cmd_meminit(["3"])
        for page in [1, 2, 3, 4, 1, 5]:
            print(f"\nAccessing App:Page{page} using FIFO")
            self.load_or_access_page("App", page, "fifo")
        self.cmd_memstatus([])

    def cmd_lru_demo(self, args: List[str]) -> None:
        print("\n=== LRU Page Replacement Demo ===")
        self.cmd_meminit(["3"])
        for page in [1, 2, 3, 1, 4, 2, 5]:
            print(f"\nAccessing App:Page{page} using LRU")
            self.load_or_access_page("App", page, "lru")
        self.cmd_memstatus([])

    # Deliverable 3 Synchronization
    def cmd_sync_demo(self, args: List[str]) -> None:
        print("\n=== Producer-Consumer Synchronization Demo ===")
        print("Using a mutex plus empty/full semaphores to protect a shared buffer.")
        buffer: Deque[str] = deque()
        max_size = 3
        mutex = threading.Lock()
        empty = threading.Semaphore(max_size)
        full = threading.Semaphore(0)

        def producer() -> None:
            for i in range(1, 6):
                item = f"item-{i}"
                print(f"Producer wants to add {item}.")
                empty.acquire()
                with mutex:
                    buffer.append(item)
                    print(f"Producer added {item}. Buffer = {list(buffer)}")
                full.release()
                time.sleep(0.08)

        def consumer() -> None:
            for _ in range(1, 6):
                print("Consumer wants to remove an item.")
                full.acquire()
                with mutex:
                    item = buffer.popleft()
                    print(f"Consumer removed {item}. Buffer = {list(buffer)}")
                empty.release()
                time.sleep(0.12)

        producer_thread = threading.Thread(target=producer, name="Producer")
        consumer_thread = threading.Thread(target=consumer, name="Consumer")
        producer_thread.start(); consumer_thread.start()
        producer_thread.join(); consumer_thread.join()
        print("Synchronization demo complete. Buffer access was protected, so no race condition occurred.")


if __name__ == "__main__":
    MiniShell().run()
