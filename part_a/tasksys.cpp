#include "tasksys.h"


IRunnable::~IRunnable() {}

ITaskSystem::ITaskSystem(int num_threads) {}
ITaskSystem::~ITaskSystem() {}

/*
 * ================================================================
 * Serial task system implementation
 * ================================================================
 */

const char* TaskSystemSerial::name() {
    return "Serial";
}

TaskSystemSerial::TaskSystemSerial(int num_threads): ITaskSystem(num_threads) {
}

TaskSystemSerial::~TaskSystemSerial() {}

void TaskSystemSerial::run(IRunnable* runnable, int num_total_tasks) {
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
}

TaskID TaskSystemSerial::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                          const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemSerial::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelSpawn::name() {
    return "Parallel + Always Spawn";
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    this->numThreads = num_threads;
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

void TaskSystemParallelSpawn::workerThreadStart(WorkerArgs* args) {
    for (int i = args->threadId; i < args->num_total_tasks; i += args->numThreads) {
        args->runnable->runTask(i, args->num_total_tasks);
    }
}

void TaskSystemParallelSpawn::run(IRunnable* runnable, int num_total_tasks) {
    
    std::thread workers[numThreads];
    WorkerArgs args[numThreads];
    for (int i = 0; i < numThreads; i++) {
        args[i].runnable = runnable;
        args[i].threadId = i;
        args[i].numThreads = numThreads;
        args[i].num_total_tasks = num_total_tasks;
    }

    for (int i = 1; i < numThreads; i++) {
        workers[i] = std::thread(workerThreadStart, &args[i]);
    }
    workerThreadStart(&args[0]);

    for (int i = 1; i < numThreads; i++) {
        workers[i].join();
    }
    /* Serial Implementation
     * 
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    */
}

TaskID TaskSystemParallelSpawn::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                 const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelSpawn::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Spinning Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSpinning::name() {
    return "Parallel + Thread Pool + Spin";
}

void TaskSystemParallelThreadPoolSpinning::workerThreadStart(int threadId) {
    int t;
    while (running) {
        if (threadId == 0 && num_done_tasks.load() == num_total_tasks) {
            // We're guaranteed that thread 0 only runs when num_total_tasks > 0
            return;
        }
        queue_lock->lock();
        if (task_queue.empty()) { 
            // don't remove if queue empty; 
            // can't move this outside lock because queue might become empty while waiting to acquire lock
            queue_lock->unlock();
            continue;
        }
        t = task_queue.front();
        task_queue.pop();
        queue_lock->unlock();

        runnable->runTask(t, num_total_tasks);

        num_done_tasks++;
    }
}

TaskSystemParallelThreadPoolSpinning::TaskSystemParallelThreadPoolSpinning(int num_threads): ITaskSystem(num_threads) {
    running = true;
    this->numThreads = num_threads;
    queue_lock = new std::mutex();
    workers = new std::thread[numThreads];
    
    for (int i = 1; i < numThreads; i++) {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSpinning::workerThreadStart, this, i);
    }
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning() {
    running = false; 
    for (int i = 1; i < numThreads; i++) {
        workers[i].join();
    }
    delete queue_lock;
    delete[] workers;
}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {
    this->num_total_tasks = num_total_tasks;
    this->num_done_tasks = 0;
    this->runnable = runnable;
    queue_lock->lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_queue.push(i);
    }
    queue_lock->unlock();

    workerThreadStart(0);

    /*  
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    */
}

TaskID TaskSystemParallelThreadPoolSpinning::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                              const std::vector<TaskID>& deps) {
    // You do not need to implement this method.
    return 0;
}

void TaskSystemParallelThreadPoolSpinning::sync() {
    // You do not need to implement this method.
    return;
}

/*
 * ================================================================
 * Parallel Thread Pool Sleeping Task System Implementation
 * ================================================================
 */

const char* TaskSystemParallelThreadPoolSleeping::name() {
    return "Parallel + Thread Pool + Sleep";
}

TaskSystemParallelThreadPoolSleeping::TaskSystemParallelThreadPoolSleeping(int num_threads): ITaskSystem(num_threads) {
    running = true;
    this->num_threads = num_threads;
    queue_lock = new std::mutex();
    counter_lock = new std::mutex();
    main_cv = new std::condition_variable();
    worker_cv = new std::condition_variable();
    workers = new std::thread[num_threads];

    Counter.completed = new bool[num_threads];    
    Counter.num_done_tasks = 0;
    Counter.num_total_tasks = 0;
    for (int i = 0; i < num_threads; i++) {
        Counter.completed[i] = true;
    }
    for (int i = 0; i < num_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::workerThreadStart, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    running = false;
    counter_lock->lock();
    for (int i = 0; i < num_threads; i++) {
        Counter.completed[i] = false;
    }
    counter_lock->unlock();
    worker_cv->notify_all();

    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
    delete queue_lock;
    delete counter_lock;
    delete main_cv;
    delete worker_cv;
    delete[] workers;
    delete[] Counter.completed;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int thread_id) {
    int t;
    while (running) {
        std::unique_lock<std::mutex> lk(*queue_lock);
        // while (Counter.completed[thread_id] && Counter.num_done_tasks == Counter.num_total_tasks) {
        // }
        // worker_cv->wait(lk);
        // printf("Thread %d; completed: %d; # done_tasks: %d; # total tasks: %d\n", thread_id, Counter.completed[thread_id], Counter.num_done_tasks, Counter.num_total_tasks);
        worker_cv->wait(lk, [&]{ 
                // counter_lock->lock();
                return !Counter.completed[thread_id] || Counter.num_done_tasks < Counter.num_total_tasks;
                // printf("Thread %d checking predicate; flag: %d\n", thread_id, flag);
                // counter_lock->unlock(); 
                // return flag;});
        // atomic variable here??
        // printf("Thread %d; unlocked!\n", thread_id);
        if (task_queue.empty()) {
            lk.unlock();
            if (!running) {
                return;
            }
        }
        else {
            // Retrieves task from queue
            // TODO: Optimization from using custom class instead of std::queue
            t = task_queue.front();
            task_queue.pop();
            lk.unlock();

            // Notifies another thread to proceed
            worker_cv->notify_one();
            // printf("Thread %d; releases queue_lock; claims task %d\n", thread_id, t);
            runnable->runTask(t, Counter.num_total_tasks);
            // TODO: Optimization from using atomic variable for num_done_tasks?
            counter_lock->lock();
            Counter.num_done_tasks++;
            counter_lock->unlock();
            // printf("Task %d done!\n", t);
        }
        counter_lock->lock();
        if (Counter.num_done_tasks == Counter.num_total_tasks) {
            Counter.completed[thread_id] = true;
            Counter.num_completed_threads++; 
            counter_lock->unlock();
            if (Counter.num_completed_threads == num_threads) {
                main_cv->notify_all();
            }
            // printf("Thread %d done! # done threads: %d\n", thread_id, Counter.num_completed_threads);
            /*
            if (thread_id == 0) {
                return;
            }
            */
        }
        else {
            counter_lock->unlock();
        }
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    // printf("NEW RUN\n\n");
    this->runnable = runnable;
    queue_lock->lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_queue.push(i);
    }
    queue_lock->unlock();

    counter_lock->lock();
    Counter.num_total_tasks = num_total_tasks;
    Counter.num_done_tasks = 0;
    Counter.num_completed_threads = 0;
    for (int i = 0; i < num_threads; i++) {
        Counter.completed[i] = false;
    }
    counter_lock->unlock();
    
    // workerThreadStart(0);

    std::unique_lock<std::mutex> lk(*counter_lock);
    worker_cv->notify_all();
    
    /*
    while (Counter.num_completed_threads != num_threads - 1) {
        worker_cv->notify_all();
    }
    */
    // main_cv->wait(lk, [&]{ return Counter.num_completed_threads == num_threads; });
    main_cv->wait(lk);
    // printf("DONE\n");
    lk.unlock();

    /*
    for (int i = 0; i < num_total_tasks; i++) {
        runnable->runTask(i, num_total_tasks);
    }
    */
}

TaskID TaskSystemParallelThreadPoolSleeping::runAsyncWithDeps(IRunnable* runnable, int num_total_tasks,
                                                    const std::vector<TaskID>& deps) {


    //
    // TODO: CS149 students will implement this method in Part B.
    //

    return 0;
}

void TaskSystemParallelThreadPoolSleeping::sync() {

    //
    // TODO: CS149 students will modify the implementation of this method in Part B.
    //

    return;
}
