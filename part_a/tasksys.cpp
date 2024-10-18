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
    ready = false;
    this->num_threads = num_threads;
    queue_lock = new std::mutex();
    worker_lock = new std::mutex();
    worker_condition = new std::condition_variable();
    workers = new std::thread[num_threads];

    for (int i = 0; i < num_threads; i++) {
        workers[i] = std::thread(&TaskSystemParallelThreadPoolSleeping::workerThreadStart, this, i);
    }
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {
    running = false;
    ready = true;
    num_total_tasks = 0;
    worker_condition->notify_all();
    for (int i = 0; i < num_threads; i++) {
        workers[i].join();
    }
    delete queue_lock;
    delete worker_lock;
    delete worker_condition;
    delete[] workers;
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart(int thread_id) {
    int t;
    while (1) {
        std::unique_lock<std::mutex> lk(*worker_lock); // worker_lock protects num_awaken_threads
        worker_condition->wait(lk, [&]{ return ready.load(); }); // waits until main thread is ready and notifies
        num_awaken_threads++;
        printf("Thread %d awaken; # awaken threads: %d\n", thread_id, num_awaken_threads);
        lk.unlock();
        while (num_done_tasks.load() < num_total_tasks || num_awaken_threads < num_threads) {
            // need to check num_awaken_threads to prevent race condition
            // where all work is done before all threads are awaken
            queue_lock->lock();
            if (task_queue.empty()) { // waiting on other threads to be awaken or tasks to finish
                queue_lock->unlock();
                continue; 
            }
            t = task_queue.front();
            task_queue.pop();
            queue_lock->unlock();

            runnable->runTask(t, num_total_tasks);

            num_done_tasks++;
            printf("Thread %d finished task %d; # done tasks: %d; # total tasks: %d\n", thread_id, t, num_done_tasks.load(), num_total_tasks);
        }
        ready = false;
    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {
    printf("NEW RUN; %d\n\n", ready.load());
    this->num_total_tasks = num_total_tasks;
    worker_lock->lock();
    this->num_awaken_threads = 0; // TODO: lock necessary here?
    worker_lock->unlock();
    this->num_done_tasks = 0;
    this->runnable = runnable;
    queue_lock->lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_queue.push(i);
    }
    queue_lock->unlock();

    ready = true;
    worker_condition->notify_all();
    while (num_done_tasks.load() < num_total_tasks || num_awaken_threads < num_threads) {
    }
    printf("DONE\n");
    // getchar();
    // TODO: add another condition variable to let main go to sleep
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
