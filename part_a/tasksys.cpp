#include "tasksys.h"
#include <vector>
#include <thread>
#include <cmath>
#include <queue>
#include <mutex>

using namespace std;
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


void TaskSystemParallelSpawn::workerThreadStart(WorkerArgs* args) {
    for (int i = args->threadId; i < args->num_total_tasks; i += args->numThreads) {
        args->runnable->runTask(i, args->num_total_tasks);
    }
}

TaskSystemParallelSpawn::TaskSystemParallelSpawn(int num_threads): ITaskSystem(num_threads) {
    numThreads = num_threads;
	//
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelSpawn::~TaskSystemParallelSpawn() {}

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
    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSpinning::~TaskSystemParallelThreadPoolSpinning()
{

    running = false;
    for (int i = 1; i < numThreads; i++) {
        workers[i].join();
    }
    delete queue_lock;
    delete[] workers;

}

void TaskSystemParallelThreadPoolSpinning::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Part A.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->num_total_tasks = num_total_tasks;
    this->num_done_tasks = 0;
    this->runnable = runnable;
    queue_lock->lock();
    for (int i = 0; i < num_total_tasks; i++) {
        task_queue.push(i);
    }
    queue_lock->unlock();

    workerThreadStart(0);

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
    this->numThreads = num_threads;
    num_done_tasks = 0;

for (int i = 0; i < num_threads; ++i) {
    workers.emplace_back(&TaskSystemParallelThreadPoolSleeping::workerThreadStart, this);
}

    //
    // TODO: CS149 student implementations may decide to perform setup
    // operations (such as thread pool construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

TaskSystemParallelThreadPoolSleeping::~TaskSystemParallelThreadPoolSleeping() {

    running = false;
    worker_cv.notify_all();

    for (int i = 0; i < numThreads; i++) {
        workers[i].join();
    }

	//
    // TODO: CS149 student implementations may decide to perform cleanup
    // operations (such as thread pool shutdown construction) here.
    // Implementations are free to add new class member variables
    // (requiring changes to tasksys.h).
    //
}

void TaskSystemParallelThreadPoolSleeping::workerThreadStart() {

	
	while (running) {
	    int t = -1;

	    {
        	std::unique_lock<std::mutex> lk(queue_lock);
        	worker_cv.wait(lk, [this]{
                               return !task_queue.empty() || !running;
		       	});

            	if (!running && task_queue.empty()) {
                return;
            	}

            if (!task_queue.empty()) {
                t = task_queue.front();
                task_queue.pop();
            }
        }

        if (t != -1) {

            runnable->runTask(t, num_total_tasks);

            if (++num_done_tasks == num_total_tasks) {
                std::lock_guard<std::mutex> lock(calling_lock);
                main_cv.notify_one();
            }
        }

    }
}

void TaskSystemParallelThreadPoolSleeping::run(IRunnable* runnable, int num_total_tasks) {


    //
    // TODO: CS149 students will modify the implementation of this
    // method in Parts A and B.  The implementation provided below runs all
    // tasks sequentially on the calling thread.
    //
    this->runnable = runnable;
    this->num_total_tasks = num_total_tasks;
    num_done_tasks = 0;

    {
        std::lock_guard<std::mutex> lock(queue_lock);
        for (int i = 0; i < num_total_tasks; ++i) {
            task_queue.push(i);
        }
    }

    worker_cv.notify_all();

    std::unique_lock<std::mutex> lock(calling_lock);
    main_cv.wait(lock, [this, num_total_tasks] 
    { 
        return num_done_tasks == num_total_tasks; 
    });


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
