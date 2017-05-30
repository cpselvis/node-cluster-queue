'use strict';

// A multiple task queues implementation
// Task is assigned with round robin algorithm.

// author: cpselvis (cpselvis@gmail.com)
// github: https://github.com/cpselvis

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

// Define worker status
const WORKER_STATUS = {
    STARTED: 'started',
    SUSPENDED: 'suspended',
    BUSY: 'busy',
    IDLE: 'idle',
    DIED: 'died'
};

// Define message type
const MASTER_ASSIGN_TASK = 1;

/**
 * Execute jobs.
 *
 * @param jobQueue.   {Array}
 * @param jobHandler  {Function}
 */
const executeJob = (jobQueue, jobHandler) => {

    if (cluster.isMaster) {

        // console.log('Master ' + process.pid + ' has started.');

        // Fork workers.
        for ( let i = 0; i < numCPUs; i ++ ) {
            const worker = cluster.fork();

            // Receive messages from this worker and handle them in the master process.
            worker.on('message', function(msg) {
                // console.log('Master ' + process.pid + ' received message from worker ' + this.pid + '.', msg);

                if (msg.workerStatus === WORKER_STATUS.STARTED || msg.workerStatus === WORKER_STATUS.IDLE) {

                    if (jobQueue.length > 0) {
                        // Send a message from the master process to the worker.
                        worker.send({pid: worker.pid, msgType: MASTER_ASSIGN_TASK, msg: 'Assign task to worker.', from: 'master', jobId: jobQueue.shift()});
                    } else {
                        console.log('Congratulations, job has all been handled.');
                    }
                }
            });
        }

        // Be notified when worker processes die.
        cluster.on('death', function(worker) {
            console.log('Worker ' + worker.pid + ' died.');
        });

    } else {
        console.log('Worker ' + process.pid + ' has started.');

        // Send message to master process.
        process.send({pid: process.pid, workerStatus: WORKER_STATUS.STARTED, msg: 'Worker has been created.', from: 'worker'});

        // Receive messages from the master process.
        process.on('message', function(msg) {
            if (msg.msgType === MASTER_ASSIGN_TASK) {

                console.log('Worker begin to handle task, worker process id:' + process.pid);
                // Get task from task queues and handle task.
                console.log('Task is finished by worker:' + process.pid);

                let jobId = msg.jobId;

                if (jobId) {
                    console.log('Job is executing, job id is:' + jobId);

                    jobHandler(jobId, () => {
                        process.send({pid: process.pid, workerStatus: WORKER_STATUS.IDLE, msg: 'Task' + jobId + ' has been handled.', from: 'worker'});
                    });
                }
            }
        });
    }

};



module.exports = {
    executeJob: executeJob
};
