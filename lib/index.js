'use strict';

// A multiple task queues implementation
// Task is assigned with round robin algorithm.

// author: cpselvis (cpselvis@gmail.com)
// github: https://github.com/cpselvis

const cluster = require('cluster');
const numCPUs = require('os').cpus().length;


// Define message type
const WORKER_CREATED = 0;
const WORKER_FREE = 1;
const MASTER_ASSIGN_TASK = 2;
const NONE_TASK = 3;

const start = (tasks, taskHandler) => {
    if (cluster.isMaster) {

        console.log('Master ' + process.pid + ' has started.');

        // Fork workers.
        for (let i = 0; i < numCPUs; i++) {
            const worker = cluster.fork();

            // Receive messages from this worker and handle them in the master process.
            worker.on('message', function(msg) {
                console.log('Master ' + process.pid + ' received message from worker ' + this.pid + '.', msg);

                if (msg.msgType === WORKER_CREATED || msg.msgType === WORKER_FREE) {
                    // Send a message from the master process to the worker.
                    worker.send({pid: worker.pid, msgType: MASTER_ASSIGN_TASK,msg: 'Assign task to worker.', from: 'master'});
                } else if (msg.msgType === NONE_TASK) {
                    console.log('Task has all been finished!');
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
        process.send({pid: process.pid, msgType: WORKER_CREATED, msg: 'Worker has been created.', from: 'worker'});

        // Receive messages from the master process.
        process.on('message', function(msg) {
            if (msg.msgType === MASTER_ASSIGN_TASK) {

                console.log('Worker begin to handle task, worker process id:' + process.pid);
                // Get task from task queues and handle task.
                console.log('Task is finished by worker:' + process.pid);

                if (tasks.length) {
                    let taskId = tasks.shift();
                    console.log('Task result is:' + taskId);

                    taskHandler(taskId, () => {
                        process.send({pid: process.pid, msgType: WORKER_FREE, msg: 'Task' + taskId + ' has been handled.', from: 'worker'});
                    });

                } else {
                    process.send({pid: process.pid, msgType: NONE_TASK, msg: 'No task anymore.', from: 'worker'});
                }
            }
        });
    }
};

module.exports = {
    start: start
};