#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import os
from sos.utils import env
from rq import Queue as rqQueue
from redis import Redis
from sos.tasks import TaskEngine, execute_task, loadTask
from sos.eval import cfg_interpolate

class RQ_TaskEngine(TaskEngine):

    def __init__(self, agent):
        super(RQ_TaskEngine, self).__init__(agent)
        #
        self.redis_host = cfg_interpolate(self.config.get('redis_host', self.config.get('address', 'localhost')))
        self.redis_port = self.config.get('redis_port', 6379)
        self.redis_queue = cfg_interpolate(self.config.get('queue', 'default'))

        try:
            env.logger.debug('Connecting to redis server {} at port {}'.format(self.redis_host, self.redis_port))
            redis_conn = Redis(host=self.redis_host, port=self.redis_port)
        except Exception as e:
            raise RuntimeError('Failed to connect to redis server with host {} and port {}: {}'.format(
                self.redis_server, self.redis.port, e))

        self.redis_queue = rqQueue(self.redis_queue, connection=redis_conn)
        self.jobs = {}

    def execute_task(self, task_id):
        #
        if not super(RQ_TaskEngine, self).execute_task(task_id):
            return False

        task_file = os.path.join(os.path.expanduser('~'), '.sos', 'tasks', task_id + '.task')
        sos_dict = loadTask(task_file).sos_dict

        # however, these could be fixed in the job template and we do not need to have them all in the runtime
        runtime = self.config
        runtime.update({x:sos_dict['_runtime'][x] for x in ('walltime', 'cur_dir', 'home_dir', 'name') if x in sos_dict['_runtime']})
        runtime['task'] = task_id
        runtime['verbosity'] = env.verbosity
        runtime['sig_mode'] = env.config['sig_mode']
        runtime['run_mode'] = env.config['run_mode']
        if 'name' in runtime:
            runtime['job_name'] = cfg_interpolate(runtime['name'], sos_dict)
        else:
            runtime['job_name'] = cfg_interpolate('${step_name}_${_index}', sos_dict)
        if 'nodes' not in runtime:
            runtime['nodes'] = 1
        if 'cores' not in runtime:
            runtime['cores'] = 1
    
        # tell subprocess where pysos.runtime is
        self.jobs[task_id] = self.redis_queue.enqueue(
            execute_task, args=(task_id, env.verbosity, runtime['run_mode'],
                runtime['sig_mode'], 5, 60),
            job_id = runtime['job_name'],
            # result expire after one day
            result_ttl=86400,
            timeout=runtime.get('walltime', 86400*30))
        return True

