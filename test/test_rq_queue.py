#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

from sos.parser import SoS_Script
from sos.workflow_executor import Base_Executor
from sos.targets import file_target

import os
import unittest
import subprocess


has_docker = True
try:
    subprocess.check_output('docker ps | grep test_sos', shell=True).decode()
except subprocess.CalledProcessError:
    subprocess.call('sh ../build_test_docker.sh', shell=True)
    try:
        subprocess.check_output('docker ps | grep test_sos', shell=True).decode()
    except subprocess.CalledProcessError:
        print('Failed to set up a docker machine with sos')
        has_docker = False

class TestRQQueue(unittest.TestCase):

    def setUp(self):
        # set up rq worker and redis server
        self.rs = subprocess.Popen('redis-server')
        self.rw = subprocess.Popen(['rq', 'worker', 'high'])

    def tearDown(self):
        self.rs.terminate()
        self.rw.terminate()

    @unittest.skipIf(not has_docker, "Docker container not usable")
    def testRemoteExecute(self):
        if os.path.isfile('result_rq.txt'):
            os.remove('result_rq.txt')
        script = SoS_Script('''
[10]
output: 'result_rq.txt'
task:

run:
  echo 'rq' > 'result_rq.txt'

''')
        wf = script.workflow()
        Base_Executor(wf, config={
                'config_file': '~/docker.yml',
                'wait_for_task': True,
                'default_queue': 'local_rq',
                'sig_mode': 'force',
                }).run()
        self.assertTrue(file_target('result_rq.txt').exists())
        with open('result_rq.txt') as res:
            self.assertEqual(res.read(), 'rq\n')

if __name__ == '__main__':
    unittest.main()
