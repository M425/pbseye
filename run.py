from flask import Flask
from subprocess import Popen, PIPE
import json
import shlex

app = Flask(__name__)


def build_resp(data):
    return json.dumps(data)


def run_cmd(cmdstr):
    proc = Popen(shlex.split(cmdstr), stdout=PIPE)
    (output, err) = proc.communicate()
    exit_code = proc.wait()

    lines_output = []
    if output:
        for line in output.split('\n'):
            if line is not None and line != '':
                lines_output.append(line)

    lines_errors = []
    if err:
        for line in err.split('\n'):
            if line is not None and line != '':
                lines_errors.append(line)

    return (exit_code, lines_output, lines_errors)


@app.route("/")
def hello():
    proc_exit, proc_out, proc_err = run_cmd('qstat -a')
    if len(proc_out) < 1:
        return build_resp([])
    # Job ID  Username    Queue    Jobname          SessID  NDS   TSK   Memory   Time    S   Time

    proc_out = proc_out[4:]
    jobs = []
    for line in proc_out:
        linarr = line.split()
        print linarr
        jobs.append({
            'job_id': linarr[0],
            'user': linarr[1],
            'queue': linarr[2],
            'job': linarr[3],
            'session': linarr[4],
            'NDS': linarr[5],
            'TSK': linarr[6],
            'memory': linarr[7],
            'reqTime': linarr[8],
            'status': linarr[9],
            'eleTime': linarr[10]
        })
    return build_resp(jobs)


@app.route("/jobs")
def hello():
    proc_exit, proc_out, proc_err = run_cmd('qstat -a')
    if len(proc_out) < 1:
        return build_resp([])
    # Job ID  Username    Queue    Jobname          SessID  NDS   TSK   Memory   Time    S   Time

    proc_out = proc_out[4:]
    jobs = []
    for line in proc_out:
        linarr = line.split()
        print linarr
        jobs.append({
            'job_id': linarr[0],
            'user': linarr[1],
            'queue': linarr[2],
            'job': linarr[3],
            'session': linarr[4],
            'NDS': linarr[5],
            'TSK': linarr[6],
            'memory': linarr[7],
            'reqTime': linarr[8],
            'status': linarr[9],
            'eleTime': linarr[10]
        })
    return build_resp(jobs)
