from flask import Flask
from flask import send_from_directory
from subprocess import Popen, PIPE
import json
import shlex
import xml.etree.ElementTree as ET

app = Flask(__name__)


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response


def build_resp(data):
    return json.dumps({"data": data})


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


@app.route("/jobs")
def get_jobs():
    proc_exit, proc_out, proc_err = run_cmd('qstat -an1')
    if len(proc_out) < 1:
        return build_resp([])
    # Job ID  Username    Queue    Jobname          SessID  NDS   TSK   Memory   Time    S   Time

    proc_out = proc_out[4:]
    jobs = []
    for line in proc_out:
        linarr = line.split()
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
            'eleTime': linarr[10],
            'wn': linarr[11],
        })
        # if linarr[9] == 'C':
        #     proc_exit, proc_out, proc_err = run_cmd('qstat -f {} -1'.format(linarr[0].split('.')[0]))
        #     for line in proc_out:
        #         if line[:18] == '    exit_status = ':
        #             exit_status = line[18:]
        #     jobs[-1]['exit_status'] = exit_status
        # else:
        #     jobs[-1]['exit_status'] = '--'

        proc_exit, proc_out, proc_err = run_cmd('qstat -f {} -1'.format(linarr[0].split('.')[0]))
        for line in proc_out:
            line = line.strip()
            j_kvarr = line.split('=', 1)
            if len(j_kvarr) != 2:
                continue
            [j_key, j_val] = j_kvarr
            j_key = j_key.strip()
            j_val = j_val.strip()
            jobs[-1][j_key] = j_val

    return build_resp(jobs)


@app.route("/xjob/<int:job_id>")
def get_jobdetailx(job_id):
    '''
    <Data>
      <Job>
        <Job_Id>89890.gridvm03.roma2.infn.it</Job_Id>
        <Job_Name>test-job.sh</Job_Name>
        <Job_Owner>pinamonti@atlas-vm-pinamonti</Job_Owner>
        <job_state>R</job_state>
        <queue>localq</queue>
        <server>gridvm03.roma2.infn.it</server>
        <Checkpoint>u</Checkpoint>
        <ctime>1518884705</ctime>
        <Error_Path>localhost:/home/pinamonti/test-job.sh.e89890</Error_Path>
        <exec_host>wn20.localdomain/1</exec_host>
        <Hold_Types>n</Hold_Types>
        <Join_Path>n</Join_Path>
        <Keep_Files>n</Keep_Files>
        <Mail_Points>a</Mail_Points>
        <mtime>1518884706</mtime>
        <Output_Path>localhost:/home/pinamonti/test-job.sh.o89890</Output_Path>
        <Priority>0</Priority>
        <qtime>1518884705</qtime>
        <Rerunable>True</Rerunable>
        <session_id>55232</session_id>
        <etime>1518884705</etime>
        <submit_args>test-job.sh -q localq -o localhost:/home/pinamonti/ -e localhost:/home/pinamonti/</submit_args>
        <start_time>1518884706</start_time>
        <start_count>1</start_count>
        <fault_tolerant>False</fault_tolerant>
        <submit_host>atlas-vm-pinamonti</submit_host>
        <init_work_dir>/home/pinamonti</init_work_dir>
      </Job>
    </Data>
    '''
    proc_exit, proc_out, proc_err = run_cmd('qstat -f {} -x'.format(job_id))
    if len(proc_out) < 1:
        return build_resp([])

    root = ET.fromstring(' '.join(proc_out))
    xmljob = root.find('Job')
    return build_resp({
        'Job_Id'         : xmljob.find('Job_Id').text,
        'Job_Name'       : xmljob.find('Job_Name').text,
        'Job_Owner'      : xmljob.find('Job_Owner').text,
        'job_state'      : xmljob.find('job_state').text,
        'queue'          : xmljob.find('queue').text,
        'server'         : xmljob.find('server').text,
        'Checkpoint'     : xmljob.find('Checkpoint').text,
        'ctime'          : xmljob.find('ctime').text,
        'Error_Path'     : xmljob.find('Error_Path').text,
        'exec_host'      : xmljob.find('exec_host').text,
        'Hold_Types'     : xmljob.find('Hold_Types').text,
        'Join_Path'      : xmljob.find('Join_Path').text,
        'Keep_Files'     : xmljob.find('Keep_Files').text,
        'Mail_Points'    : xmljob.find('Mail_Points').text,
        'mtime'          : xmljob.find('mtime').text,
        'Output_Path'    : xmljob.find('Output_Path').text,
        'Priority'       : xmljob.find('Priority').text,
        'qtime'          : xmljob.find('qtime').text,
        'Rerunable'      : xmljob.find('Rerunable').text,
        'session_id'     : xmljob.find('session_id').text,
        'etime'          : xmljob.find('etime').text,
        'submit_args'    : xmljob.find('submit_args').text,
        'start_time'     : xmljob.find('start_time').text,
        'start_count'    : xmljob.find('start_count').text,
        'fault_tolerant' : xmljob.find('fault_tolerant').text,
        'submit_host'    : xmljob.find('submit_host').text,
        'init_work_dir'  : xmljob.find('init_work_dir').text,
        'exit_status'    : xmljob.find('exit_status').text if xmljob.find('exit_status') is not None else '--',
        'comp_time'      : xmljob.find('comp_time').text if xmljob.find('comp_time') is not None else '--',
        'mem'      : xmljob.find('mem').text if xmljob.find('mem') is not None else '--',
        'vmem'      : xmljob.find('vmem').text if xmljob.find('vmem') is not None else '--',
        'walltime'      : xmljob.find('walltime').text if xmljob.find('walltime') is not None else '--',
    })


@app.route("/job/<int:job_id>")
def get_jobdetail(job_id):
    proc_exit, proc_out, proc_err = run_cmd('qstat -f {} -1'.format(job_id))
    if len(proc_out) < 1:
        return build_resp([])

    ret = {}
    for line in proc_out:
        line = line.strip()
        j_kvarr = line.split('=', 1)
        if len(j_kvarr) != 2:
            continue
        [j_key, j_val] = j_kvarr
        j_key = j_key.strip()
        j_val = j_val.strip()
        ret[j_key] = j_val

    return build_resp(ret)


@app.route("/stdstream/<stdtype>/<int:job_id>")
def get_jobstream(stdtype, job_id):
    proc_exit, proc_out, proc_err = run_cmd('qstat -f {} -x'.format(job_id))
    if len(proc_out) < 1:
        return build_resp([])

    root = ET.fromstring(' '.join(proc_out))
    xmljob = root.find('Job')
    target_path = 'Output_Path' if stdtype == 'output' else 'Error_Path'
    jobstdstream = xmljob.find(target_path).text.split(':')[1]
    submit_host = xmljob.find('submit_host').text

    proc_exit, proc_out, proc_err = run_cmd('cat {}'.format(jobstdstream))
    if len(proc_out) < 1:
        return build_resp([])

    return build_resp(proc_out)


@app.route("/serverinfo")
def get_serverinfo():
    ret = {}
    ret['localgroup'] = {'total': 0, 'used': 0, 'avail': 0}
    proc_exit, proc_out, proc_err = run_cmd('df')
    if len(proc_out) > 1:
        for line in proc_out:
            line = line.strip()
            linearr = line.split(' ')
            if linearr[-1].strip() == '/localgroup':
                ret['localgroup']['total'] = linearr[0].strip()
                ret['localgroup']['used'] = linearr[1].strip()
                ret['localgroup']['avail'] = linearr[2].strip()

    return build_resp(ret)
