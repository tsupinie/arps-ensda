
import subprocess
from datetime import datetime
from math import ceil
import time

def parseQLineStampede(line):
    cols_order = ['id', 'name', 'username', 'state', 'ncores', 'timerem'] #, 'timestart']
    cols = dict([
        ('id',        slice(0,  9)), 
        ('name',      slice(9,  20)),
        ('username',  slice(20, 34)),
        ('state',     slice(34, 42)),
        ('ncores',    slice(42, 50)),
        ('timerem',   slice(50, 61)),
        ('timestart', slice(61, None))
    ])

    line_dict = {}
    for name in cols_order:
        line_dict[name] = line[cols[name]].strip()

    try:
        line_dict['id'] = int(line_dict['id'])
        line_dict['ncores'] = int(line_dict['ncores'])
    except ValueError:
        return ""

    return line_dict

def parseSubmitStampede(text):
    lines = text.split("\n")
    ret_val = None
    try:
        submit_line = [ l for l in lines if l.startswith("Submitted") ][0]
        ret_val = int(submit_line.rsplit(" ", 1)[-1])
    except IndexError:
        print "Error!"
        print text
        ret_val = -1
    return ret_val

def parseQLineKraken(line):
    field_widths = []
    qstat = []
    for line in text:
        if line.startswith('---'):
            field_widths = [ len(f) for f in line.split() ]
            continue

        if field_widths != []:
            fields = []
            offset = 0
            for width in field_widths:
                fields.append(line[(offset):(offset + width)])
                offset += width + 1

#           print fields
            qstat.append(fields)
    return qstat

def parseSubmitKraken(text):
    return int(text.split(".")[0])

_environment = {
    'stampede':{
        'btmarker':"SBATCH", 
        '-J':"%(jobname)s",
        '-o':"%(debugfile)s",
        '-n':"%(nmpi)d",
        '-N':"%(nnodes)d",
        '-p':"%(queue)s",
        '-t':"%(timereq)s",
        'queueprog':'showq',   # Name of the program that gets the queue state
        'queueparse':parseQLineStampede,
        'submitprog':'sbatch', # Name of the program submits the batch file
        'submitparse':parseSubmitStampede,

        'mpiprog':'ibrun',     # Name of the program that runs MPI
        'mpicoresopt':'np',
        'mpimultcoresopt':'n',
        'mpimultoffsetopt':'o',
        'mpiompopt':'',

        'n_cores_per_node':16, 
    },
    'kraken':{ 
        'btmarker':"PBS",
        'queueprog':'qstat',
        'queueparse':parseQLineKraken,
        'submitprog':'qsub',
        'submitparse':parseSubmitKraken,

        'mpiprog':'aprun',
        'mpicoresopt':'n',
        'mpimultcoresopt':'n',
        'mpimultoffsetopt':'',
        'mpiompopt':'d',

        'n_cores_per_node':12,
    },
    'oscer':{
    }
}

class Batch(object):
    def __init__(self, environment, username="tsupinie"):
        self._env = _environment[environment]
        self._username = username
        self._resetMultiMPICount()
        return

    def genSubmission(self, commands, **kwargs):
        env_dict = {}
        for k, v in self._env.iteritems():
            try:
                assert k[0] == '-'
                env_dict[k] = v % kwargs
            except (AssertionError, KeyError):
                pass
            
        text = "#!/bin/csh\n"
        for arg, val in env_dict.iteritems():
            text += "#%s %s %s\n" % (self._env['btmarker'], arg, val)

        text += "\n" + "\n".join(commands) + "\n"
        return text

    def genMPICommand(self, command, mpi_config, stdin=None, stdout=None):
        nx, ny = mpi_config
        options = [
            ('mpicoresopt', nx * ny)
        ]
        stdin_str, stdout_str, switch_str = self._prepCmdArgs(stdin, stdout, *options)
        return "%s%s %s%s%s" % (self._env['mpiprog'], switch_str, command, stdin_str, stdout_str)

    def genHybridCommand(self, command, mpi_config, stdin=None, stdout=None):
        nx, ny = mpi_config
        options = [
            ('mpicoresopt', nx * ny), 
            ('mpiompopt', self.getNCoresPerNode())
        ]
        stdin_str, stdout_str, switch_str = self._prepCmdArgs(stdin, stdout, *options)
        return "%s%s %s%s%s" % (self._env['mpiprog'], switch_str, command, stdin_str, stdout_str)

    def genMultiMPICommand(self, command, mpi_config, first_cmd=False, max_cmds=None, stdin=None, stdout=None, offset=None):
        if first_cmd: self._resetMultiMPICount()
        nx, ny = mpi_config

        if offset is None:
            offset = nx * ny

        options = [
            ('mpimultcoresopt', nx * ny), 
            ('mpimultoffsetopt', self._mpi_offset)
        ]
        stdin_str, stdout_str, switch_str = self._prepCmdArgs(stdin, stdout, *options)

        if max_cmds is None or (max_cmds - 1) * offset != self._mpi_offset:
            self._mpi_offset += offset
        else:
            self._resetMultiMPICount()
        return "%s%s %s%s%s &" % (self._env['mpiprog'], switch_str, command, stdin_str, stdout_str)

    def _resetMultiMPICount(self):
        self._mpi_offset = 0
        return

    def submit(self, text):
        subm = subprocess.Popen([ self._env['submitprog'] ], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        subm.stdin.write(text)
        subm.stdin.close()

        ret_value = subm.stdout.read()
        pid = self._env['submitparse'](ret_value)

        if pid > 0:
            print "Submitted job %d ..." % pid

        self._resetMultiMPICount()
        return pid

    def getQueueStatus(self, display=True):
        queue = subprocess.Popen([self._env['queueprog'], '-u', self._username], stdout=subprocess.PIPE)
        queue_text = queue.communicate()[0]
        lines = []
        for line in queue_text.strip().split("\n"):
            line_dict = self._env['queueparse'](line)
            if line_dict != "":
                lines.append(line_dict)

        if display:
            self.displayQueue(lines)
        return lines

    def monitorQueue(self, pids, sleep_time=2):
        all_completed = False
        job_completed = [ False for p in pids ]

        while not all_completed:
            time.sleep(sleep_time * 60)

            queue = self.getQueueStatus()
            jobs_queued = [ r['id'] for r in queue ]

            for idx, pid in enumerate(pids):
                if pid in jobs_queued:
                    jdy = jobs_queued.index(pid)
                    if queue[jdy]['state'] == 'Complte':
                        job_completed[idx] = True
                else:
                    job_completed[idx] = True

            print "Completed: " + " ".join( "C" if c else "N" for c in job_completed )
            all_completed = all(job_completed)

        print "All jobs are completed."
        return

    def displayQueue(self, queue):
        print "Queue State as of %s" % datetime.now().strftime("%H:%M:%S %d %b %Y")
        for line in queue:
            if line['state'].lower() == 'running':
                print "%(name)s (PID %(id)d): %(state)s (%(timerem)s remaining)" % line
            else:
                print "%(name)s (PID %(id)d): %(state)s" % line

        if len(queue) == 0:
            print "[ empty ]"
        return

    def getMPIprogram(self):
        return self._env['mpiprog']

    def getNCoresPerNode(self):
        return self._env['n_cores_per_node']

    def getNNodes(self, n_cores):
        return int(ceil(float(n_cores) / self.getNCoresPerNode()))

    def _prepCmdArgs(self, stdin, stdout, *opts):
        stdin_str = "" if stdin is None else " < %s" % stdin
        stdout_str = "" if stdout is None else " > %s" % stdout

        switches = []
        for opt, val in opts:
            sw = "" if self._env[opt] == '' else " -%s %d" % (self._env[opt], val)
            switches.append(sw)

        switch_str = "".join(switches)
        return stdin_str, stdout_str, switch_str

if __name__ == "__main__":
    bt = Batch('stampede')
#   bt_text = bt.gen(['ls $HOME', 'ls $WORK'], jobname='test', debugfile='test.debug', ncores=1, nnodes=1, queue='normal', timereq='00:05:00')
#   bt.submit(bt_text)
    bt.getQueueStatus()
