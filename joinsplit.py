
import subprocess
import os
from math import ceil

from editNamelist import editNamelistFile
from batch import Batch
from util import getPaths, getEnsDirectory
import argparse

def genSplitCommands(cm_args, batch, file_dir, file_base):
    work_path, input_path, debug_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True)

    nx, ny = cm_args.mpi_config

    input_file_name = "%s/%s.hdfsubdomain.input" % (input_path, file_base)
    debug_file_name = "%s/%s.hdfsubdomain.debug" % (debug_path, file_base)

    kwargs = { 'hisfile(1)':"%s/%s.hdf%06d" % (work_path, file_base, t_ens) }
    editNamelistFile("%s/hdfsubdomain.input" % base_path, input_file_name,
        hdmpinopt=2,
        nproc_x=1, nproc_y=1,
        runname=file_base,
        dirname="%s/%s" % (work_path, file_dir),
        hdmpfheader="%s/%s/%s" % (work_path, file_dir, file_base),
        grdbasfn="%s/%s.hdfgrdbas" % (work_path, file_base),
        nprocx_in=1, nprocy_in=1,
        nprocx_out=nx, nprocy_out=ny,
        **kwargs
    )

    commands = []
    cmd = batch.genMultiMPICommand("$base/hdfsubdomain", cm_args.mpi_config, stdin=input_file_name, stdout=debug_file_name)
    commands.append(cmd)
    return commands

def splitExperiment(cm_args, batch):
    work_path, input_path, debug_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True)

    commands = []
    for n_ens in xrange(cm_args.n_ens_members):
        ens_dir, ens_base = getEnsDirectory(cm_args.n_ens_members, cm_args.fcst)
        cmds = genSplitCommands(cm_args, batch, ens_dir, ens_base)
        commands.extend(cmds)
    
    mean_base = 'efmean' if cm_args.fcst else 'enmean'
    cmds = genSplitCommands(cm_args, batch, '.', mean_base)
    commands.extend(cmds)

    commands.append("wait")

    n_cores = cm_args.n_ens_members
    n_nodes = batch.getNNodes(n_cores) 

    file_text = batch.genSubmission(commands, jobname=cm_args.job_name, debugfile="%s/%s-split.output" % (debug_path, cm_args.job_name), nmpi=n_cores, nnodes=n_nodes, queue='normal', timereq="00:45:00")

    batch_file_name = "%s-split.csh" % job_name

    file = open(batch_file_name, 'w')
    file.write(text)
    file.close()

    if cm_args.submit:
        batch.submit(file_text)
    else:
        print "I would submit here ..."

    return

def prepareJoinNamelist(cm_args, file_dir, file_base):
    work_path, input_path, debug_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True)

    start_time = cm_args.t_ens_start
    for t_ens in xrange(cm_args.t_ens_start, cm_args.t_ens_end + cm_args.dt_ens_step, cm_args.dt_ens_step):
        if os.path.exists("%s/%s.hdf%06d" % (work_path, file_base, t_ens)):
            start_time = t_ens + cm_args.dt_ens_step

    if start_time > cm_args.t_ens_end:
        print "Ensemble member %s is done, moving on ..." % file_base
        return False

    txt_file_base = "%s.%d-%d.hdfsubdomain" % (file_base, cm_args.t_ens_start, cm_args.t_ens_end)
    input_file_name = "%s/%s.input" % (input_path, txt_file_base)

    nx, ny = cm_args.mpi_config

    editNamelistFile("%s/hdfsubdomain.input" % cm_args.base_path, input_file_name,
        hdmpinopt=1,
        nproc_x=nx, nproc_y=ny,
        runname=file_base,
        dirname="%s" % work_path,
        tbgn_dmpin=start_time,
        tend_dmpin=cm_args.t_ens_end,
        tintv_dmpin=cm_args.dt_ens_step,
        hdmpfheader="%s/%s/%s" % (work_path, file_dir, file_base),
        nprocx_in=nx, nprocy_in=ny,
        nprocx_out=1, nprocy_out=1,
    )
    return True

def genJoinCommand(cm_args, batch, file_dir, file_base):
    work_path, input_path, debug_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True)

    txt_file_base = "%s.%d-%d.hdfsubdomain" % (file_base, cm_args.t_ens_start, cm_args.t_ens_end)
    input_file_name = "%s/%s.input" % (input_path, txt_file_base)
    debug_file_name = "%s/%s.debug" % (debug_path, txt_file_base)

    if cm_args.job_grouping == 'yes':
        cmd = batch.genMultiMPICommand("$base/hdfsubdomain", cm_args.mpi_config, stdin=input_file_name, stdout=debug_file_name)
    else:
        cmd = batch.genMPICommand("$base/hdfsubdomain", cm_args.mpi_config, stdin=input_file_name, stdout=debug_file_name)
    return cmd

def submitJoin(cm_args, cmds, batch, file_base, write_batch=False):
    debug_path = getPaths(cm_args.base_path, cm_args.job_name, debug=True)

    commands = [
        "set base=%s" % cm_args.base_path,
        "cd $base",
        "",
    ]
    commands.extend(cmds)

    c_runs = sum( 1 for c in cmds if c.strip().startswith(batch.getMPIprogram()) )

    if c_runs == 0:
        print "No jobs to submit!"
        return

    nx, ny = cm_args.mpi_config
    n_cores = nx * ny * c_runs
    n_nodes = batch.getNNodes(n_cores)

    file_text = batch.genSubmission(commands, 
        jobname=cm_args.job_name, 
        debugfile="%s/%s-%s-join.output" % (debug_path, file_base, cm_args.job_name), 
        nmpi=n_cores, 
        nnodes=n_nodes, 
        queue='normal', 
        timereq=(cm_args.time_req + ":00")
    )

    if write_batch:
        batch_file_name = "%s-join.csh" % cm_args.job_name
        file = open(batch_file_name, 'w')
        file.write(file_text)
        file.close()

    if cm_args.submit:
        batch.submit(file_text)
    else:
        print "I would submit %s here ..." % file_base

    return

def joinExperiment(cm_args, batch):
    if cm_args.job_grouping == 'yes':
        cmds = []

    for n_ens in xrange(cm_args.n_ens_members):
        ens_directory, ens_base = getEnsDirectory(n_ens + 1, cm_args.fcst)

        if prepareJoinNamelist(cm_args, ens_directory, ens_base):
            cmd = genJoinCommand(cm_args, batch, ens_directory, ens_base)
            if cm_args.job_grouping == 'yes':
                cmds.append(cmd)
            else:
                submitJoin(cm_args, [ cmd ], batch, ens_base, write_batch=(n_ens == 1))

    mean_base = 'efmean' if cm_args.fcst else 'enmean'
    if prepareJoinNamelist(cm_args, '.', mean_base):
        cmd = genJoinCommand(cm_args, batch, '.', mean_base)
        if cm_args.job_grouping == 'yes':
            cmds.append(cmd)
        else:
            submitJoin(cm_args, [ cmd ], batch, mean_base)

    if cm_args.job_grouping == 'yes':
        cmds.extend([ "", "wait" ])
        submitJoin(cm_args, cmds, batch, 'ens', write_batch=True)

    print "Done submitting."
    return

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-ens', dest='n_ens_members', default=4, type=int)
    ap.add_argument('--members', dest='members', nargs='+', default=[], type=int)
    ap.add_argument('--base-path', dest='base_path', default=os.getcwd())
    ap.add_argument('--job-name', dest='job_name', default="run_osse_test")
    ap.add_argument('--mpi-config', dest='mpi_config', nargs=2, default=(3, 4), type=int)
    ap.add_argument('--time-req', dest='time_req', default='00:30')

    ap.add_argument('--ens-start', dest='t_ens_start', default=1200, type=int)
    ap.add_argument('--ens-end', dest='t_ens_end', default=1500, type=int)
    ap.add_argument('--ens-step', dest='dt_ens_step', default=300, type=int)

    ap.add_argument('--job-grouping', dest='job_grouping', choices=['no', 'yes'], default='no')
    ap.add_argument('--split', dest='split', action='store_true')
    ap.add_argument('--forecast', dest='fcst', action='store_true')
    ap.add_argument('--no-submit', dest='submit', action='store_false')

    args = ap.parse_args()

    batch = Batch('stampede')
    if args.split:
        splitExperiment(args, batch)
    else:
        joinExperiment(args, batch)
    return

if __name__ == "__main__":
    main()
