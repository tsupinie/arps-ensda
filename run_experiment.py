
import os, sys
import subprocess 
import time
import argparse
from math import ceil
from collections import defaultdict
import re

from editNamelist import editNamelistFile
from batch import Batch
from util import getPaths, getEnsDirectory

def isDivisible(dividend, divisor):
    return float(dividend) / int(divisor) == int(dividend) / int(divisor)

def appendCommands(current_commands, commands):
    for e, cmd in commands.iteritems():
        current_commands[e].extend(cmd)
    return current_commands

def prettyPath(path, base_path):
    if path.startswith(base_path):
        path = '$base' + path[len(base_path):]

    if path.endswith('/'):
        path = path[:-1]

    return re.sub(r'/+', "/", path)

def doForEnsemble(commands, member_list, current_commands=None):

    if type(commands) not in [ list, tuple ]:
        commands = [ commands ]

    if current_commands is None:
        current_commands = defaultdict(list)

    for n_ens in member_list:
        key = "ena%03d" % (n_ens + 1)
        for cmd in commands:
            current_commands[key].append(cmd % {'ens':(n_ens + 1)})

    return current_commands

def doForMPIConfig(command, mpi_config):
    nproc_x, nproc_y = mpi_config

    commands = []

    for idx in range(nproc_x):
        for jdy in range(nproc_y):
            commands.append(command % {'x_proc':idx + 1, 'y_proc':jdy + 1, 'ens':'%(ens)03d' })

    return commands

def generateEnsembleIntegration(cm_args, batch, start_time, end_time, dump_time, split_files='neither', move_for_assim=True):
    work_path, input_path, debug_path, bc_path, hx_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True, boundary=True, hx=True)

    nproc_x, nproc_y = cm_args.mpi_config

    extraneous_files = [
        "%s/%s.hdf%06d.01" % (prettyPath(work_path, cm_args.base_path), 'ena%(ens)03d', start_time),
        "%s/%s.hdfgrdbas.01" % (prettyPath(work_path, cm_args.base_path), 'ena%(ens)03d'),
        "%s/%s.log" % (prettyPath(work_path, cm_args.base_path), 'ena%(ens)03d'),
        "%s/%s.maxmin" % (prettyPath(work_path, cm_args.base_path), 'ena%(ens)03d'),
    ]

    read_split = split_files in ['read', 'both']
    dump_split = split_files in ['dump', 'both']

    epilogue = []

    arps_input_file_name = "%s/%s.%d-%d.arps.input" % (input_path, 'ena%(ens)03d', start_time, end_time)
    arps_debug_file_name = "%s/%s.%d-%d.arps.debug" % (debug_path, 'ena%(ens)03d', start_time, end_time)
    if cm_args.algorithm == "4densrf":
        arpsenkf_input_file_name = "%s/%d.arpsenkf.input" % (input_path, end_time)

#       epilogue.extend([
#           "mv ????????????_%s.lso %s" % ('%(ens)03d', prettyPath(hx_path, cm_args.base_path)),
#           "mv ????????????_%s.snd %s" % ('%(ens)03d', prettyPath(hx_path, cm_args.base_path)),
#           "mv ????_*_??????_%s %s" % ('%(ens)03d', prettyPath(hx_path, cm_args.base_path)),
#       ])

    for n_ens in cm_args.members:
        ena_member_directory, ena_member_name = getEnsDirectory(n_ens + 1, False)
        enf_member_directory, enf_member_name = getEnsDirectory(n_ens + 1, True)

        ens_arps_file_name = arps_input_file_name % { 'ens':(n_ens + 1) } 

        kwargs = {'hxopt':0, 'tstop':end_time}

        # Find input file and path
        if read_split:
            kwargs['inifile'] = "%s/%s/%s.hdf%06d" % (work_path, ena_member_directory, ena_member_name, start_time),
            kwargs['inigbf'] = "%s/%s/%s.hdfgrdbas" % (work_path, ena_member_directory, ena_member_name),
        else:
            kwargs['inifile'] = "%s/%s.hdf%06d" % (work_path, ena_member_name, start_time),
            kwargs['inigbf'] = "%s/%s.hdfgrdbas" % (work_path, ena_member_name),

        # Find output file and path
        if dump_split:
            if move_for_assim:
                kwargs['dirname'] = "%s/%s/" % (work_path, enf_member_directory)
            else:
                kwargs['dirname'] = "%s/%s/" % (work_path, ena_member_directory)
        else:
            kwargs['dirname'] = "%s/" % work_path

        # Find what our run name should be
        kwargs['runname'] = enf_member_name if move_for_assim else ena_member_name

        if cm_args.algorithm == '4densrf':
            kwargs['memid'] = n_ens + 1
            kwargs['hxopt'] = 1
            kwargs['tstop'] += 150

        editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arps_template), ens_arps_file_name,
            nproc_x=nproc_x, nproc_y=nproc_y,
            initopt=3,
            exbcname="%s/%s" % (bc_path, ena_member_name),
            tstart=start_time,
            tstrtdmp=start_time + dump_time,
            thisdmp=dump_time,
            dmp_out_joined=int(not dump_split),
            inisplited=int(read_split),
            **kwargs
        )

    cmd_base = "$base/arps %s" % prettyPath(arps_input_file_name, cm_args.base_path) if cm_args.algorithm == 'ensrf' else "$base/arps %s %s" % (prettyPath(arps_input_file_name, cm_args.base_path), prettyPath(arpsenkf_input_file_name, cm_args.base_path))

    if cm_args.job_grouping == 'none':
        cmd = batch.genMPICommand(cmd_base, cm_args.mpi_config, stdout=arps_debug_file_name)

        command = [
            cmd,
            "",
            "rm %s" % ( " ".join(extraneous_files) )
        ]
        command.extend([ l % {'ens':(e + 1)} for l in epilogue for e in cm_args.members ])
        command_lines = doForEnsemble(command, cm_args.members)
    else:
        max_parallel_cmds = cm_args.n_ens_members if cm_args.job_grouping == 'integration' else batch.getNCoresPerNode()
        command_lines = {}
        for n_ens in cm_args.members:
            key = "ena%03d" % (n_ens + 1)

            cmd = batch.genMultiMPICommand(cmd_base, cm_args.mpi_config, first_cmd=(n_ens == cm_args.members[0]), max_cmds=max_parallel_cmds, stdout=prettyPath(arps_debug_file_name, cm_args.base_path))
            command_lines[key] = [ cmd % { 'ens':(n_ens + 1)} ]
            cleanup = "rm %s" % ( " ".join(extraneous_files) )
            command_lines[key].extend([ "", cleanup % {'ens':(n_ens + 1)} ] + [ l % {'ens':(e + 1)} for l in epilogue for e in cm_args.members ])

    return command_lines

def generateEnsemblePerturbations(cm_args, batch, start_time):
    work_path, input_path, debug_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True)

    for n_ens in cm_args.members:
        ens_member_name = "ena%03d" % (n_ens + 1)

        arpsenkfic_input_file_name = "%s/%s.arpsenkfic.input" % (input_path, ens_member_name)

        editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arpsenkfic_template), arpsenkfic_input_file_name,
            seeds=-n_ens,
            dirnamp="%s/" % work_path,
            outdumpdir="%s/" % work_path,
            outname=ens_member_name,
            tfgs=start_time)

    arps_input_file_name = "%s/arps.input" % input_path
    arpsenkfic_input_file_name = "%s/%s.arpsenkfic.input" % (input_path, 'ena%(ens)03d')
    arpsenkfic_debug_file_name = "%s/%s.arpsenkfic.debug" % (debug_path, 'ena%(ens)03d')

    if cm_args.job_grouping == 'none':
        command = batch.genMPICommand(
            "$base/arpsenkfic %s" % prettyPath(arps_input_file_name, cm_args.base_path), 
            cm_args.mpi_config, 
            stdin=prettyPath(arpsenkfic_input_file_name, cm_args.base_path), 
            stdout=prettyPath(arpsenkfic_debug_file_name, cm_args.base_path)
        )
        command_lines = doForEnsemble([ command, "" ], cm_args.members)
    else:
        max_parallel_cmds = cm_args.n_ens_members if cm_args.job_grouping == 'integration' else batch.getNCoresPerNode()
        command_lines = {}
        for n_ens in cm_args.members:
            key = "ena%03d" % (n_ens + 1)

            # cm_args.mpi_config,
            command = batch.genMultiMPICommand(
                "$base/arpsenkfic %s" % prettyPath(arps_input_file_name, cm_args.base_path), 
                (1, 1),
                first_cmd=(n_ens == cm_args.members[0]),
                max_cmds=max_parallel_cmds,
                stdin=prettyPath(arpsenkfic_input_file_name, cm_args.base_path), 
                stdout=prettyPath(arpsenkfic_debug_file_name, cm_args.base_path)
            )

            command_lines[key] = [ command % { 'ens':(n_ens + 1)}, "" ]
    return command_lines

def generateEnKFAssimilation(cm_args, batch, assim_time, radar_data_flag):
    work_path, input_path, debug_path, batch_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True, batch=True)

    nproc_x, nproc_y = cm_args.mpi_config

    arps_input_file_name = "%s/%d.arps.input" % (input_path, assim_time)
    enkf_input_file_name = "%s/%d.arpsenkf.input" % (input_path, assim_time)
    enkf_debug_file_name = "%s/%d.arpsenkf.debug" % (debug_path, assim_time)
    batch_file_name = "%s/%d.csh" % (batch_path, assim_time)

    kwargs = {}

    # Figure out what conventional data we're assimilating (combine this with the next section!)
    cvn_data_flags = dict( (k, getattr(cm_args, k)) for k in ['sndgflags', 'profflags', 'surfflags' ] )
    assim_all = False
    if isDivisible(assim_time, 3600):
        print "Assimilate all data ..."
    else:
        cvn_data_flags['sndgflags'] = 'no'
        cvn_data_flags['profflags'] = 'no'

    # Conventional DA flags
    for cvn_flag, kw_flag in [ ('sndgflags', 'sndassim'), ('profflags', 'proassim'), ('surfflags', 'sfcassim') ]:
        if cvn_flag in cvn_data_flags and cvn_data_flags[cvn_flag].lower() == 'yes':
            kwargs[kw_flag] = 1
        else:
            kwargs[kw_flag] = 0

    if cm_args.split_files:
        kwargs['nproc_x_in'] = nproc_x
        kwargs['nproc_y_in'] = nproc_y
        kwargs['inidirname'] = "%s/ENF%s/" % (work_path, "%3N")
    else:
        kwargs['inidirname'] = "%s/" % work_path

    # Figure out what our covariance inflation will be (combine this with the next section!)
    for cov_infl in cm_args.cov_infl:
        if ':' in cov_infl:
            time, factors = cov_infl.split(':')
            if assim_time >= int(time):
                covariance_inflation = factors
        else:
            covariance_inflation = cov_infl

    print "Covariance inflation for this timestep is", covariance_inflation

    # Covariance inflation flags
    kwargs['mult_inflat'] = 0
    kwargs['adapt_inflat'] = 0
    kwargs['relax_inflat'] = 0

    try:
        covariance_inflation = covariance_inflation.split(',')
    except ValueError:
        covariance_inflation = [ covariance_inflation ]

    for cov_infl in covariance_inflation:
        if '=' in cov_infl:
            inflation_method, inflation_factor = cov_infl.split('=')
            inflation_factor = float(inflation_factor)

            if inflation_method == "mults":
                # Multiplicative inflation in the storm region only
                kwargs['mult_inflat'] = 1
                kwargs['cinf'] = inflation_factor

            elif inflation_method == "multd":
                # Multiplicative inflation over the entire domain
                kwargs['mult_inflat'] = 2
                kwargs['cinf'] = inflation_factor
        
            elif inflation_method == "adapt":
                # Relaxation to Prior Spread ("Adaptive") inflation
                kwargs['adapt_inflat'] = 1
                kwargs['rlxf'] = inflation_factor

            elif inflation_method == "relax":
                # Relaxation to Prior Perturbation ("Relaxation") inflation
                kwargs['relax_inflat'] = 1
                kwargs['rlxf'] = inflation_factor

    if cm_args.algorithm == 'ensrf':
        kwargs['anaopt'] = 2
    elif cm_args.algorithm == '4densrf':
        kwargs['anaopt'] = 5

    n_radars = len(radar_data_flag[True]) if True in radar_data_flag else 0
    radardaopt = 1 if n_radars > 0 else 0

    editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arpsenkf_template), enkf_input_file_name, 
        nen=cm_args.n_ens_members,
        casenam=cm_args.job_name,
        enkfdtadir="%s/" % work_path,
        cvndatadir="%s/obs/" % cm_args.base_path,
        assim_time=assim_time,
        radardaopt=radardaopt,
        nrdrused=n_radars,
        rmsfcst=2,
        hdmpfheader=cm_args.job_name,
        **kwargs
    )

    if cm_args.algorithm == '4densrf':
        #Hard-code the 4densrf assimilation window for now, change me later
        kwargs = {'hdmptim(1)':assim_time, 'tstop':assim_time + 150}
    else:
        kwargs = {}

    joined = 0 if cm_args.split_files else 1
    editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arps_template), arps_input_file_name,
        nproc_x=nproc_x, nproc_y=nproc_y,
        dmp_out_joined=joined,
        inisplited=3 * (1 - joined),
        sfcdat=3,
        dirname="%s/" % work_path,
        **kwargs
    )

    cmd = batch.genHybridCommand("$base/arpsenkf %s" % prettyPath(arps_input_file_name, cm_args.base_path), cm_args.mpi_config, stdin=prettyPath(enkf_input_file_name, cm_args.base_path), stdout=prettyPath(enkf_debug_file_name, cm_args.base_path))
    command_lines = [
        "cd %s" % prettyPath(work_path, cm_args.base_path),
        cmd,
        "cd -",
        "",
    ]

    return command_lines

def generateDomainSubset(cm_args, batch, src_path, start_time, end_time, step_time, perturb_ic=True, copy_ic=False):
    work_path, input_path, debug_path, bc_path = \
        getPaths(cm_args.base_path, cm_args.job_name, work=True, input=True, debug=True, boundary=True)

    for n_ens in cm_args.members:
        ens_member_name = "ena%03d" % (n_ens + 1)

        interp_input_file_name = "%s/%s.arpsintrp.input" % (input_path, ens_member_name)
        arpsenkfic_input_file_name = "%s/%s.arpsenkfic.input" % (input_path, ens_member_name)
        arps_input_file_name = "%s/%s.arps.input" % (input_path, ens_member_name)

        editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arpsintrp_template), interp_input_file_name,
            runname="%s" % ens_member_name,
            hdmpinopt=1,
            hdmpfheader="%s/%s" % (src_path, ens_member_name),
            dirname=bc_path,
            tbgn_dmpin=start_time,
            tend_dmpin=end_time,
            tintv_dmpin=step_time,
        )

        editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arps_template), arps_input_file_name,
            runname="%s" % ens_member_name,
            initopt=3,
            inifile="%s/%s.hdf%06d" % (bc_path, ens_member_name, start_time),
            inigbf="%s/%s.hdfgrdbas" % (bc_path, ens_member_name),
            tstart=start_time,
            tstop=end_time,
            dirname="%s/" % work_path
        )

        if perturb_ic:
            editNamelistFile("%s/%s" % (cm_args.base_path, cm_args.arpsenkfic_template), arpsenkfic_input_file_name,
                seeds=-n_ens,
                dirnamp="%s/" % work_path,
                outdumpdir="%s/" % work_path,
                outname=ens_member_name,
                tfgs=start_time
            )

    interp_input_file_name = "%s/ena%s.arpsintrp.input" % (prettyPath(input_path, cm_args.base_path), '%(ens)03d')
    interp_debug_file_name = "%s/ena%s.arpsintrp.debug" % (prettyPath(debug_path, cm_args.base_path), '%(ens)03d')
    arps_input_file_name = "%s/ena%s.arps.input" % (prettyPath(input_path, cm_args.base_path), '%(ens)03d')
    arpsenkfic_input_file_name = "%s/ena%s.arpsenkfic.input" % (prettyPath(input_path, cm_args.base_path), '%(ens)03d')
    arpsenkfic_debug_file_name = "%s/ena%s.arpsenkfic.debug" % (prettyPath(debug_path, cm_args.base_path), '%(ens)03d')

    command_lines = defaultdict(list)
    if cm_args.job_grouping == "none":
        commands = batch.genMPICommand("$base/arpsintrp %s/arps.input" % prettyPath(input_path, cm_args.base_path), cm_args.mpi_config, stdin=interp_input_file_name, stdout=interp_debug_file_name)
        appendCommands(command_lines, 
            doForEnsemble(commands, cm_args.members)
        )
    else:
        max_parallel_cmds = cm_args.n_ens_members if cm_args.job_grouping in ['integration', 'experiment'] else batch.getNCoresPerNode()
        for n_ens in cm_args.members:
            key = "ena%03d" % (n_ens + 1)

            # cm_args.mpi_config,
            command = batch.genMultiMPICommand(
                "$base/arpsintrp %s/arps.input" % prettyPath(input_path, cm_args.base_path),
                (1, 1),
                first_cmd=(n_ens == cm_args.members[0]),
                max_cmds=max_parallel_cmds,
                stdin=interp_input_file_name, 
                stdout=interp_debug_file_name,
                offset=batch.getNCoresPerNode(),
            )

            command_lines[key].extend([ command % { 'ens':(n_ens + 1)}, "" ])

    if perturb_ic:
        if cm_args.job_grouping == "none":
            commands = batch.genMPICommand("$base/arpsenkfic %s" % arps_input_file_name, cm_args.mpi_config, stdin=arpsenkfic_input_file_name, stdout=arpsenkfic_debug_file_name)
            appendCommands(command_lines, 
                doForEnsemble(commands, cm_args.members)
            )
        else:
            max_parallel_cmds = cm_args.n_ens_members if cm_args.job_grouping in ['integration', 'experiment'] else batch.getNCoresPerNode()
            for n_ens in cm_args.members:
                key = "ena%03d" % (n_ens + 1)

                # cm_args.mpi_config,
                command = batch.genMultiMPICommand(
                    "$base/arpsenkfic %s" % arps_input_file_name,
                    (1, 1),
                    first_cmd=(n_ens == cm_args.members[0]),
                    max_cmds=max_parallel_cmds,
                    stdin=arpsenkfic_input_file_name, 
                    stdout=arpsenkfic_debug_file_name,
                    offset=batch.getNCoresPerNode(),
                )

                command_lines[key].extend([ command % { 'ens':(n_ens + 1)}, "" ])
    else:
        if copy_ic:
            commands = [
               "cp %s/%s.hdf%06d %s" % (prettyPath(bc_path, cm_args.base_path), 'ena%(ens)03d', start_time, prettyPath(work_path, cm_args.base_path)),
               "cp %s/%s.hdfgrdbas %s" % (prettyPath(bc_path, cm_args.base_path), 'ena%(ens)03d', prettyPath(work_path, cm_args.base_path)),
               "cp %s/%s.hdfgrdbas %s/%s.hdfgrdbas" % (prettyPath(bc_path, cm_args.base_path), 'ena%(ens)03d', prettyPath(work_path, cm_args.base_path), 'enf%(ens)03d')
            ]

            appendCommands(command_lines,
                doForEnsemble(commands, cm_args.members)
            )

    return command_lines

def combineMemberCmds(command_set, max_parallel_cmds):
    sections = []
    for t_ens in sorted(command_set.keys()):
        cmds = command_set[t_ens]
        cyc_sections = zip(*[ cmd for ens, cmd in cmds.iteritems() if ens != 'enkf' ])

        # Collapse sections with the same command repeated a bunch of times into one command
        cyc_sections = [ list(sect[:1]) if sect.count(sect[0]) == len(sect) else list(sect) for sect in cyc_sections ]

        # Sort by ensemble member
        cyc_sections = [ sorted(sect, key=lambda cmd: re.findall(r"en[fa]([\d]{3})", cmd)[0]) if len(sect) > 1 and all('ena' in s or 'enf' in s for s in sect) else sect for sect in cyc_sections ]

        par_idxs = [ idx for idx in xrange(len(cyc_sections)) if all( l.endswith('&') for l in cyc_sections[idx] ) ] 
        for pidx in par_idxs[::-1]:
            wait_idxs = [ idx + 1 for idx in xrange(len(cyc_sections[pidx])) if (idx + 1) % max_parallel_cmds == 0 and (idx + 1) != len(cyc_sections[pidx]) ]
            for widx in wait_idxs[::-1]:
                # 'wait's in the middle of sections
                cyc_sections[pidx][widx:widx] = [ '', 'wait', '' ]

            # 'wait's after entire sections
            cyc_sections.insert(pidx + 1, [ '', 'wait' ])

        script = [ l for s in cyc_sections for l in s ]

        if 'enkf' in cmds:
            script.append('')
            script.extend(cmds['enkf'])

        sections.extend(script)
    return sections

def submitGroups(cm_args, batch, command_sets, prologue):
    def timeStr2Minutes(time_str):
        hr, mn = time_str.split(":")
        return int(hr) * 60 + int(mn)

    def minutes2TimeStr(minutes):
        return "%d:%02d" % (minutes / 60, minutes % 60)

    pids = []
    nx, ny = cm_args.mpi_config

    mem_check_integ = "ena%03d" % (cm_args.members[0] + 1)
    last_t_ens = sorted(command_sets.keys())[-1]
    submitted = False
    errored = False

    n_on_enkf = sum( 1 for t_ens in command_sets.iterkeys() if not (t_ens % 3600) and 'enkf' in command_sets[t_ens] )
    n_off_enkf = sum( 1 for t_ens in command_sets.iterkeys() if (t_ens % 3600) and 'enkf' in command_sets[t_ens] )
    n_init_integ = sum( 1 for t_ens in command_sets.iterkeys() if t_ens == cm_args.exp_start and mem_check_integ in command_sets[t_ens] )
    n_post_integ = sum( 1 for t_ens in command_sets.iterkeys() if t_ens != cm_args.exp_start and mem_check_integ in command_sets[t_ens] )

    enkf_time = n_on_enkf * timeStr2Minutes(cm_args.assim_on_req) + \
                n_off_enkf * timeStr2Minutes(cm_args.assim_off_req)
    integ_time = n_init_integ * timeStr2Minutes(cm_args.init_fcst_req) + \
                 n_post_integ * timeStr2Minutes(cm_args.fcst_req)

    if 'enkf' in command_sets[last_t_ens] and mem_check_integ in command_sets[last_t_ens]:
        ens_times = range(cm_args.t_ens_start, cm_args.t_ens_end, cm_args.dt_assim_step)
        n_integ_groups = int(ceil(float(len(cm_args.members)) / batch.getNCoresPerNode()))
        wall_time = minutes2TimeStr(integ_time * n_integ_groups + enkf_time)

        if cm_args.job_grouping == 'cycle':
            print "submitGroups(): Submitting cycle ..."
            # Submit single cycle together
            script = combineMemberCmds(command_sets, batch.getNCoresPerNode())

            n_cores = nx * ny * batch.getNCoresPerNode()
            cycle_num = ens_times.index(last_t_ens) + 1
            pid = submit(cm_args, batch, "cycle%02d" % cycle_num, prologue + script, wall_time, n_cores, write_batch=(cycle_num == 1))

            if pid > 0:           
                pids = [ pid ]
                submitted = True
            else:
                errored = True

        elif cm_args.job_grouping == 'experiment':
            # Check to see that we have all the cycles, submit entire experiment
            if sorted(command_sets.keys()) == ens_times:
                print "submitGroups(): Submitting everything ..."
                script = combineMemberCmds(command_sets, batch.getNCoresPerNode())

                n_cores = nx * ny * batch.getNCoresPerNode()
                pid = submit(cm_args, batch, "exp", prologue + script, wall_time, n_cores, write_batch=True)

                if pid > 0:
                    pids = [ pid ]
                    submitted = True
                else:
                    errored = True

    elif 'enkf' in command_sets[last_t_ens]:
        # Just submit it
        print "submitGroups(): Submitting enkf ..."
        submitted = True

        n_cores = nx * ny * batch.getNCoresPerNode() 
        wall_time = minutes2TimeStr(enkf_time)
        pid = submit(cm_args, batch, "enkf", prologue + command_sets[last_t_ens]['enkf'], wall_time, n_cores, write_batch=True, hybrid=True)
        pids = [ pid ]
    elif mem_check_integ in command_sets[last_t_ens]:
        wall_time = minutes2TimeStr(integ_time)
        if cm_args.job_grouping == 'none':
            # Submit ensemble members individually
            print "submitGroups(): Submitting members ..."

            n_cores = nx * ny
            for ens, cmds in command_sets[last_t_ens].iteritems():
                pid = submit(cm_args, batch, ens, prologue + cmds, wall_time, n_cores, write_batch=(ens == mem_check_integ))
                if pid > -1:
                    pids.append(pid)
                else:
                    errored = True
                    break    
        
            if len(pids) == len(command_sets[last_t_ens]):
                submitted = True

        elif cm_args.job_grouping == 'integration':
            print "submitGroups(): Submitting integration ..."
            script = combineMemberCmds(command_sets, len(cm_args.members))

            n_cores = nx * ny * len(cm_args.members)
            pid = submit(cm_args, batch, "integ", prologue + script, wall_time, n_cores, write_batch=True)

            if pid > 0:
                pids = [ pid ]
                submitted = True
            else:
                errored = True

    if submitted and cm_args.submit:
        batch.monitorQueue(pids)
    if errored:
        submitted = None
    return submitted

def submit(cm_args, batch, name, command_lines, wall_time, n_cores, hybrid=False, write_batch=False):
    n_nodes = batch.getNNodes(n_cores)
    n_mpi = n_nodes if hybrid else n_cores

    debug_path = getPaths(cm_args.base_path, cm_args.job_name, debug=True)
    job_key = "%s-%s" % (name, cm_args.job_name)
    file_text = batch.genSubmission(command_lines, jobname=job_key, debugfile="%s/%s.output" % (debug_path, job_key), nmpi=n_mpi, nnodes=n_nodes, queue='normal', timereq=wall_time + ":00")
    if write_batch:
        file = open("%s.csh" % job_key, 'w')
        file.write(file_text)
        file.close()

    if cm_args.submit:
        pid = batch.submit(file_text)
    else:
        pid = None
        print "I would submit %s here ..." % job_key
    return pid

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--n-ens', dest='n_ens_members', default=4, type=int)
    ap.add_argument('--members', dest='members', nargs='+', default=[], type=int)
    ap.add_argument('--base-path', dest='base_path', default=os.getcwd())
    ap.add_argument('--job-name', dest='job_name', default="run_osse_test")
    ap.add_argument('--mpi-config', dest='mpi_config', nargs=2, default=(3, 4), type=int)
    ap.add_argument('--algorithm', dest='algorithm', choices=['ensrf', '4densrf'], default='ensrf')

    ap.add_argument('--ens-start', dest='t_ens_start', default=1200, type=int)
    ap.add_argument('--ens-end', dest='t_ens_end', default=1500, type=int)
    ap.add_argument('--assim-step', dest='dt_assim_step', default=300, type=int)
    ap.add_argument('--ens-step', dest='dt_ens_step', default=300, type=int)

    ap.add_argument('--subset-ic', dest='subset', action='store_true')
    ap.add_argument('--bound-cond', dest='icbc', default=None)
    ap.add_argument('--free-forecast', dest='free_forecast', action='store_true')
    ap.add_argument('--covariance-inflation', dest='cov_infl', nargs='+', default=["mult=1.1"])

    ap.add_argument('--arps-template', dest='arps_template', default='arps.input')
    ap.add_argument('--arpsenkf-template', dest='arpsenkf_template', default='arpsenkf.input')
    ap.add_argument('--arpsenkfic-template', dest='arpsenkfic_template', default='arpsenkfic.input')
    ap.add_argument('--arpsintrp-template', dest='arpsintrp_template', default='arpsintrp.input')

    ap.add_argument('--assim-radar', dest='radflags')
    ap.add_argument('--assim-sndg', dest='sndgflags', default='yes')
    ap.add_argument('--assim-prof', dest='profflags', default='yes')
    ap.add_argument('--assim-surf', dest='surfflags', default='yes')

    ap.add_argument('--init-fcst-req', dest='init_fcst_req', default='0:40')
    ap.add_argument('--fcst-req', dest='fcst_req', default='0:20')
    ap.add_argument('--assim-on-req', dest='assim_on_req', default='1:00')
    ap.add_argument('--assim-off-req', dest='assim_off_req', default='0:45')

    ap.add_argument('--chunk-size', dest='chunk_size', default=1200, type=int)
    ap.add_argument('--join-files', dest='split_files', action='store_false')
    ap.add_argument('--split-init', dest='split_init', choices=['auto', 'yes', 'no'], default='auto') # Whether or not the initialization is split.
    ap.add_argument('--job-grouping', dest='job_grouping', choices=['none', 'integration', 'cycle', 'experiment'], default='none')

    ap.add_argument('--no-submit', dest='submit', action='store_false')
    ap.add_argument('--restart', dest='restart', action='store_true')
    ap.add_argument('--debug', dest='debug', action='store_true')

    args = ap.parse_args()
    batch = Batch.autoDetectMachine()

    work_path, input_path, debug_path, boundary_path, hx_path =\
        getPaths(args.base_path, args.job_name, work=True, input=True, debug=True, boundary=True, hx=True)

    if not os.path.exists(work_path):
        os.mkdir(work_path, 0755)
    if not os.path.exists(hx_path):
        os.mkdir(hx_path, 0755)

    member_list = [ m - 1 for m in args.members ]
    if member_list == []: member_list = range(args.n_ens_members)
    args.members = member_list

    if args.split_files:
        for n_ens in args.members:
            try:
                os.mkdir("%s/EN%03d" % (work_path, n_ens + 1), 0755)
                os.mkdir("%s/ENF%03d" % (work_path, n_ens + 1), 0755)
            except OSError:
                pass

    nproc_x, nproc_y = args.mpi_config

    prologue_start = [
        "echo \"\" > %s/ena%s-%s.output" % (prettyPath(debug_path, args.base_path), '%(ens)03d', args.job_name),
        "",
    ]

    prologue_lines = [
        "set base=%s" % args.base_path, "cd $base", ""
    ]

    cyc_command_lines = doForEnsemble(prologue_start, member_list)
    command_lines = defaultdict(lambda: defaultdict(list))

    joined = 0 if args.split_files else 1
    editNamelistFile("%s/%s" % (args.base_path, args.arps_template), "%s/arps.input" % input_path,
        nproc_x=nproc_x, nproc_y=nproc_y,
        dmp_out_joined=joined,
        inisplited=3 * (1 - joined),
        sfcdat=3,
        dirname="%s/" % work_path
    )

    do_first_integration = True
    ens_chunk_start = args.t_ens_start

    if not args.free_forecast and args.chunk_size > args.dt_assim_step:
        args.chunk_size = args.dt_assim_step

    if args.dt_ens_step > args.chunk_size:
        args.dt_ens_step = args.chunk_size
        print "Warning: resetting dt_ens_step to be chunk_size (%d)" % args.dt_ens_step

    if args.job_grouping in [ 'cycle', 'experiment' ] and args.chunk_size < args.dt_assim_step:
        args.chunk_size = args.dt_assim_step
        print "Warning: resetting chunk size to be dt_assim step (%d) for job grouping '%s'" % (args.dt_assim_step, args.job_grouping)

    if not args.free_forecast:
        exec open("%s/%s" % (args.base_path, args.radflags), 'r') in locals() # Get the radar assimilation flags from the file
    args.exp_start = args.t_ens_start

    # Copy the configuration information to the working directory, so we'll **ALWAYS HAVE IT IF WE NEED TO GO BACK AND LOOK AT IT**
    config_files = [ "%s/%s" % (args.base_path, f) for f in [ args.arps_template, args.arpsenkf_template, args.arpsenkfic_template ] ]
    config_files.extend(['run_experiment.py', 'run_experiment.csh'])

    for file in config_files:
        subprocess.Popen(['cp', file, "%s/." % work_path])

    if args.restart:
        for t_ens in xrange(args.t_ens_start, args.t_ens_end + args.dt_assim_step, args.dt_ens_step):

            if args.split_files and args.split_init != 'no':
                ena_exist = [ os.path.exists("%s/EN%03d/ena%03d.hdf%06d_001001" % (work_path, n_ens + 1, n_ens + 1, t_ens)) for n_ens in member_list ]
                enf_exist = [ os.path.exists("%s/ENF%03d/enf%03d.hdf%06d_001001" % (work_path, n_ens + 1, n_ens + 1, t_ens)) for n_ens in member_list ]
            else:
                ena_exist = [ os.path.exists("%s/ena%03d.hdf%06d" % (work_path, n_ens + 1, t_ens)) for n_ens in member_list ]
                enf_exist = [ os.path.exists("%s/enf%03d.hdf%06d" % (work_path, n_ens + 1, t_ens)) for n_ens in member_list ]

            all_ena_exist = all(ena_exist)
            all_enf_exist = all(enf_exist)

            if all_ena_exist:
                args.t_ens_start = t_ens
                ens_chunk_start = t_ens
                do_first_integration = True
            elif all_enf_exist and not all_ena_exist:
                args.t_ens_start = t_ens - args.dt_assim_step
                do_first_integration = False
            elif all_ena_exist and args.free_forecast:
                args.t_ens_start = t_ens
                ens_chunk_start = t_ens
                do_first_integration = True

            for t_chunk in xrange(t_ens, t_ens + args.dt_assim_step, args.chunk_size):
                if args.split_files and args.split_init != 'no':
                    ena_exist = [ os.path.exists("%s/EN%03d/ena%03d.hdf%06d_001001" % (work_path, n_ens + 1, n_ens + 1, t_chunk)) for n_ens in member_list ]
                else:
                    ena_exist = [ os.path.exists("%s/ena%03d.hdf%06d" % (work_path, n_ens + 1, t_chunk)) for n_ens in member_list ]

                if all(ena_exist):
                    ens_chunk_start = t_chunk

        if do_first_integration:
            print "Restarting from time %d (with integration) ..." % (args.t_ens_start)
        else:
            print "Restarting from time %d (no integration) ..." % (args.t_ens_start + args.dt_assim_step)

        if args.subset and args.t_ens_start == args.exp_start and do_first_integration:
            print "Subset the boundary conditions ..."
            appendCommands(cyc_command_lines,
                generateDomainSubset(args, batch, args.icbc, args.t_ens_start, args.t_ens_end, args.dt_ens_step, perturb_ic=False, copy_ic=False)
            )

            command = "cp %s/%s.hdfgrdbas %s/%s.hdfgrdbas" % (prettyPath(boundary_path, args.base_path), 'ena%(ens)03d', prettyPath(work_path, args.base_path), 'enf%(ens)03d')
            appendCommands(cyc_command_lines,
                doForEnsemble(command, member_list)
            )
    else:
        print "New experiment ..."

#       if args.split_files:
#           command = "mkdir %s/%s ; mkdir %s/%s" % (prettyPath(work_path, args.base_path), 'EN%(ens)03d', prettyPath(work_path, args.base_path), 'ENF%(ens)03d')
#           appendCommands(cyc_command_lines, 
#               doForEnsemble([ command, "" ], member_list)
#           )

        if args.subset:
            print "Subset and perturb the domain ..."
            appendCommands(cyc_command_lines,
                generateDomainSubset(args, batch, args.icbc, args.t_ens_start, args.t_ens_end, args.dt_ens_step, perturb_ic=True)
            )

            command = "cp %s/%s.hdfgrdbas %s/%s.hdfgrdbas" % (prettyPath(boundary_path, args.base_path), 'ena%(ens)03d', prettyPath(work_path, args.base_path), 'enf%(ens)03d')
            appendCommands(cyc_command_lines, 
                doForEnsemble(command, member_list)
            )

        else:
            print "Generate random initial conditions ..."
            appendCommands(cyc_command_lines, 
                generateEnsemblePerturbations(args, batch, args.t_ens_start)
            )

            command = "cp %s/%s.hdfgrdbas %s/%s.hdfgrdbas" % (prettyPath(work_path, args.base_path), 'ena%(ens)03d', prettyPath(work_path, args.base_path), 'enf%(ens)03d')
            appendCommands(cyc_command_lines, 
                doForEnsemble([ command, "" ], member_list)
            )

    command_lines[args.t_ens_start] = cyc_command_lines

    if args.free_forecast:
        if args.dt_assim_step < (args.t_ens_end - args.t_ens_start):
            args.dt_assim_step = args.t_ens_end - args.t_ens_start
            print "Warning: resetting dt_assim_step to be t_ens_end - t_ens_start (%d) for a free forecast." % args.dt_assim_step

    for t_ens in xrange(args.t_ens_start, args.t_ens_end, args.dt_assim_step):
        print "Generating timestep %d ..." % t_ens

        start_time = t_ens
        end_time = t_ens + args.dt_assim_step

        if do_first_integration or t_ens > args.t_ens_start:
            n_chunks = int(ceil(float(end_time - start_time) / args.chunk_size))
            n_chunk_start = 0
            if start_time == args.t_ens_start:
                n_chunk_start = (ens_chunk_start - start_time) / args.chunk_size
                start_time = ens_chunk_start

            for n_chunk, t_chunk in enumerate(range(start_time, end_time, args.chunk_size)):
                print "Generating ensemble integration for timestep %d (chunk %d of %d) ..." % (t_ens, n_chunk + n_chunk_start + 1, n_chunks)

                command = [ "setenv OMP_NUM_THREADS 1", "" ]
                appendCommands(command_lines[t_chunk],
                    doForEnsemble(command, member_list)
                )

                chunk_start = t_chunk
                chunk_end = t_chunk + args.chunk_size
                if chunk_end > end_time:
                    chunk_end = end_time

                which_split = 'neither'
                if args.split_files:
                    if chunk_start == args.exp_start:
                        # For the first chunk
                        if args.restart:
                            # We're restarting
                            if args.split_init == 'auto' or args.split_init == 'yes':
                                which_split = 'both'
                            elif args.split_init == 'no':
                                which_split = 'dump'
                        else:
                            # No restart
                            if args.split_init == 'auto' or args.split_init == 'no':
                                which_split = 'dump'
                            elif args.split_init == 'yes':
                                which_split = 'both'
                    else:
                        # Everything after the first chunk
                        which_split = 'both'

                if args.algorithm == '4densrf':
                    # ARPS for the 4DEnSRF wants an EnKF input file, too.
                    generateEnKFAssimilation(args, batch, chunk_end, radar_data_flag[end_time])

                appendCommands(command_lines[t_chunk],
                    generateEnsembleIntegration(args, batch, chunk_start, chunk_end, args.dt_ens_step, split_files=which_split, move_for_assim=(chunk_end == end_time and not args.free_forecast))
                )

                if args.split_files and t_chunk == args.t_ens_start and not args.free_forecast:
                    command = doForMPIConfig("cp %s/%s/%s.hdfgrdbas_%s %s/%s/%s.hdfgrdbas_%s" % (prettyPath(work_path, args.base_path), 'ENF%(ens)s', 'enf%(ens)s', '%(x_proc)03d%(y_proc)03d', 
                        prettyPath(work_path, args.base_path), 'EN%(ens)s', 'ena%(ens)s', '%(x_proc)03d%(y_proc)03d'), args.mpi_config)

                    appendCommands(command_lines[t_chunk],
                        doForEnsemble(command, member_list)
                    )

                submitted = submitGroups(args, batch, command_lines, prologue_lines)
                if submitted is None:
                    sys.exit()
                elif submitted:
                    command_lines.clear()

        if not args.free_forecast:
            print "Generating assimilation for timestep %d ..." % t_ens
            assimilation_lines = [ "setenv OMP_NUM_THREADS %d" % batch.getNCoresPerNode(), "" ]
            assimilation_lines.extend(
                generateEnKFAssimilation(args, batch, end_time, radar_data_flag[end_time])
            )
            command_lines[t_ens]['enkf'] = assimilation_lines
            submitted = submitGroups(args, batch, command_lines, prologue_lines)
            if submitted is None:
                sys.exit()
            elif submitted:
                command_lines.clear()
    print "Experiment complete!"
    return

if __name__ == "__main__":
    main()
