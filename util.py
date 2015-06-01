
def getPaths(base_path, job_name, **kwargs):
    paths = []
    default_paths = {
        'work':"%s/%s/" % (base_path, job_name),
        'input':"%s/input/" % base_path,
        'debug':"%s/debug/" % base_path,
        'batch':"%s/batch/" % base_path,
        'boundary':"%s/boundary/" % base_path,
        'hx':"%s/%s/" % (base_path, job_name),
    }
    for p in ['work', 'input', 'debug', 'batch', 'boundary', 'hx']:
        if p in kwargs and kwargs[p]:
            paths.append(default_paths[p])

    if len(paths) == 1: paths = paths[0]
    return paths

def getEnsDirectory(n_ens, fcst):
    base_state = 'f' if fcst else 'a'
    dir_state = 'F' if fcst else ''

    ens_base = "en%s%03d" % (base_state, n_ens)
    ens_dir = "EN%s%03d" % (dir_state, n_ens)
    return ens_dir, ens_base
