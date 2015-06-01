
from collections import OrderedDict
import re

class Namelist(object):
    def __init__(self, file_name, expand_lists=False):
        self._file_name = file_name
        self._expand_lists = expand_lists
        self._parse(file_name)
        return

    def write(self, file_name=None):
        if not file_name:
            file_name = self._file_name

        file = open(file_name, 'w')
        file.write(self._comments.pop(0))

        cur_group = ""
        for (group, var), val in self.iter():
            if group != cur_group:
                if cur_group != "":
                    file.write("/\n\n")
                    file.write(self._comments.pop(0))

                file.write("\n&%s\n" % group)
                cur_group = group

            file.write("  %s = %s,\n" % (var, Namelist._val2nl(val, self._expand_lists)))

        file.write("/\n")
        file.close()
        return

    def setall(self, other):
        for key, val in other.iter():
            self[key] = val
        return

    def __getitem__(self, var):
        group, group_var = self._index2tuple(var)
        return self._names[group][group_var]

    def __setitem__(self, var, value):
        group, group_var = self._index2tuple(var)
        self._names[group][group_var] = value
        return

    def __delitem__(self, var):
        group, group_var = self._index2tuple(var)
        del self._names[group][group_var]
        return 

    def iter(self):
        for group_name, group in self._names.iteritems():
            for var_name, val in group.iteritems():
                yield (group_name, var_name), val
        return

    def _parse(self, file_name):
        self._comments = [ "" ]
        self._names = OrderedDict()

        def_group_name = "__nameless__"
        group_name = def_group_name

        file = open(file_name, 'r')
        for raw_line in file:
            line = raw_line.strip()
            if line == "":
                continue

            elif line[0] == "!":
                self._comments[-1] += raw_line

            elif line[0] == "&":
                group_name = line[1:]
                self._names[group_name] = OrderedDict()
                 
            elif line[0] == "/":
                group_name = def_group_name
                self._comments.append("")

            else:
                for var, val in Namelist._parse_line(line):
                    val = [ Namelist._nl2val(v.strip()) for v in val if v.strip() != "" ]
                    if len(val) == 1: val = val[0]
                    self._names[group_name][var] = val

        file.close()

        for group_name, group_vars in self._names.iteritems():
            ary_vars = [ v for v in group_vars.iterkeys() if "(" in v ]
            if len(ary_vars) > 0:
                ary_grouped = list(set([ v[:v.index("(")] for v in ary_vars ]))
#               for ag in ary_grouped:
                    

#       print self._names

        return

    def _index2tuple(self, index):
        tup = None
        if type(index) in [ list, tuple ]:
            if len(index) == 2:
                tup = tuple(index)
            else:
                raise ValueError("Incorrect number of indices for a Namelist object.")
        else:
            candidates = [ n for n, g in self._names.iteritems() for i in g.iterkeys() if i == index ]

            if len(candidates) == 0:
                raise KeyError("Variable '%s' not found in namelist file" % index)
            elif len(candidates) == 1:
                tup = candidates[0], index
            else:
                raise KeyError("Variable '%s' is in multiple groups: pass in a tuple ('group', 'variable') to be more specific" % index)
        return tup

    @staticmethod
    def _parse_line(line):
        match = re.findall("([\w][^\s]*?)[\s]*=[\s]*((?:(?:'.*?'|[-\d.]+[eE]?(?:[-\d]+)?)[\s]*,?[\s]*)+)", line)
#       if match != []: pass # print match
#       else: print line

        match = [ (k, v.split(",")) for k, v in match ]
        return match

    @staticmethod
    def _nl2val(val):
        if val[0] == "'" and val[-1] == "'":
            return val[1:-1]
        elif val[0] == "." and val[-1] == ".":
            return val.lower() == ".true."
        else:
            try:
                return int(val)
            except ValueError:
                return float(val)
        return

    @staticmethod
    def _val2nl(val, expand_arrays):
        if type(val) in [ list, tuple ]:
            vals = [ Namelist._val2nl(v, expand_arrays) for v in val ]
            if expand_arrays:
                return ", ".join(vals)
            else:
                return vals
        elif type(val) == str:
            return "'%s'" % val
        elif type(val) == bool:
            return ".%s." % (str(val).lower())
        else:
            return str(val)
        return

if __name__ == "__main__":
    nl = Namelist("/scratch/01479/tsupine/05June2009/arps.1km.input")
#   nl = Namelist("/scratch/01479/tsupine/24May2011/arpsenkf.3km.input")

    nl.write(file_name="test.input")

