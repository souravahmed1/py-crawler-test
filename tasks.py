"""
Task definitions
These are kept in a separate file to avoid circular references

author: QuikFynd
"""

from time import time
from collections import namedtuple, Mapping

# http://stackoverflow.com/questions/11351032/named-tuple-and-optional-keyword-arguments
def _namedtuple_with_defaults(typename, field_names, default_values=()):
    T = namedtuple(typename, field_names)
    T.__new__.__defaults__ = (None,) * len(T._fields)
    if isinstance(default_values, Mapping):
        prototype = T(**default_values)
    else:
        prototype = T(*default_values)
    T.__new__.__defaults__ = tuple(prototype)
    T.__module__ = __name__
    return T

# Named tuple for ScanEntry
ScanEntry = _namedtuple_with_defaults('ScanEntry', 'scandir_path path name is_symlink is_dir st_size st_ctime st_mtime st_uid st_gid st_file_attributes tags downloadhandle permissions content_modified permissions_modified')


FilePermissionsEntry = _namedtuple_with_defaults('FilePermissionsEntry', 'file_id entity_name')

def create(taskname, **kwargs):
    # Given a task name, gets the namedtuple that contains attributes of
    # that need to be filled in for that task
    if 'userid' not in kwargs:
        kwargs['userid'] = None
    if 'dbtoken' not in kwargs:
        kwargs['dbtoken'] = ''
    if 'context' not in kwargs:
        kwargs['context'] = None
    if 'result' not in kwargs:
        kwargs['result'] = None
    if 'eventtime' not in kwargs:
        kwargs['eventtime'] = time()
    kwargs['name'] = taskname
    return Task(**kwargs)
