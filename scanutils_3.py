"""
Scan File System task
Applicable to all use cases

author: QuikFynd

"""


from functools import wraps
from time import time

def timed(f):
  @wraps(f)
  def wrapper(*args, **kwds):
    start = time()
    result = f(*args, **kwds)
    elapsed = time() - start
    print ("{} took {} time to finish".format(f.__name__, elapsed))
    return result
  return wrapper


# ==============================
import os
import sys
import scandir

# Named tuple for result that is posted to complete_queue
from tasks import ScanEntry

is_windows = sys.platform == 'win32'
if is_windows:
    import win32con

def _skip_path(path):
    if path.startswith("."):
        return True
    else:
        return False

def _get_scan_entry(top):
    stat = os.stat(top)
    top_entry = ScanEntry(scandir_path=os.path.dirname(top),
        path=top,
        name=os.path.basename(top),
        is_symlink=False,
        is_dir=True,
        st_size=stat.st_size,
        st_ctime=stat.st_ctime,
        st_mtime=stat.st_mtime,
        st_uid=stat.st_uid,
        st_gid=stat.st_gid,
        st_file_attributes=None,
        tags=None, downloadhandle=None,
        permissions=None)

    return top_entry

def _walk(top, topdown=True, onerror=None, followlinks=False):
    """Like Python 3.5's implementation of os.walk() -- faster than
    the pre-Python 3.5 version as it uses scandir() internally.
    """
    dirs = {}
    nondirs = {}

    # We may not have read permission for top, in which case we can't
    # get a list of the files the directory contains.  os.walk
    # always suppressed the exception then, rather than blow up for a
    # minor reason when (say) a thousand readable directories are still
    # left to visit.  That logic is copied here.
    try:
        scandir_it = scandir.scandir(top)
    except OSError as error:
        if onerror is not None:
            onerror(error)

        # In case of error, we return as if we were not
        # able to traverse that directory
        visited = {}
        try:
            top_entry = _get_scan_entry(top)
            visited = {top:top_entry}
        except OSError as e:
            pass

        return(visited,dirs,nondirs)

    while True:
        try:
            try:
                entry = next(scandir_it)
            except StopIteration:
                break
        except OSError as error:
            if onerror is not None:
                onerror(error)
            # In case of error, we return as if we were not
            # able to traverse that directory
            try:
                top_entry = _get_scan_entry(top)
            except OSError as e:
                pass

            return({top:top_entry},dirs,nondirs)

        try:
            is_dir = entry.is_dir()
        except OSError:
            # If is_dir() raises an OSError, consider that the entry is not
            # a directory, same behaviour than os.path.isdir().
            is_dir = False

        try:
            stat = entry.stat()
            if is_windows:
                st_file_attributes = stat.st_file_attributes
            else:
                st_file_attributes = None
        except OSError as e:
            continue

        # We skip certain type of entries. For Windows, we use
        # win32con FILE_ATTRIBUTES
        skip_entry = entry.is_symlink() | _skip_path(entry.name)
        if is_windows:
            """
            skip_entry = st_file_attributes & win32con.FILE_ATTRIBUTE_HIDDEN | \
                         st_file_attributes & win32con.FILE_ATTRIBUTE_REPARSE_POINT | \
                         st_file_attributes & win32con.FILE_ATTRIBUTE_SYSTEM | \
                         st_file_attributes & win32con.FILE_ATTRIBUTE_TEMPORARY | \
                         st_file_attributes & win32con.FILE_ATTRIBUTE_NOT_CONTENT_INDEXED | \
                         skip_entry
            """

        if not skip_entry:
            scan_entry = ScanEntry(scandir_path=top,
                path=entry.path,
                name=entry.name,
                is_symlink=entry.is_symlink(),
                is_dir=is_dir,
                st_size=stat.st_size,
                st_ctime=stat.st_ctime,
                st_mtime=stat.st_mtime,
                st_uid=stat.st_uid,
                st_gid=stat.st_gid,
                st_file_attributes=st_file_attributes,
                tags=None, downloadhandle=None,
                permissions=None)

            if is_dir:
                dirs[entry.path] = scan_entry
            else:
                nondirs[entry.path] = scan_entry

    # We also return scan_entry for top
    visited = {}
    try:
        top_entry = _get_scan_entry(top)
        visited = {top:top_entry}
    except OSError as e:
        pass

    return(visited,dirs,nondirs)

if sys.platform == 'passthru':
    walk = _walk
else:
    # Fix for broken unicode handling on Windows on Python 2.x, see:
    # https://github.com/benhoyt/scandir/issues/54
    file_system_encoding = sys.getfilesystemencoding()

    def walk(top, topdown=True, onerror=None, followlinks=False, limitToOne=False):
        try:
            if isinstance(top, bytes):
                top = top.decode(file_system_encoding)

            visited = {}
            dirs = {}
            nondirs = {}

            if isinstance(top, dict):
                dirs = top
                (_top, entry) =  dirs.popitem()
            else:
                _top = top
            while True:

                if not os.path.isdir(_top):
                    # Directory we want to scan doesn't exist, so
                    # lets try next one
                    if len(dirs) == 0:
                        break
                    else:
                        (_top, entry) = dirs.popitem()
                        continue

                (_visited, _dirs, _nondirs) = _walk(_top, topdown, onerror, followlinks)

                visited.update(_visited)
                dirs.update(_dirs)
                nondirs.update(_nondirs)

                # If we only want to look at one folder, break
                if limitToOne:
                    break

                # if we have collected enough elements to return, then break out of loop
                #if len(dirs) + len(nondirs) >= 4096:
                if len(nondirs) >= 128:
                    break

                # if we have collected enough directories, break
                # we never want to collect more than 100 directories to stay within limit of
                # parameters of our DB query
                if len(visited) >= 64:
                    break

                # ok, we haven't collected enough items, so check if we have any more
                # directories to traverse in this path, if yes, then process one more
                # directory, otherwise break
                if len(dirs) == 0:
                    break

                # ok, so we can traverse one more directory, let's do so
                (_top, entry) = dirs.popitem()

            return (visited, dirs, nondirs)
        except Exception as e:
            pass


def traverse(start):
    visited, dirs, nondirs = walk(start, topdown=True, followlinks=True, limitToOne=False)
    for directory in list(dirs.keys()):
        if not visited.get(directory):
            traverse(directory)

@timed
def main():
    start = "/Users/inno/Documents/mock-api/faq-mock"
    traverse(start)


if __name__ == "__main__":
    # visited, dirs, nondirs = walk("/Users/inno/Documents/mock-api/events-mock", followlinks=True, topdown=True)
    # print("directories: ", dirs)
    # print("total dirs: ", len(dirs))

    # print("visited: ", visited)
    # print("total visited: ", len(visited))

    # print("nondirs: ", nondirs)
    # print("total nondirs: ", len(nondirs))
    main()
    

