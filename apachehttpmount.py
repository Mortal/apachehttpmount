#!/usr/bin/env python

from __future__ import with_statement

import os
import re
import pwd
import sys
import stat
import errno
import datetime
import collections

from fuse import FUSE, FuseOSError, Operations
import requests


DEBUG = 0


LISTING = b'''<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
 <head>
  <title>Index of '''


class Http(Operations):
    def __init__(self, root):
        assert root.startswith('http')
        self.root = root
        self._cache = {}
        nobody = pwd.getpwnam('nobody')
        self.uid = nobody.pw_uid
        self.gid = nobody.pw_gid

    # Helpers
    # =======

    def _full_path(self, partial):
        path = os.path.join(self.root, partial.lstrip('/'))
        return path

    def _get(self, path):
        try:
            result = self._cache[path]
        except KeyError:
            pass
        else:
            if result is None:
                raise FileNotFoundError(path)
            if DEBUG: print(path, "Cached", len(result))
            return result
        full_path = self._full_path(path)
        print('GET ' + full_path)
        response = requests.get(full_path)
        if response.status_code == 404:
            self._cache[path] = None
            raise FileNotFoundError(path)
        elif response.status_code != 200:
            print("Response code %s" % response.status_code)
            raise FuseOSError(errno.EIO)
        if response.content.startswith(LISTING):
            self._cache[path] = self._parse_listing(response.text)
            if DEBUG: print(path, "Entries %s" % len(self._cache[path]))
        else:
            self._cache[path] = response.content
            if DEBUG: print(path, "Size %s" % len(response.content))
            pardir = self._get(os.path.dirname(path))
            basename = os.path.basename(path)
            icon, size_, mtime_dt = pardir[basename]
            pardir[basename] = icon, len(response.content), mtime_dt
        return self._cache[path]

    def _parse_listing(self, text):
        start = '<pre>'
        end = '</pre>'
        assert text.count(start) == text.count(end) == 1
        listing = text[text.index(start)+len(start):text.index(end)]
        lines = listing.splitlines()
        header = lines[0]
        assert header.count('Name') == header.count('Last modified') == 1
        entries = lines[1:-1]
        assert lines[-1] == '<hr>'
        result = collections.OrderedDict()
        for str_entry in entries:
            pattern = (r'^<img src="[^"]+" alt="\[(?P<icon>[^"]+)\]"> ' +
                       r'<a href="(?P<url>[^"]+)">(?P<name>[^<]+)</a>\s*' +
                       r'(?P<date>\d+-\w+-\d+)\s+(?P<time>\d+:\d+)\s+' +
                       r'(?P<size>[0-9.]+|-)(?P<sizeunit>\w?)\s*$')
            mo = re.match(pattern, str_entry)
            assert mo, str_entry
            icon, url, name = mo.group('icon', 'url', 'name')
            assert url == name, (url, name)
            isdir = name.endswith('/')
            name = name.rstrip('/')
            assert isdir == (icon == 'DIR')
            str_date, str_time = mo.group('date', 'time')
            mtime = datetime.datetime.strptime(
                ' '.join((str_date, str_time)),
                '%d-%b-%Y %H:%M')
            str_size, str_sizeunit = mo.group('size', 'sizeunit')
            sizes = [''] + list('KMGTP')
            size = (0 if str_size == '-' else
                    int((0.1 + float(str_size)) * 2 ** (10*sizes.index(str_sizeunit))))

            result[name] = (icon, size, mtime)
        return result

    def _getdent(self, path):
        if path == '/':
            return ('DIR', 0, datetime.datetime.fromtimestamp(0))
        dirname = os.path.dirname(path)
        if DEBUG: print("Get dent", path, dirname)
        try:
            dir = self._get(dirname)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        if not isinstance(dir, collections.OrderedDict):
            raise FuseOSError(errno.ENOENT)
        try:
            return dir[os.path.basename(path)]
        except KeyError:
            raise FileNotFoundError(path) from None

    # Filesystem methods
    # ==================

    def trace(fn):
        def wrapped(self, path, *args):
            res = '???'
            try:
                result = fn(self, path, *args)
            except FuseOSError as exn:
                res = '%s' % (errno.errorcode[exn.args[0]],)
                raise
            except Exception as exn:
                res = repr(exn)
                raise
            else:
                res = 'OK'
            finally:
                if DEBUG: print("%s(%s) -> %s" % (fn.__name__, path, res))
            return result
        return wrapped

    @trace
    def access(self, path, mode):
        if DEBUG: print(path, oct(mode))
        if mode & os.W_OK:
            raise FuseOSError(errno.EROFS)
        try:
            icon, size, mtime_dt = self._getdent(path)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        if mode & os.X_OK and icon != 'DIR':
            raise FuseOSError(errno.EACCES)

    @trace
    def chmod(self, path, mode):
        raise FuseOSError(errno.EROFS)

    @trace
    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EROFS)

    @trace
    def getattr(self, path, fh=None):
        try:
            icon, size, mtime_dt = self._getdent(path)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        mtime = mtime_dt.timestamp()
        mode = 0o555 if icon == 'DIR' else 0o444
        if icon == 'DIR':
            mode |= stat.S_IFDIR
        else:
            mode |= stat.S_IFREG
        return {
            'st_atime': mtime,
            'st_ctime': mtime,
            'st_gid': self.gid,
            'st_mode': mode,
            'st_mtime': mtime,
            'st_nlink': 1 + (icon == 'DIR'),
            'st_size': size,
            'st_uid': self.uid,
        }

    @trace
    def readdir(self, path, fh):
        entry = self._get(path)
        if not isinstance(entry, collections.OrderedDict):
            raise FuseOSError(errno.ENOTDIR)
        dirents = ['.', '..'] + list(entry.keys())
        return dirents

    @trace
    def readlink(self, path):
        raise NotImplementedError

    @trace
    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EPERM)

    @trace
    def rmdir(self, path):
        raise FuseOSError(errno.EROFS)

    @trace
    def mkdir(self, path, mode):
        raise FuseOSError(errno.EROFS)

    @trace
    def statfs(self, path):
        return dict(f_bsize=4096, f_frsize=4096, f_blocks=4114392,
                    f_bfree=4114392, f_bavail=4114392, f_files=4114392,
                    f_ffree=4113860, f_favail=4113860, f_flag=4098,
                    f_namemax=255)

    @trace
    def unlink(self, path):
        raise FuseOSError(errno.EROFS)

    @trace
    def symlink(self, name, target):
        raise FuseOSError(errno.EROFS)

    @trace
    def rename(self, old, new):
        raise FuseOSError(errno.EROFS)

    @trace
    def link(self, target, name):
        raise FuseOSError(errno.EROFS)

    @trace
    def utimens(self, path, times=None):
        raise FuseOSError(errno.EROFS)

    # File methods
    # ============

    @trace
    def open(self, path, flags):
        O_LARGEFILE = 0o100000
        flags &= ~O_LARGEFILE
        if flags != os.O_RDONLY:
            if DEBUG: print(oct(flags))
            raise FuseOSError(errno.EROFS)
        try:
            result = self._get(path)
        except FileNotFoundError:
            raise FuseOSError(errno.ENOENT)
        if isinstance(result, collections.OrderedDict):
            raise FuseOSError(errno.EISDIR)
        self.fd += 1
        return self.fd

    fd = 0

    @trace
    def create(self, path, mode, fi=None):
        raise FuseOSError(errno.EROFS)

    @trace
    def read(self, path, length, offset, fh):
        if DEBUG: print(path, length, offset)
        result = self._get(path)
        return result[offset:offset+length]

    @trace
    def write(self, path, buf, offset, fh):
        raise FuseOSError(errno.EROFS)

    @trace
    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EROFS)

    @trace
    def flush(self, path, fh):
        pass

    @trace
    def release(self, path, fh):
        pass

    @trace
    def fsync(self, path, fdatasync, fh):
        pass


def main(mountpoint, root):
    FUSE(Http(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])
