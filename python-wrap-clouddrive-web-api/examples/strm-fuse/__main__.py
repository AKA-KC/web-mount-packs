#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 6)

if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter

    parser = ArgumentParser(description="""\
基于 clouddrive 和 fuse 的只读文件系统，支持罗列 strm
    1. Linux 要安装 libfuse：  https://github.com/libfuse/libfuse
    2. MacOSX 要安装 MacFUSE： https://github.com/osxfuse/osxfuse
    3. Windows 要安装 WinFsp： https://github.com/winfsp/winfsp

⏰ 由于网盘对多线程访问的限制，请停用挂载目录的显示图标预览

访问源代码：
    - https://github.com/ChenyangGao/web-mount-packs/tree/main/python-wrap-clouddrive-web-api/examples/strm-fuse

下面的选项 --ignore、--ignore-file、--strm、--strm-file 支持相同的配置语法。
    0. --strm、--strm-file 优先级高于 --ignore、--ignore-file，但前两者只针对文件（不针对目录），后两者都针对
    1. 从配置文件或字符串中，提取模式，执行模式匹配
    2. 模式匹配语法如下：
        1. 如果模式以反斜杠 \\ 开头，则跳过开头的 \\ 后，剩余的部分视为使用 gitignore 语法，对路径执行匹配（开头为 ! 时也不具有结果取反意义）
            - gitignore：https://git-scm.com/docs/gitignore#_pattern_format
        2. 如果模式以 ! 开头，则跳过开头的 ! 后，执行模式匹配，匹配成功是为失败，匹配失败是为成功，也就是结果取反
        3. 以 ! 开头的模式，优先级高于不以此开头的
        4. 如果模式以 =、^、$、:、;、,、<、>、|、~、-、% 之一开头，视为匹配文件名对应的 mimetype，否则使用 gitignore 语法，对路径执行匹配
            - https://developer.mozilla.org/en-US/docs/Web/HTTP/Basics_of_HTTP/MIME_types

            0.     跳过下面的开头字符，剩余的部分称为模式字符串
            1. =   模式字符串等于被匹配字符串
            2. ^   模式字符串匹配被匹配字符串的开头
            3. $   模式字符串匹配被匹配字符串的结尾
            4. :   被匹配字符串里有等于此模式字符串的部分
            5. ;   对被匹配字符串按空白符号(空格、\\r、\\n、\\t、\\v、\\f 等)拆分，有一个部分等于此模式字符串
            6. ,   对被匹配字符串按逗号 , 拆分，有一个部分等于此字符串
            7. <   被匹配字符串里有一个单词（非标点符号、空白符号等组成的字符串）以此模式字符串开头
            8. >   被匹配字符串里有一个单词（非标点符号、空白符号等组成的字符串）以此模式字符串结尾
            9. |   被匹配字符串里有一个单词（非标点符号、空白符号等组成的字符串）等于此模式字符串
            10. ~  模式字符串是为正则表达式，被匹配字符串的一部分匹配此正则表达式
            11. -  模式字符串是为正则表达式，被匹配字符串的整体匹配此正则表达式
            12. %  模式字符串是为通配符表达式，被匹配字符串的整体匹配此通配符表达式
""", formatter_class=RawTextHelpFormatter)
    parser.add_argument("mount_point", nargs="?", help="挂载路径")
    parser.add_argument("-o", "--origin", default="http://localhost:19798", help="clouddrive 服务器地址，默认 http://localhost:19798")
    parser.add_argument("-u", "--username", default="", help="用户名，默认为空")
    parser.add_argument("-p", "--password", default="", help="密码，默认为空")
    parser.add_argument("--ignore", help="""\
接受配置，忽略其中罗列的文件和文件夹。
如果有多个，用空格分隔（如果文件名中包含空格，请用 \\ 转义）。""")
    parser.add_argument("--ignore-file", help="""\
接受一个配置文件路径，忽略其中罗列的文件和文件夹。
一行写一个配置，支持 # 开头作为注释。""")
    parser.add_argument("--strm", help="""\
接受配置，把罗列的文件显示为带 .strm 后缀的文件，打开后是链接。
优先级高于 --ignore 和 --ignore-file，如果有多个，用空格分隔（如果文件名中包含空格，请用 \\ 转义）。""")
    parser.add_argument("--strm-file", help="""\
接受一个配置文件路径，把罗列的文件显示为带 .strm 后缀的文件，打开后是链接。
优先级高于 --ignore 和 --ignore-file，如果有多个，用空格分隔（如果文件名中包含空格，请用 \\ 转义）。""")
    parser.add_argument("-m", "--max-read-threads", type=int, default=1, help="单个文件的最大读取线程数，如果小于 0 就是无限，默认值 1")
    parser.add_argument("-z", "--zipfile-as-dir", action="store_true", help="为 .zip 文件生成一个同名 + .d 后缀的文件夹")
    parser.add_argument("-v", "--version", action="store_true", help="输出版本号")
    parser.add_argument("-d", "--debug", action="store_true", help="调试模式，输出更多信息")
    parser.add_argument("-l", "--log-level", default=0, help=f"指定日志级别，可以是数字或名称，不传此参数则不输出日志，默认值: 0 (NOTSET)")
    parser.add_argument("-b", "--background", action="store_true", help="后台运行")
    parser.add_argument("-s", "--nothreads", action="store_true", help="不用多线程")
    parser.add_argument("--allow-other", action="store_true", help="允许 other 用户（也即不是 user 和 group）")
    #parser.add_argument("-i", "--iosize", type=int, help="每次读取的字节数")
    args = parser.parse_args()
    if args.version:
        print(*__version__, sep=".")
        raise SystemExit
    if not args.mount_point:
        parser.parse_args(["-h"])

    from sys import version_info

    if version_info < (3, 10):
        print("python 版本过低，请升级到至少 3.10")
        raise SystemExit(1)

try:
    # pip install clouddrive
    from clouddrive import CloudDriveFileSystem
    from clouddrive.util.ignore import read_str, read_file, parse
    from clouddrive.util.file import HTTPFileReader
    # pip install types-cachetools
    from cachetools import LRUCache, TTLCache
    # pip install types-python-dateutil
    from dateutil.parser import parse as parse_datetime
    # pip install fusepy
    from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
except ImportError:
    from subprocess import run
    from sys import executable
    run([executable, "-m", "pip", "install", "-U", "clouddrive", "cachetools", "fusepy", "python-dateutil"], check=True)

    from clouddrive import CloudDriveFileSystem
    from clouddrive.util.ignore import read_str, read_file, parse
    from clouddrive.util.file import HTTPFileReader
    from cachetools import LRUCache, TTLCache
    from dateutil.parser import parse as parse_datetime
    from fuse import FUSE, FuseOSError, Operations, LoggingMixIn # type: ignore

from collections.abc import Callable, MutableMapping
from datetime import datetime
from functools import partial, update_wrapper
from errno import EISDIR, ENOENT, EIO
from itertools import count
from mimetypes import guess_type
from posixpath import basename, dirname, join as joinpath, split as splitpath
from sys import maxsize
from stat import S_IFDIR, S_IFREG
from threading import Event, Lock, Semaphore, Thread
from time import time
from types import MappingProxyType
from typing import cast, Any, IO, Optional
from weakref import WeakKeyDictionary, WeakValueDictionary
from zipfile import ZipFile, Path as ZipPath, BadZipFile

from util.log import logger


_EXTRA = MappingProxyType({"instance": __name__})


def parse_as_ts(s: Optional[str] = None, /) -> float:
    if not s:
        return 0.0
    if s.startswith("0001-01-01"):
        return 0.0
    try:
        return parse_datetime(s).timestamp()
    except:
        logger.warning("can't parse datetime: %r", s, extra=_EXTRA)
        return 0.0


def update_readdir_later(
    func=None, 
    /, 
    refresh_max_workers: int = 8, 
    refresh_min_interval: int = 10, 
):
    if func is None:
        return partial(
            update_readdir_later, 
            refresh_max_workers=refresh_max_workers, 
            refresh_min_interval=refresh_min_interval, 
        )
    event_pool: dict[Any, Event] = {}
    refresh_freq: MutableMapping = TTLCache(maxsize, ttl=refresh_min_interval)
    lock = Lock()
    sema = Semaphore(refresh_max_workers)
    def run_update(self, path, fh, /, do_refresh=True):
        with lock:
            try:
                evt = event_pool[path]
                wait_event = True
            except KeyError:
                evt = event_pool[path] = Event()
                wait_event = False
        if wait_event:
            if do_refresh:
                return
            evt.wait()
        else:
            try:
                if do_refresh:
                    with sema:
                        func(self, path, fh)
                else:
                    func(self, path, fh)
            except BaseException as e:
                self._log(
                    logging.ERROR, 
                    "can't readdir: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                    path, type(e).__qualname__, e, 
                )
                raise FuseOSError(EIO) from e
            finally:
                evt.set()
                event_pool.pop(path, None)
    def wrapper(self, /, path, fh=0):
        while True:
            try:
                cache = self.cache[path]
            except KeyError:
                pass
            else:
                try:
                    if path not in refresh_freq:
                        refresh_freq[path] = None
                        Thread(target=run_update, args=(self, path, fh)).start()
                except BaseException as e:
                    self._log(
                        logging.ERROR, 
                        "can't start new thread for path: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                        path, type(e).__qualname__, e, 
                    )
                    raise FuseOSError(EIO) from e
                finally:
                    return [".", "..", *cache]
            run_update(self, path, fh, do_refresh=False)
    return update_wrapper(wrapper, func)


# Learn: https://www.stavros.io/posts/python-fuse-filesystem/
class CloudDriveFuseOperations(LoggingMixIn, Operations):

    def __init__(
        self, 
        /, 
        origin: str = "http://localhost:19798", 
        username: str = "", 
        password: str = "", 
        cache: Optional[MutableMapping] = None, 
        predicate: Optional[Callable] = None, 
        strm_predicate: Optional[Callable] = None, 
        max_read_threads: int = 1, 
        zipfile_as_dir: bool = False, 
    ):
        self.fs = CloudDriveFileSystem.login(origin, username, password)
        if cache is None:
            cache = TTLCache(maxsize, ttl=3600)
        self.cache = cache
        self.predicate = predicate
        self.strm_predicate = strm_predicate
        self.max_read_threads = max_read_threads
        self.zipfile_as_dir = zipfile_as_dir
        self._next_fh: Callable[[], int] = count(1).__next__
        self._fh_to_file: MutableMapping[int, IO[bytes]] = TTLCache(maxsize, ttl=60)
        self._fh_to_zdir: MutableMapping[int, ZipPath] = {}
        self._path_to_file: WeakValueDictionary[tuple[int, str], IO[bytes]] = WeakValueDictionary()
        self._path_to_zfile: WeakValueDictionary[str, ZipFile] = WeakValueDictionary()
        self._file_to_release: MutableMapping[IO[bytes], None] = TTLCache(maxsize, ttl=1)
        self._log = partial(logger.log, extra={"instance": repr(self)})

    def __del__(self, /):
        try:
            cache = self._fh_to_file
            popitem = cache.popitem
            while cache:
                try:
                    _, file = popitem()
                    file.close()
                except BaseException as e:
                    self._log(
                        logging.WARNING, f"can't close file: %r  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                        file, type(e).__qualname__, e, 
                    )
            self._fh_to_zdir.clear()
            self._path_to_file.clear()
            self._path_to_zfile.clear()
            self._file_to_release.clear()
        except AttributeError:
            pass

    def getattr(self, /, path: str, fh: int = 0, _rootattr={"st_mode": S_IFDIR | 0o555}) -> dict:
        self._log(logging.DEBUG, "getattr(path=\x1b[4;34m%r\x1b[0m, fh=%r)", path, fh)
        if path == "/":
            return _rootattr
        dir_, name = splitpath(path)
        try:
            dird = self.cache[dir_]
        except KeyError:
            try:
                self.readdir(dir_)
                dird = self.cache[dir_]
            except BaseException as e:
                self._log(
                    logging.WARNING, 
                    "file not found: \x1b[4;34m%s\x1b[0m, since readdir failed: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                    path, dir_, type(e).__qualname__, e, 
                )
                raise FuseOSError(EIO) from e
        try:
            return dird[name]
        except KeyError as e:
            self._log(
                logging.ERROR, 
                "file not found: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                path, type(e).__qualname__, e, 
            )
            raise FuseOSError(ENOENT) from e

    def _open(self, path: str, start: int = 0, /) -> HTTPFileReader:
        file_size = self.getattr(path)["st_size"]
        try:
            file = cast(HTTPFileReader, self.fs.as_path(path).open("rb", start=start))
        except BaseException as e:
            self._log(
                logging.ERROR, 
                "can't open file: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                path, type(e).__qualname__, e, 
            )
            raise FuseOSError(EIO) from e
        if file.length != file_size:
            self._log(logging.ERROR, "incorrect file size: \x1b[4;34m%s\x1b[0m %s != %s", path, file.length, file_size)
            raise FuseOSError(EIO)
        return file

    # TODO: 需要优化，增强协同效率，不要加锁
    def _open_zip(self, path: str, /) -> ZipFile:
        try:
            return self._path_to_zfile[path]
        except KeyError:
            zfile = self._path_to_zfile[path] = ZipFile(self.fs.open(path, "rb"))
            self.cache[path + ".d"] = {
                zinfo.filename.rstrip("/"): dict(
                    st_uid=0, 
                    st_gid=0, 
                    st_mode=(S_IFDIR if zinfo.is_dir() else S_IFREG) | 0o555, 
                    st_nlink=1, 
                    st_size=zinfo.file_size, 
                    st_ctime=(dt := datetime(*zinfo.date_time).timestamp()), 
                    st_mtime=dt, 
                    st_atime=time(), 
                    _is_zip=True, 
                    _zip_path=path, 
                ) for zinfo in zfile.filelist
            }  
            return zfile

    def open(self, /, path: str, flags: int=0, fh: int = 0) -> int:
        self._log(logging.DEBUG, "open(path=\x1b[4;34m%r\x1b[0m, flags=%r, fh=%r)", path, flags, fh)
        attr = self.getattr(path)
        if attr.get("_loaded", False):
            return 0
        size = attr["st_size"]
        if size == 0:
            self._log(logging.ERROR, "is a directory: \x1b[4;34m%s\x1b[0m", path)
            raise FuseOSError(EISDIR)
        try:
            if not fh:
                fh = self._next_fh()
            if attr.get("_is_zip", False):
                zip_path = attr["_zip_path"]
                try:
                    file = self._path_to_file[(0, path)]
                except KeyError:
                    zfile = self._open_zip(zip_path)
                    fp = zfile.fp
                    if fp is None or fp.closed:
                        zfile.close()
                        self._path_to_zfile.pop(zip_path, None)
                        zfile = self._open_zip(zip_path)
                    try:
                        file = self._path_to_file[(0, path)] = zfile.open(path.removeprefix(zip_path + ".d/"))
                    except BadZipFile:
                        zfile.fp and zfile.fp.close()
                        zfile.close()
                        self._path_to_zfile.pop(zip_path, None)
                        zfile = self._open_zip(zip_path)
                        file = self._path_to_file[(0, path)] = zfile.open(path.removeprefix(zip_path + ".d/"))
                    attr.update(_data=file.read(2048))
                    if size <= 2048:
                        attr.update(_loaded=True)
                        file.close()
                        return 0
            else:
                threads = self.max_read_threads
                if threads <= 0:
                    file = self._open(path)
                    attr.update(_data=file.read(2048))
                    if size <= 2048:
                        attr.update(_loaded=True)
                        file.close()
                        return 0
                else:
                    try:
                        file = self._path_to_file[(fh % threads, path)]
                    except KeyError:
                        file = self._path_to_file[(fh % threads, path)] = self._open(path)
                        attr.update(_data=file.read(2048))
                        if size <= 2048:
                            attr.update(_loaded=True)
                            file.close()
                            return 0
            self._fh_to_file[fh] = file
            return fh
        except BaseException as e:
            self._log(
                logging.ERROR, 
                "can't open file: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                path, type(e).__qualname__, e, 
            )
            raise FuseOSError(EIO) from e

    def opendir(self, /, path: str) -> int:
        self._log(logging.DEBUG, "opendir(path=\x1b[4;34m%r\x1b[0m)", path)
        if not self.zipfile_as_dir:
            return 0
        attr = self.getattr(path)
        if not attr.get("_is_zip", False):
            return 0
        zip_path = attr["_zip_path"]
        try:
            zfile = self._open_zip(zip_path)
            fh = self._next_fh()
            if path == zip_path + ".d":
                self._fh_to_zdir[fh] = ZipPath(zfile)
            else:
                self._fh_to_zdir[fh] = ZipPath(zfile).joinpath(path.removeprefix(zip_path + ".d/"))
        except BaseException as e:
            self._log(
                logging.ERROR, 
                "can't open file: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                zip_path, type(e).__qualname__, e, 
            )
            raise FuseOSError(EIO) from e
        return fh

    def read(self, /, path: str, size: int, offset: int, fh: int = 0) -> bytes:
        self._log(logging.DEBUG, "read(path=\x1b[4;34m%r\x1b[0m, size=%r, offset=%r, fh=%r)", path, size, offset, fh)
        attr = self.getattr(path)
        if fh == 0:
            if attr.get("_loaded", False):
                return attr["_data"][offset:offset+size]
        try:
            try:
                file = self._fh_to_file[fh] = self._fh_to_file[fh]
            except (KeyError, OSError):
                self.open(path, fh=fh)
                file = self._fh_to_file[fh]
            if 0 <= offset < 2048:
                if offset + size <= 2048:
                    return attr["_data"][offset:offset+size]
                else:
                    file.seek(2048)
                    return attr["_data"][offset:] + file.read(offset+size-2048)
            file.seek(offset)
            return file.read(size)
        except BaseException as e:
            self._log(
                logging.ERROR, 
                "can't read file: \x1b[4;34m%s\x1b[0m\n  |_ \x1b[1;4;31m%s\x1b[0m: %s", 
                path, type(e).__qualname__, e, 
            )
            raise FuseOSError(EIO) from e

    @update_readdir_later
    def readdir(self, /, path: str, fh: int = 0) -> list[str]:
        self._log(logging.DEBUG, "readdir(path=\x1b[4;34m%r\x1b[0m, fh=%r)", path, fh)
        if fh:
            ls = [".", ".."]
            try:
                zdir = self._fh_to_zdir[fh]
            except KeyError:
                pass
            else:
                ls.extend(p.name for p in zdir.iterdir())
            return ls
        predicate = self.predicate
        strm_predicate = self.strm_predicate
        try:
            old_cache = self.cache[path]
        except KeyError:
            old_cache = None
        cache = {}
        for pathobj in self.fs.listdir_path(path):
            is_dir = pathobj.is_dir()
            name = pathobj.name
            mtime = parse_as_ts(pathobj.get("writeTime"))
            subpath = joinpath(path, name)
            if self.zipfile_as_dir and not is_dir and name.endswith(".zip", 1):
                name_d = name + ".d"
                try:
                    d = old_cache[name_d]
                    if not d["_is_zip"] or d["st_mtime"] != mtime:
                        raise KeyError 
                except (TypeError, KeyError):
                    cache[name_d] = dict(
                        st_uid=0, 
                        st_gid=0, 
                        st_mode=S_IFDIR | 0o555, 
                        st_nlink=1, 
                        st_size=0, 
                        st_ctime=parse_as_ts(pathobj.get("createTime")), 
                        st_mtime=mtime, 
                        st_atime=parse_as_ts(pathobj.get("accessTime")), 
                        _is_zip=True, 
                        _zip_path=subpath, 
                    )
                else:
                    d.update(
                        st_ctime=parse_as_ts(pathobj.get("createTime")), 
                        st_mtime=mtime, 
                        st_atime=parse_as_ts(pathobj.get("accessTime")), 
                    )
                    cache[name_d] = d
            loaded = False
            data = b""
            size = 0
            if not is_dir and strm_predicate and strm_predicate(name):
                data = pathobj.url.encode("latin-1")
                size = len(data)
                name += ".strm"
                loaded = True
            elif predicate and not predicate(subpath + "/"[:is_dir]):
                continue
            elif not is_dir:
                try:
                    size = int(pathobj.get("size", 0))
                except KeyError:
                    breakpoint()
                if size == 0:
                    loaded = True
            try:
                d = old_cache[name]
                if d["st_mtime"] != mtime:
                    raise KeyError
            except (KeyError, TypeError):
                d = dict(
                    st_uid=0, 
                    st_gid=0, 
                    st_mode=(S_IFDIR if is_dir else S_IFREG) | 0o555, 
                    st_nlink=1, 
                    st_size=size, 
                    st_ctime=parse_as_ts(pathobj.get("createTime")), 
                    st_mtime=parse_as_ts(pathobj.get("writeTime")), 
                    st_atime=parse_as_ts(pathobj.get("accessTime")), 
                )
                if not is_dir:
                    d.update(_loaded=loaded, _data=data)
            else:
                d.update(
                    st_ctime=parse_as_ts(pathobj.get("createTime")), 
                    st_mtime=mtime, 
                    st_atime=parse_as_ts(pathobj.get("accessTime")), 
                )
            cache[name] = d
        self.cache[path] = cache
        return [".", "..", *cache]

    def release(self, /, path: str, fh: int = 0):
        self._log(logging.DEBUG, "release(path=\x1b[4;34m%r\x1b[0m, fh=%r)", path, fh)
        if fh:
            if self.max_read_threads > 0:
                self._file_to_release[self._fh_to_file.pop(fh)] = None
            else:
                self._fh_to_file.pop(fh)

    def releasedir(self, /, path: str, fh: int = 0):
        self._log(logging.DEBUG, "releasedir(path=\x1b[4;34m%r\x1b[0m, fh=%r)", path, fh)
        if fh:
            self._fh_to_zdir.pop(fh, None)


if __name__ == "__main__":
    import logging

    log_level = args.log_level
    if isinstance(log_level, str):
        try:
            log_level = int(log_level)
        except ValueError:
            log_level = getattr(logging, log_level.upper(), logging.NOTSET)
    logger.setLevel(log_level)

    ls: list[str] = []
    strm_predicate = None
    if args.strm:
        ls.extend(read_str(args.strm))
    if args.strm_file:
        try:
            ls.extend(read_file(open(args.strm_file, encoding="utf-8")))
        except OSError:
            logger.exception("can't read file: %r", args.strm_file, extra=_EXTRA)
    if ls:
        strm_predicate = parse(ls, check_mimetype=True)

    ls = []
    predicate = None
    if args.ignore:
        ls.extend(read_str(args.ignore))
    if args.ignore_file:
        try:
            ls.extend(read_file(open(args.ignore_file, encoding="utf-8")))
        except OSError:
            logger.exception("can't read file: %r", args.ignore_file, extra=_EXTRA)
    if ls:
        ignore = parse(ls, check_mimetype=True)
        if ignore:
            predicate = lambda p: not ignore(p)

    print("\n    👋 Welcome to use clouddrive fuse and strm 👏\n")
    # https://code.google.com/archive/p/macfuse/wikis/OPTIONS.wiki
    fuse = FUSE(
        CloudDriveFuseOperations(
            args.origin, 
            args.username, 
            args.password, 
            predicate=predicate, 
            strm_predicate=strm_predicate, 
            max_read_threads=args.max_read_threads, 
            zipfile_as_dir=args.zipfile_as_dir, 
        ),
        args.mount_point, 
        ro=True, 
        allow_other=args.allow_other, 
        foreground=not args.background, 
        nothreads=args.nothreads, 
        debug=args.debug, 
    )

