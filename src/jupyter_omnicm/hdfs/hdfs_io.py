"""
Utilities for HDFS-based Contents/Checkpoints managers.
"""

import uuid
from contextlib import contextmanager

from notebook.utils import to_os_path, to_api_path

try:  # new notebook
    from notebook import _tz as tz
except ImportError: # old notebook
    from notebook.services.contents import tz
import os

import nbformat
import tempfile
from tornado.web import HTTPError
from traitlets.config import Configurable

try: #PY3
    from base64 import encodebytes, decodebytes
except ImportError: #PY2
    from base64 import encodestring as encodebytes, decodestring as decodebytes



class HDFSManagerMixin(Configurable):
    """
    Mixin for ContentsAPI classes that interact with the filesystem.

    Provides facilities for reading, writing, and copying both notebooks and
    generic files.

    Shared by FileContentsManager and FileCheckpoints.

    Note
    ----
    Classes using this mixin must provide the following attributes:

    root_dir : unicode
        A directory against against which API-style paths are to be resolved.

    fs : HadoopFileSystem
        A HDFS instance handler against which FS operations are performed.

    log : logging.Logger
    """

    def _to_fs_path(self, path):
        """Given an API path, return its HDFS path.

        Parameters
        ----------
        path : string
            The relative API path to the named file.

        Returns
        -------
        fs_path : string
            Native, absolute HDFS path to for a file.

        Raises
        ------
        404: if path is outside root
        """
        fs_path = to_os_path(path, self.root_dir)
        if not fs_path.startswith(self.root_dir):
            raise HTTPError(404, "%s is outside root contents directory" % path)
        return fs_path

    def _to_api_path(self, fs_path):
        """Given an HDFS path, return its API path.

        Parameters
        ----------
        fs_path : string
            The HDFS path to the named file.

        Returns
        -------
        path : string
            Relative API path to for a file.
        """
        return to_api_path(fs_path, self.root_dir)

    @contextmanager
    def perm_to_403(self):
        """context manager for turning permission errors into 403."""
        try:
            yield
        except (OSError, IOError) as e:
            raise HTTPError(403, f'Permission denied {e}')

    def _ensure_path_is_valid(self, path, type=None, enforce_exists=False):
        if self.is_hidden(path):
            raise HTTPError(400, f'Invalid hidden file/directory: {path}')
        fs_path = self._to_fs_path(path)
        if self.fs.exists(fs_path):
            if type is not None:
                if type in ('file', 'notebook'):
                    if self.fs.isdir(fs_path):
                        raise HTTPError(400, f'Not a file: {path}')
                else:
                    if self.fs.isfile(fs_path):
                        raise HTTPError(400, f'Not a directory: {path}')
        elif enforce_exists:
            raise HTTPError(400, f'{path} does not exist')

    def _save_directory(self, path, mode=0o0770):
        """Create a directory.

        Parameters
        ----------
        path : string
            The API path of the directory.

        Raises
        ------
        400: if path is a hidden directory, if directory is already exists or if it is not a directory
        403: if permission is denied
        """
        if self.is_hidden(path):
            raise HTTPError(400, f'Cannot create hidden directory {path}')
        fs_path = self._to_fs_path(path)
        if self.fs.exists(fs_path):
            if self.fs.isfile(fs_path):
                raise HTTPError(400, f'Not a directory: {path}')
            else:
                raise HTTPError(400, f'Directory {path} already exists')
        with self.perm_to_403():
            self.fs.mkdir(fs_path)
            self.fs.chmod(fs_path, mode)

    def _save_file(self, path, content, format):
        """Save content in a file, creating it if needed.

        Parameters
        ----------
        path : string
            The API path of the file.

        content: blob
            The content to save in the file.

        format: string
            The format of the content. Can be either 'text' or 'base64'

        Raises
        ------
        400: if format has invalid parameter. if path is a hidden file, if file already exists but is a directory
        403: if permission is denied
        """
        if format not in {'text', 'base64'}:
            raise HTTPError(400, "Must specify format of file contents as 'text' or 'base64'")
        self._ensure_path_is_valid(path, 'file')
        fs_path = self._to_fs_path(path)
        try:
            if format == 'text':
                bcontent = content.encode('utf8')
            else:
                b64_bytes = content.encode('ascii')
                bcontent = decodebytes(b64_bytes)
        except Exception as e:
            raise HTTPError(400, f'Content encoding error for {path} {e}')
        with self.perm_to_403():
            with self.fs.open(fs_path, 'wb') as fp:
                try:
                    fp.write(bcontent)
                except Exception as e:
                    raise HTTPError(400, f"Unwritable file: {path} {e}")

    def _save_notebook(self, path, nb, as_version=nbformat.NO_CONVERT):
        """Save a notebook in a file, creating it if needed.

        Parameters
        ----------
        path : string
            The API path of the file.

        nb: blob
            The notebook to save in the file.

        as_version: string
            The format of the notebook. Defaults to NO_CONVERT.

        Raises
        ------
        400: if path is a hidden file, if file already exists but is a directory
        403: if permission is denied
        """
        self._ensure_path_is_valid(path, 'file')
        fs_path = self._to_fs_path(path)
        tempdir = tempfile.mkdtemp()
        filename = f'{tempdir}/{uuid.uuid4()}'
        try:
            with open(filename, mode='w+', encoding='utf-8') as fp:
                nbformat.write(nb, fp, as_version)
            with self.perm_to_403():
                with open(filename, mode='rb') as fp:
                    self.fs.upload(fs_path, fp)
            os.unlink(filename)
        except Exception as e:
            if os.path.exists(filename):
                os.unlink(filename)
            raise HTTPError(400, f"Unwritable Notebook: {path} {e}")

    def _read_notebook(self, path, as_version=4):
        """Read a notebook from a path.

        Parameters
        ----------
        path : string
            The API path of the file.

        as_version: string
            The format of the notebook. Defaults to NO_CONVERT.

        Raises
        ------
        400: if path is a hidden file, if file already exists but is a directory
        403: if permission is denied
        """
        self._ensure_path_is_valid(path, 'file', enforce_exists=True)
        fs_path = self._to_fs_path(path)
        with self.perm_to_403():
            with self.fs.open(fs_path, 'rb') as fp:
                try:
                    return nbformat.reads(fp.read(), as_version)
                except Exception as e:
                    raise HTTPError(400, f"Unreadable file: {path} {e}")

    def _read_file(self, path, format):
        """Read a non-notebook file.

        path: The path to be read.
        format:
          If 'text', the contents will be decoded as UTF-8.
          If 'base64', the raw bytes contents will be encoded as base64.
          If not specified, try to decode as UTF-8, and fall back to base64

        Parameters
        ----------
        path : string
            The API path of the file.

        format: string
            The format of the file. Can be either 'text' or 'base64'.

        Raises
        ------
        400: if path is a hidden file, if file already exists but is a directory
        403: if permission is denied
        """
        self._ensure_path_is_valid(path, 'file', enforce_exists=True)
        fs_path = self._to_fs_path(path)
        with self.perm_to_403():
            with self.fs.open(fs_path, 'rb') as fp:
                try:
                    bcontent = fp.readall()
                    # Try to interpret as unicode if format is unknown or if unicode
                    # was explicitly requested.
                    try:
                        return bcontent.decode('utf8'), 'text'
                    except UnicodeError:
                        if format == 'text':
                            raise HTTPError(400, f"{path} is not UTF-8 encoded")
                    return encodebytes(bcontent).decode('ascii'), 'base64'
                except Exception as e:
                    raise HTTPError(400, f"Unreadable file: {path} {e}")
