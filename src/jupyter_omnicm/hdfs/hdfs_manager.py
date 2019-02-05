""" A content manager that uses the HDFS File system for storage. """
import mimetypes

import nbformat
from notebook.services.contents.manager import ContentsManager
from pyarrow.hdfs import HadoopFileSystem
from tornado import web
from traitlets import Unicode, default, Instance, Integer, Dict

from .hdfs_checkpoints import HDFSCheckpoints
from .hdfs_io import HDFSManagerMixin

try:  # new notebook
    from notebook import _tz as tz
except ImportError: # old notebook
    from notebook.services.contents import tz

try:
    from os.path import samefile
except ImportError:
    # windows + py2
    from notebook.utils import samefile_simple as samefile

import pyarrow as pa

_script_exporter = None


class HDFSContentsManager(HDFSManagerMixin, ContentsManager):

    host: str            = Unicode(u'default', config=True, help="NameNode. Set to 'default' for fs.defaultFS from core-site.xml. Default 'default'.")
    port: int            = Integer(0, config=True, help="NameNode's port. Set to 0 for default or logical (HA) nodes. Default 0.")
    user: str            = Unicode(u'', config=True, help='Username when connecting to HDFS; None implies login user. Default None.')
    kerb_ticket: str     = Unicode(u'', config=True, help='Path to Kerberos ticket cache. Default None.')
    driver: str          = Unicode(u'libhdfs', config=True, help="Connect using 'libhdfs' (JNI-based) or 'libhdfs3' (3rd-party C++ library). Default 'libhdfs'.")
    extra_conf: dict     = Dict({}, config=True, help='extra Key/Value pairs for config; Will override any hdfs-site.xml properties. Default None.')
    fs: HadoopFileSystem = Instance(HadoopFileSystem, config=True, help="HDFS connection. Setup automatically based on the other parameters. Do not set manually.")
    root_dir: str        = Unicode(config=True)

    @default('root_dir')
    def _default_root_dir(self):
        try:
            nb_dir = self.parent.notebook_dir
            return self.fs.info(nb_dir)['path']
        except:
            return self.fs.info('.')['path']

    @default('fs')
    def _default_fs(self):
        user = self.user
        if len(user) == 0:
            user = None
        kerb_ticket = self.kerb_ticket
        if len(kerb_ticket) == 0:
            kerb_ticket = None
        return pa.hdfs.connect(host=self.host, port=self.port, user=user, kerb_ticket=kerb_ticket,
                               driver=self.driver, extra_conf=self.extra_conf)

    def _checkpoints_class_default(self):
        HDFSCheckpoints.fs = self.fs
        HDFSCheckpoints.root_dir = self.root_dir
        return HDFSCheckpoints

    def dir_exists(self, path):
        """Does a directory exist at the given path?

        Like os.path.isdir

        Parameters
        ----------
        path : string
            The path to check

        Returns
        -------
        exists : bool
            Whether the path does indeed exist.
        """
        fs_path = self._to_fs_path(path)
        return self.fs.exists(fs_path) and self.fs.isdir(fs_path)

    def is_hidden(self, path):
        """Is path a hidden directory or file?

        Parameters
        ----------
        path : string
            The path to check. This is an API path (`/` separated,
            relative to root dir).

        Returns
        -------
        hidden : bool
            Whether the path is hidden.

        """
        fs_path = self._to_fs_path(path)
        return any(part.startswith('.') for part in fs_path.split('/'))

    def file_exists(self, path=''):
        """Does a file exist at the given path?

        Like os.path.isfile

        Override this method in subclasses.

        Parameters
        ----------
        path : string
            The API path of a file to check for.

        Returns
        -------
        exists : bool
            Whether the file exists.
        """
        fs_path = self._to_fs_path(path)
        return self.fs.exists(fs_path) and self.fs.isfile(fs_path)

    def __base_model(self, path, type):
        """Build the common base of a model"""
        self._ensure_path_is_valid(path, type, enforce_exists=True)
        fs_path = self._to_fs_path(path)
        info = self.fs.info(fs_path)
        last_modified = tz.utcfromtimestamp(info[u'last_modified'])
        created = tz.utcfromtimestamp(info[u'last_accessed'])
        model = {}
        model['name'] = path.rsplit('/', 1)[-1]
        model['path'] = path
        model['last_modified'] = last_modified
        model['created'] = created
        model['content'] = None
        model['format'] = None
        model['mimetype'] = None
        model['type'] = info[u'kind']

        try:
            model['writable'] = (info[u'permissions'] & 0o0200) > 0
        except OSError:
            self.log.error(f"Failed to check write permissions on {path}")
            model['writable'] = False
        return model

    def __get_dir(self, path, content=True):
        """Build a model for a directory
        if content is requested, will include a listing of the directory
        """
        model = self.__base_model(path, 'directory')
        fs_path = self._to_fs_path(path)
        if content:
            model['content'] = contents = []
            for subpath in self.fs.ls(fs_path):
                name = subpath.strip('/').rsplit('/', 1)[-1]
                if self.should_list(name) and not self.is_hidden(subpath):
                    contents.append(self.get(
                        path='%s/%s' % (path, name),
                        content=False))
            model['format'] = 'json'
        return model

    def __get_file(self, path, content=True, format=None):
        """Build a model for a file
        if content is requested, include the file hdfscontents.
        format:
          If 'text', the hdfscontents will be decoded as UTF-8.
          If 'base64', the raw bytes hdfscontents will be encoded as base64.
          If not specified, try to decode as UTF-8, and fall back to base64
        """
        model = self.__base_model(path, 'file')
        model['mimetype'] = mimetypes.guess_type(path)[0]
        if content:
            content, format = self._read_file(path, format)
            if model['mimetype'] is None:
                default_mime = {
                    'text': 'text/plain',
                    'base64': 'application/octet-stream'
                }[format]
                model['mimetype'] = default_mime

            model.update(
                content=content,
                format=format)
        return model

    def __get_notebook(self, path, content=True):
        """Build a notebook model
        if content is requested, the notebook content will be populated
        as a JSON structure (not double-serialized)
        """
        model = self.__base_model(path, 'file')
        model['type'] = 'notebook'
        if content:
            nb = self._read_notebook(path)
            self.mark_trusted_cells(nb, path)
            model['content'] = nb
            model['format'] = 'json'
            self.validate_notebook_model(model)
        return model

    def get(self, path, content=True, type=None, format=None):
        """Get a file or directory model."""
        if not type: # Infers the type if not specified.
            fs_path = self._to_fs_path(path)
            if not self.fs.exists(fs_path):
                raise web.HTTPError(400, f'{path} does not exist')
            info = self.fs.info(fs_path)
            type = info[u'kind']
        if path.endswith('.ipynb'): # fix type with notebook special case.
            type = 'notebook'
        if type == 'directory':
            model = self.__get_dir(path, content=content)
        elif type == 'notebook':
            model = self.__get_notebook(path, content=content)
        else:
            model = self.__get_file(path, content=content, format=format)
        return model

    def save(self, model, path):
        """
        Save a file or directory model to path.

        Should return the saved model with no content.  Save implementations
        should call self.run_pre_save_hook(model=model, path=path) prior to
        writing any data.
        """
        self._ensure_path_is_valid(path)
        if 'type' not in model:
            raise web.HTTPError(400, u'No file type provided')
        if 'content' not in model and model['type'] != 'directory':
            raise web.HTTPError(400, u'No file content provided')
        self.run_pre_save_hook(model=model, path=path)
        try:
            if model['type'] == 'notebook':
                nb = nbformat.from_dict(model['content'])
                self.check_and_sign(nb, path)
                self._save_notebook(path, nb)
                # One checkpoint should always exist for notebooks.
                if not self.checkpoints.list_checkpoints(path):
                    self.create_checkpoint(path)
            elif model['type'] == 'file':
                # Missing format will be handled internally by _save_file.
                self._save_file(path, model['content'], model.get('format'))
            elif model['type'] == 'directory':
                self._save_directory(path)
            else:
                raise web.HTTPError(400, f"Unhandled content type: {model['type']}")
        except web.HTTPError:
            raise
        except Exception as e:
            raise web.HTTPError(500, f'Unexpected error while saving file: {path} {e}')
        validation_message = None
        if model['type'] == 'notebook':
            self.validate_notebook_model(model)
            validation_message = model.get('message', None)
        model = self.get(path, content=False)
        if validation_message:
            model['message'] = validation_message
        return model

    def delete_file(self, path):
        """Delete the file or directory at path."""
        if not self.exists(path):
            raise web.HTTPError(404, f'File or directory does not exist: {path}')
        if self.is_hidden(path):
            raise web.HTTPError(400, f'Invalid hidden file/directory: {path}')
        fs_path = self._to_fs_path(path)
        if self.fs.isfile(fs_path):
            with self.perm_to_403():
                self.fs.delete(fs_path)
        else:
            if len(self.fs.ls(fs_path)) > 0:
                raise web.HTTPError(400, f'Directory not empty: {path}')
            else:
                with self.perm_to_403():
                    self.fs.delete(fs_path)

    def rename_file(self, old_path, new_path):
        """Rename a file or directory."""
        if not self.exists(old_path):
            raise web.HTTPError(404, f'File or directory does not exist: {old_path}')
        if self.is_hidden(old_path):
            raise web.HTTPError(400, f'Invalid hidden file/directory: {old_path}')
        if self.exists(new_path):
            raise web.HTTPError(404, f'File or directory already exist: {new_path}')
        if self.is_hidden(new_path):
            raise web.HTTPError(400, f'Invalid hidden file/directory: {new_path}')
        old_fs_path = self._to_fs_path(old_path)
        new_fs_path = self._to_fs_path(new_path)
        with self.perm_to_403():
            self.fs.rename(old_fs_path, new_fs_path)

    def info_string(self):
        return f"Serving notebooks from HDFS directory: {self.root_dir}"
