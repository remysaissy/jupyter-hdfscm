"""
HDFS-based Checkpoints implementations.
"""
import os

from notebook import _tz as tz
from notebook.services.contents.checkpoints import Checkpoints
from pyarrow.hdfs import HadoopFileSystem
from tornado.httpclient import HTTPError
from traitlets import Unicode

from .hdfs_io import HDFSManagerMixin


class HDFSCheckpoints(HDFSManagerMixin, Checkpoints):
    """
    A Checkpoints that caches checkpoints for files in adjacent
    directories.

    Only works with HDFSContentsManager.
    """

    checkpoint_dir = Unicode(
        u'.ipynb_checkpoints',
        config=True,
        help="""The directory name in which to keep file checkpoints
        This is a path relative to the file's own directory.
        By default, it is .ipynb_checkpoints
        """,
    )

    fs: HadoopFileSystem = None
    root_dir = None

    def create_checkpoint(self, contents_mgr, path):
        """Create a checkpoint."""
        fs_path = self._to_fs_path(path)
        checkpoint_id = u'checkpoint'
        dest_path = self.checkpoint_path(checkpoint_id, path)
        fs_dest_path = self._to_fs_path(dest_path)
        self.__fs_copy(fs_path, fs_dest_path)
        return self.checkpoint_model(checkpoint_id, dest_path)

    def restore_checkpoint(self, contents_mgr, checkpoint_id, path):
        """Restore a checkpoint."""
        fs_path = self._to_fs_path(path)
        src_path = self.checkpoint_path(checkpoint_id, path)
        fs_src_path = self._to_fs_path(src_path)
        self.__fs_copy(fs_src_path, fs_path)

    def rename_checkpoint(self, checkpoint_id, old_path, new_path):
        """Rename a checkpoint from old_path to new_path."""
        old_cp_path = self.checkpoint_path(checkpoint_id, old_path)
        fs_old_cp_path = self._to_api_path(old_cp_path)
        new_cp_path = self.checkpoint_path(checkpoint_id, new_path)
        fs_new_cp_path = self._to_api_path(new_cp_path)
        if self.fs.exists(fs_old_cp_path):
            self.log.info(
                "Renaming checkpoint %s -> %s",
                old_cp_path,
                new_cp_path,
            )
            with self.perm_to_403():
                self.fs.rename(fs_old_cp_path, fs_new_cp_path)

    def delete_checkpoint(self, checkpoint_id, path):
        """delete a file's checkpoint"""
        cp_path = self.checkpoint_path(checkpoint_id, path)
        fs_cp_path = self._to_fs_path(cp_path)
        if not self.fs.exists(fs_cp_path):
            raise HTTPError(404, f'Checkpoint does not exist: {path}@{checkpoint_id}')
        self.log.info("Removing checkpoint %s", cp_path)
        with self.perm_to_403():
            self.fs.delete(fs_cp_path)

    def list_checkpoints(self, path):
        """list the checkpoints for a given file
        This contents manager currently only supports one checkpoint per file.
        """
        checkpoint_id = u'checkpoint'
        cp_path = self.checkpoint_path(checkpoint_id, path)
        fs_cp_path = self._to_fs_path(cp_path)
        if not self.fs.exists(fs_cp_path):
            return []
        else:
            return [self.checkpoint_model(checkpoint_id, cp_path)]

    def checkpoint_path(self, checkpoint_id, path):
        """find the path to a checkpoint"""
        path = path.strip('/')
        parent, name = ('/' + path).rsplit('/', 1)
        parent = parent.strip('/')
        basename, ext = os.path.splitext(name)
        filename = u"{name}-{checkpoint_id}{ext}".format(
            name=basename,
            checkpoint_id=checkpoint_id,
            ext=ext,
        )
        cp_dir = os.path.join(parent, self.checkpoint_dir)
        fs_cp_dir = self._to_fs_path(cp_dir)
        with self.perm_to_403():
            self.fs.exists(fs_cp_dir) and self.fs.isdir(fs_cp_dir)
        cp_path = os.path.join(cp_dir, filename)
        return cp_path

    def checkpoint_model(self, checkpoint_id, path):
        """construct the info dict for a given checkpoint"""
        fs_path = self._to_fs_path(path)
        stats = self.fs.info(fs_path)
        last_modified = tz.utcfromtimestamp(stats[u'last_modified'])

        info = dict(
            id=checkpoint_id,
            last_modified=last_modified,
        )
        return info

    def __fs_copy(self, fs_src, fs_dst, mode=0o0770, chunk_size=2**16):
        """Copy a file.

        Parameters
        ----------
        fs_src: string
            Path of source file on HDFS

        fs_dst: string
            Path of destination file on HDFS

        mode: integer, optional
            Permissions to apply to the destination file. Default 0770.

        chunk_size: integer, optional
            Copy chunk size to use when doing the copy. Default 2**16.
        """
        with self.fs.open(fs_dst, mode='wb') as f1:
            with self.fs.open(fs_src, mode='rb') as f2:
                while True:
                    out = f2.read(chunk_size)
                    if len(out) == 0:
                        break
                    f1.write(out)
        self.fs.chmod(fs_dst, mode)
