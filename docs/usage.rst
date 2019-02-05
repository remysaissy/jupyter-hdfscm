=====
Usage
=====

Behind the scene, the HDFS support of jupyter-omnicm relies on pyarrow (https://arrow.apache.org/).

To use jupyter-omnicm HDFS in Jupyter, add the following line in your jupyter_notebook_config.py::

	c.NotebookApp.contents_manager_class = 'jupyter_omnicm.hdfs.hdfs_manager.HDFSContentsManager'



*Running Jupyter as a YARN container*

You can run a Jupyter notebook as a YARN container using Skein: https://github.com/jcrist/skein for eg.

If the cluster is kerberized, you don't need more configuration as the HDFS delegation token will be used automatically.
If the custer is not kerberized, you can add the following in jupyter_notebook_config.py::

    c.HDFSContentsManager.user = 'username'


*Running Jupyter outside of a YARN container*

In the case where you run the jupyter notebook outside of a YARN container (laptop, server, other orchestrator, ...),
you need to do a bit more configuration.
Depending on your setup, you can add any of the following lines in your jupyter_notebook_config.py::

    c.HDFSContentsManager.host = 'namenode DNS (str). Default 'default' (detect from *-site.xml).'
    c.HDFSContentsManager.port = 'namenode client RPC port (int). Default 0 (detect from *-site.xml).'
    c.HDFSContentsManager.user = 'username. Default None.'
    c.HDFSContentsManager.kerb_ticket = 'Path to Kerberos ticket cache. Default None.'
    c.HDFSContentsManager.driver = 'Connect using libhdfs (JNI-based) or libhdfs3 (3rd-party C++ library). Default libhdfs.'
    c.HDFSContentsManager.extra_conf = {key:value} 'extra Key/Value pairs for config; Will override any hdfs-site.xml properties.'

No need to add all of them however!
Keep in mind that we use pyarrow under the hood which is able to read HDFS configuration *-site.xml automatically given
that variables like HADOOP_CONF_DIR, HADOOP_HOME, ... are properly setup. Refer to pyarrow documentation for it.

*Configuring the root directory*
An absolute path in Jupyter's default notebook_dir is used for that.
If you don't specify anything for this parameter, the notebook dir will be the user's home directory on HDFS::

    c.NotebookApp.notebook_dir
