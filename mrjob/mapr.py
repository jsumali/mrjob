# Copyright 2009-2012 Yelp and Contributors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import getpass

from hadoop import HadoopJobRunner

class MapRJobRunner(HadoopJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on your Hadoop cluster.

    It's pretty much the same as HadoopJobRunner, except MapR uses maprfs://
    instead of hdfs:// paths.
    """
    alias = 'mapr'

    def __init__(self, **kwargs):
        super(MapRJobRunner, self).__init__(**kwargs)

    def fully_qualify_hdfs_path(self, path):
        """If path isn't an ``maprfs://`` URL, turn it into one."""
        if path.startswith('maprfs://') or path.startswith('s3n:/'):
            return path
        elif path.startswith('/'):
            return 'maprfs://' + path
        else:
            return 'maprfs:///user/%s/%s' % (getpass.getuser(), path)