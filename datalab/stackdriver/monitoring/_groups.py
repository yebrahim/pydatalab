# Copyright 2016 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
# in compliance with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied.  See the License for the specific language governing permissions and limitations under
# the License.

"""Groups for the Google Monitoring API."""

from __future__ import absolute_import

import collections
import fnmatch

from . import _impl
from . import _utils


class Groups(object):
  """Represents a list of Stackdriver groups for a project."""

  def __init__(self, project_id=None, context=None):
    """Initializes the Groups based on the specified filters.

    Args:
      project_id: An optional project ID or number to override the one provided
          by the context.
      context: An optional Context object to use instead of the global default.
    """
    self._client = _utils.make_client(project_id, context)
    self._group_dict = None

  def list(self, pattern='*'):
    """Returns a list of groups that match the filters.

    Args:
      pattern: An optional pattern to filter the groups based on their display
        name. This can include Unix shell-style wildcards. E.g. "Production*".
    """
    if self._group_dict is None:
      self._group_dict = {
          group.name: group for group in _impl.Group.list(self._client)}

    return [group for group in self._group_dict.itervalues()
            if fnmatch.fnmatch(group.display_name, pattern)]

  def table(self, pattern='*', max_rows=-1):
    """Visualize the matching descriptors as an HTML table.

    Args:
      pattern: An optional pattern to further filter the descriptors. This can
        include Unix shell-style wildcards. E.g. "aws*", "*cluster*".
      max_rows: The maximum number of rows (timestamps) to display. A value less
        than 0 shows all rows.
    """
    import IPython.core.display
    import datalab.utils.commands

    data = []
    for i, group in enumerate(self.list(pattern)):
      if max_rows >= 0 and i >= max_rows:
        break

      parent = self._group_dict.get(group.parent_name)
      parent_display_name = '' if parent is None else parent.display_name
      data.append(
          collections.OrderedDict([
              ('Group ID', group.group_id),
              ('Group Name', group.display_name),
              ('Parent ID', group.parent_id),
              ('Parent Name', parent_display_name),
              ('Is Cluster', group.is_cluster),
              ('Filter', group.filter),
          ])
      )
    return IPython.core.display.HTML(
        datalab.utils.commands.HtmlBuilder.render_table(data))
