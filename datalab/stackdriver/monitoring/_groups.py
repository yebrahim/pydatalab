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
from __future__ import unicode_literals
from builtins import object

import collections
import fnmatch

import pandas

import datalab.context

from . import _impl
from . import _utils


_Node = collections.namedtuple('_Node',
                               ('entity_id', 'parent_id', 'size', 'metric'))


class Groups(object):
  """Represents a list of Stackdriver groups for a project."""

  def __init__(self, project_id=None, context=None):
    """Initializes the Groups based on the specified filters.

    Args:
      project_id: An optional project ID or number to override the one provided
          by the context.
      context: An optional Context object to use instead of the global default.
    """
    self._context = context or datalab.context.Context.default()
    self._project_id = project_id or self._context.project_id
    self._client = _utils.make_client(project_id, context)
    self._group_dict = None

  def list(self, pattern='*'):
    """Returns a list of groups that match the filters.

    Args:
      pattern: An optional pattern to filter the groups based on their display
          name. This can include Unix shell-style wildcards. E.g. "Production*".

    Returns:
      A list of Group objects that match the filters.
    """
    if self._group_dict is None:
      self._group_dict = {
          group.group_id: group for group in _impl.Group.list(self._client)}

    return [group for group in self._group_dict.itervalues()
            if fnmatch.fnmatch(group.display_name, pattern)]

  def table(self, pattern='*', max_rows=-1):
    """Visualize the matching descriptors as an HTML table.

    Args:
      pattern: An optional pattern to further filter the descriptors. This can
          include Unix shell-style wildcards. E.g. "aws*", "*cluster*".
      max_rows: The maximum number of rows (timestamps) to display. A value less
          than 0 shows all rows.

    Returns:
      The HTML rendering for a table of matching metric descriptors.
    """
    import IPython.core.display
    import datalab.utils.commands

    data = []
    for i, group in enumerate(self.list(pattern)):
      if max_rows >= 0 and i >= max_rows:
        break

      parent = self._group_dict.get(group.parent_id)
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

  def treemap(self, metric_type=None, offset='1h'):
    """Draws a treemap with the group hierarchy in the project.

    Args:
      metric_type: An optional metric type that assigns color to the treemap
          cells.
      offset: The offset from the current time to use for the time interval of
          the metric query. This can be specified as a Python timedelta object or a
          pandas offset alias string:
      http://pandas.pydata.org/pandas-docs/stable/timeseries.html#offset-aliases

    Returns:
      The HTML rendering of a treemap.
    """
    import datalab.utils.commands

    dataframe = self._hierarchy(metric_type, offset).fillna(0)
    properties = dict(height=500, maxPostDepth=1,
                      minColor='green', maxColor='red')
    if metric_type is not None:
      properties['midColor'] = 'yellow'

    prop = '\n'.join('%s: %s' % (key, value)
                     for key, value in properties.iteritems())
    return datalab.utils.commands._chart._chart_cell({
        'chart': 'treemap',
        'data': dataframe,
        'fields': ','.join(dataframe.columns)
    }, prop)

  def _hierarchy(self, metric_type=None, offset='1h'):
    """Returns a dataframe with the group hierarchy of the project. """
    query = None
    if metric_type is not None:
      query = self._build_query(metric_type, offset)

    hierarchy_rows = [_Node(entity_id=self._project_id, parent_id=None,
                            size=0, metric=0)]
    for group in self.list():
      parent_id = self._group_display_id(group.parent_id) or self._project_id
      entity_id = self._group_display_id(group.group_id)
      if metric_type is not None:
        dataframe = query.select_group(group.group_id).as_dataframe()
        metric = dataframe.mean().mean()
      else:
        metric = 0
      hierarchy_rows.append(
          _Node(entity_id=entity_id, parent_id=parent_id, size=1,
                metric=metric))

    return pandas.DataFrame.from_records(hierarchy_rows, columns=_Node._fields)

  def _group_display_id(self, group_id):
    """Creates the ID to display for a group."""
    group = self._group_dict.get(group_id)
    if group is None:
      return group_id
    return '%s (%s)' % (group.display_name, group_id)

  def _build_query(self, metric_type, offset='1h'):
    """Builds a query object based on the metric_type and offset."""
    from . import _query

    descriptor = self._client.fetch_metric_descriptor(metric_type)
    if descriptor.value_type not in ['INT64', 'DOUBLE']:
      raise ValueError('Only numeric metric types are supported')

    if descriptor.metric_kind == 'CUMULATIVE':
      alignment_method = 'ALIGN_DELTA'
    else:
      alignment_method = 'ALIGN_MEAN'

    query = _query.Query(metric_type, project_id=self._project_id,
                         context=self._context)
    query = query.select_interval(offset=offset)
    duration = (query._end_time - query._start_time).total_seconds()
    query = query.align(alignment_method, seconds=int(duration))
    query = query.reduce('REDUCE_MEAN', 'resource.project_id')
    return query
