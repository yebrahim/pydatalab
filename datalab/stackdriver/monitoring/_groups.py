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

import pandas

from gcloud.monitoring import Resource

from . import _impl
from . import _utils


def _resource_hash(resource):
  """Returns a hash for a Resource object."""
  elements = frozenset([('type', resource.type)] + resource.labels.items())
  return hash(elements)


def _resource_eq(resource1, resource2):
  """Compares two resources for equality."""
  return (isinstance(resource2, Resource) and
          resource1.__hash__() == resource2.__hash__())


Resource.__hash__ = _resource_hash
Resource.__eq__ = _resource_eq
_Node = collections.namedtuple('_Node', ('entity_id', 'parent_id', 'is_leaf'))


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
    self._project_id = self._client.project
    self._group_dict = None
    self._group_to_members = None

  def list(self, pattern='*'):
    """Returns a list of groups that match the filters.

    Args:
      pattern: An optional pattern to filter the groups based on their display
        name. This can include Unix shell-style wildcards. E.g. "Production*".
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

  def hierarchy(self, with_members=True):
    """Creates a dataframe with the group hierarchy in the project."""
    hierarchy_rows = [_Node(entity_id=self._project_id, parent_id=None,
                            is_leaf=0)]
    for group_id, group in self._group_dict.iteritems():
      parent_id = self._group_display_id(group.parent_id) or self._project_id
      entity_id = self._group_display_id(group_id)
      hierarchy_rows.append(
          _Node(entity_id=entity_id, parent_id=parent_id, is_leaf=0))

    if with_members:
      for group_id, members in self.membership_map().iteritems():
        parent_id = self._group_display_id(group_id)
        for resource in members:
          entity_id = self._resource_display_id(group_id, resource)
          hierarchy_rows.append(
              _Node(entity_id=entity_id, parent_id=parent_id, is_leaf=1))
    return pandas.DataFrame.from_records(hierarchy_rows, columns=_Node._fields)

  def membership_map(self):
    """Returns group members after removing duplicates from ancestor groups."""
    if self._group_to_members is None:
      member_dict = {group_id: set(group.members())
                     for group_id, group in self._group_dict.iteritems()}
      processed_groups = set()
      for group_id in self._group_dict:
        self._update_ancestors(member_dict, group_id, processed_groups)

      self._group_to_members = member_dict

    return self._group_to_members

  def _group_display_id(self, group_id):
    group = self._group_dict.get(group_id)
    if group is None:
      return group_id
    return '%s (%s)' % (group.display_name, group_id)

  @staticmethod
  def _resource_display_id(group_id, resource):
    """Returns a unique ID for a resource and parent group combination"""
    labels = ', '.join(resource.labels.itervalues())
    return '%s: %s (%s)' % (resource.type, labels, group_id)

  def _update_ancestors(self, member_dict, current_group_id, processed_groups):
    """Recursively update all ancestor groups of the current group."""
    current_group = self._group_dict[current_group_id]
    if not current_group.parent_name or current_group_id in processed_groups:
      return

    # Recursively call the method on the ancestors.
    parent_id = current_group.parent_id
    self._update_ancestors(member_dict, parent_id, processed_groups)

    # Update the members of the parent by removing overlapping members.
    member_dict[parent_id] -= member_dict[current_group_id]
    processed_groups.add(current_group_id)


