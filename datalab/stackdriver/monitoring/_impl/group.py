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
from past.builtins import basestring

import re

from gcloud.monitoring import Resource


class Group(object):
  """Represents a Stackdriver group.

  Attributes:
    group_id: The ID of the group.
    project: The project ID or number where the group is defined.
    name: The fully qualified name of the group in the format
        "projects/<project>/groups/<id>". This is a read-only property.
        If the group ID is not defined, the value is an empty string.
    display_name: A user-assigned name for this group.
    parent_name: The name (not ID) of the group's parent, if it has one.
    filter: The filter string used to determine which monitored resources
        belong to this group.
    is_cluster: Whether the service should consider this group a cluster and
        perform additional analysis on it.
  """
  _NAME_PATTERN = r'projects/(?P<project>.*)/groups/(?P<group_id>.*)$'

  def __init__(self, client, name='', display_name='', parent_name='',
               filter_string='', is_cluster=False):
    self.client = client
    self.name = name
    self.display_name = display_name
    self.parent_name = parent_name
    self.filter = filter_string
    self.is_cluster = is_cluster

  def __repr__(self):
    return (
        '<Group: \n'
        ' name={name!r},\n'
        ' display_name={display_name!r},\n'
        ' parent_name={parent_name!r},\n'
        ' filter={filter!r},\n'
        ' is_cluster={is_cluster!r}>'
    ).format(**self.__dict__)

  @property
  def group_id(self):
    """The ID of  the group."""
    return self.extract_id(self.name)

  @property
  def parent_id(self):
    """The ID of  the parent group."""
    return self.extract_id(self.parent_name)

  @property
  def project(self):
    """The project of the group."""
    return self.extract_project(self.name)

  @property
  def path(self):
    """URI path for use in monitoring APIs."""
    return '/%s' % (self.name,)

  def create(self):
    """Create a new group based on this object.

    The name attribute is ignored in preparing the creation request.
    All attributes are overwritten by the values received in the response
    (normally affecting only name).
    """
    response = self.client.connection.api_request(
        method='POST', path=self.path, data=self._to_dict())
    self._init_from_dict(response)

  def delete(self):
    """Delete the group identified by this object.

    Only the client and name attributes are used.
    """
    self.client.connection.api_request(method='DELETE', path=self.path)

  def children(self):
    """Lists all children of this group.

    Returns:
      A list of Group instances.
    """
    return self._list(self.client, children_of_group=self.name)

  def ancestors(self):
    """Lists all ancestors of this group.

    Returns:
      A list of Group instances.
    """
    return self._list(self.client, ancestors_of_group=self.name)

  def descendants(self):
    """Lists all descendants of this group.

    Returns:
      A list of Group instances.
    """
    return self._list(self.client, descendants_of_group=self.name)

  def members(self, filter_string=None, end_time=None, start_time=None):
    """Lists all resources matching this group.

    Args:
      filter_string: An optional filter string describing the members to be
        returned.
      end_time: The end time (inclusive) of the time interval for which results
          should be returned, as either a Python datetime object or a timestamp
          string in RFC3339 UTC "Zulu" format. Only members that were part of
          the group during the specified interval are included in the response.
      start_time: The start time (exclusive) of the time interval for which
          results should be returned, as either a datetime object or a
          timestamp string.

    Returns:
      A list of Resource instances.
    """
    path = self.path + '/members'
    resources = []
    page_token = None

    while True:
      params = {'name': self.name}

      if filter_string is not None:
        params['filter'] = filter_string

      if end_time is not None:
        params['interval.endTime'] = _format_timestamp(end_time)

      if start_time is not None:
        params['interval.startTime'] = _format_timestamp(start_time)

      if page_token is not None:
        params['pageToken'] = page_token

      response = self.client.connection.api_request(
          method='GET', path=path, query_params=params)
      for info in response.get('members', []):
        resources.append(Resource._from_dict(info))

      page_token = response.get('nextPageToken')
      if not page_token:
        break

    return resources

  @classmethod
  def extract_id(cls, name):
    """Extract the group ID from a group name."""
    regex_match = re.match(cls._NAME_PATTERN, name)
    return regex_match.group('group_id') if regex_match else ''

  @classmethod
  def extract_project(cls, name):
    """Extract the project ID/number from a group name."""
    regex_match = re.match(cls._NAME_PATTERN, name)
    return regex_match.group('project') if regex_match else ''

  @staticmethod
  def path_helper(project, group_id=''):
    """Returns the path to the group API.

    Args:
      project: The project ID or number to use.
      group_id: The ID of the group.

    Returns:
      The relative URL path for the specific group.
    """
    return '/projects/{project}/groups/{group_id}'.format(
        project=project, group_id=group_id)

  @classmethod
  def _fetch(cls, client, group_id):
    """Looks up a group by ID.

    Args:
      client: The Client to use.
      group_id: The ID of the group.

    Returns:
      A Group instance with all attributes populated.

    Raises:
      RequestException with status == 404 if the group does not exist.
    """
    path = cls.path_helper(client.project, group_id)
    info = client.connection.api_request(method='GET', path=path)
    return cls._from_dict(client, info)

  @classmethod
  def list(cls, client):
    """Lists all groups defined on the project ID.

    Args:
      client: The Client to use.

    Returns:
      A list of Group instances.
    """
    return cls._list(client)

  @classmethod
  def _list(cls, client, children_of_group=None, ancestors_of_group=None,
            descendants_of_group=None):
    """Lists all groups defined on the project ID.

    Args:
      client: The Client to use.
      children_of_group: The name of the group whose children are to be listed.
      ancestors_of_group: The name of the group whose ancestors are to be
          listed.
      descendants_of_group: The name of the group whose descendants are to be
          listed.

    Returns:
      A list of Group instances.
    """
    path = cls.path_helper(client.project)
    groups = []
    page_token = None

    while True:
      params = {}

      if children_of_group is not None:
        params['childrenOfGroup'] = children_of_group

      if ancestors_of_group is not None:
        params['ancestorsOfGroup'] = ancestors_of_group

      if descendants_of_group is not None:
        params['descendantsOfGroup'] = descendants_of_group

      if page_token is not None:
        params['pageToken'] = page_token

      response = client.connection.api_request(
          method='GET', path=path, query_params=params)
      for info in response.get('group', []):
        groups.append(cls._from_dict(client, info))

      page_token = response.get('nextPageToken')
      if not page_token:
        break

    return groups

  @classmethod
  def _from_dict(cls, client, info):
    """Constructs a Group instance from the parsed JSON representation.

    Args:
      client: A client to be included in the returned object.
      info: A dict parsed from the JSON wire-format representation.

    Returns:
      A Group instance with all attributes populated.
    """
    group = cls(client)
    group._init_from_dict(info)
    return group

  def _init_from_dict(self, info):
    """Initialize attributes from the parsed JSON representation.

    :type info: dict
    :param info:
        A ``dict`` parsed from the JSON wire-format representation.
    """
    self.name = info['name']
    self.display_name = info.get('displayName', '')
    self.parent_name = info.get('parentName', '')
    self.filter = info['filter']
    self.is_cluster = info.get('isCluster', False)

  def _to_dict(self):
    """Build a dictionary ready to be serialized to the JSON wire format.

    Returns:
     A dictionary.
    """
    info = {
      'name': self.name,
      'filter': self.filter,
      'displayName': self.display_name,
    }

    if self.parent_name:
      info['parentName'] = self.parent_name
    if self.is_cluster:
      info['isCluster'] = self.is_cluster

    return info


def _format_timestamp(timestamp):
  """Converts a datetime object to a string as required by the API.

  Args:
    timestamp: A Python datetime object or a timestamp string in RFC3339
        UTC "Zulu" format.

  Returns:
    The string version of the timestamp. For example:
        "2016-02-17T19:18:01.763000Z".
  """
  if isinstance(timestamp, basestring):
    return timestamp

  if timestamp.tzinfo is not None:
    # Convert to UTC and remove the time zone info.
    timestamp = timestamp.replace(tzinfo=None) - timestamp.utcoffset()

  return timestamp.isoformat() + 'Z'
