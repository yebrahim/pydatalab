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

"""QueryMetadata object that shows the metadata in a query's results."""


from __future__ import absolute_import
from __future__ import unicode_literals
from builtins import object

import gcloud.monitoring
import pandas

import datalab.context

from . import _metric
from . import _resource


class QueryMetadata(object):
  """QueryMetadata object contains the metadata of a timeseries query."""

  def __init__(self, query):
    self._query = query
    self._data = list(query.iter(headers_only=True))
    self._dataframe = self._initialize_dataframe()
    self._metric_descriptors = None
    self._resource_descriptors = None

  def __iter__(self):
    for timeseries in self._data:
      yield timeseries

  @property
  def metric_descriptors(self):
    """Returns the descriptor for the metric in the underlying query."""
    if self._metric_descriptors is None:
      filter_string = 'metric.type = "%s"' % self._query.metric_type
      self._metric_descriptors = _metric.MetricDescriptors(filter_string)
      self._metric_descriptors._client = self._query._client
    return self._metric_descriptors

  @property
  def resource_descriptors(self):
    """Returns the descriptor for the resource types in the underlying query."""
    if self._resource_descriptors is None:
      resource_types = set([ts.resource.type for ts in self._data])
      self._resource_descriptors = _resource.ResourceDescriptors(
          types=resource_types)
      self._resource_descriptors._client = self._query._client
    return self._resource_descriptors

  def table(self, max_rows=-1):
    """Visualize the results as a table.

    Args:
      max_rows: The maximum number of rows to display. Defaults to -1 which
          shows all the data.
    """
    import IPython.display
    IPython.display.display(self._dataframe.head(max_rows))

  def _initialize_dataframe(self):
    """Returns the resource and metric metadata as a dataframe.

    Returns:
      A pandas dataframe containing the resource type and resource and metric
      labels. Each row in this dataframe corresponds to the metadata from one
      time series.
    """
    headers = [{'resource': ts.resource.__dict__, 'metric': ts.metric.__dict__}
               for ts in self._data]
    if not headers:
      return pandas.DataFrame()
    df = pandas.io.json.json_normalize(headers)

    # Add a 2 level column header.
    df.columns = pandas.MultiIndex.from_tuples(
        [(col, '') if col == 'resource.type' else col.rsplit('.', 1)
         for col in df.columns])

    # Re-order the columns.
    resource_keys = gcloud.monitoring._dataframe._sorted_resource_labels(
        df['resource.labels'].columns)
    sorted_columns = [('resource.type', '')]
    sorted_columns += [('resource.labels', key) for key in resource_keys]
    sorted_columns += sorted(col for col in df.columns
                             if col[0] == 'metric.labels')
    df = df[sorted_columns]

    # Sort the data, and clean up index values, and NaNs.
    df = df.sort_values(sorted_columns).reset_index(drop=True).fillna('')
    return df
