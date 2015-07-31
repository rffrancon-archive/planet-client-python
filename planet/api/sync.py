# Copyright 2015 Planet Labs, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import json
import os
from os import path
from .utils import write_to_file
from .utils import strp_timestamp
from .utils import strf_timestamp


class _SyncTool(object):

    def __init__(self, client, destination, aoi, scene_type, **filters):
        self.client = client
        self.destination = destination
        self.aoi = aoi
        self.scene_type = scene_type
        self.filters = filters
        self._init()
        self.latest = None
        self.transferred = 0
        self.sync_file = path.join(self.destination, 'sync.json')

    def await_futures(self, futures):
        for f in futures:
            f.await()

    def _init(self):
        dest = self.destination
        if not path.exists(dest) or not path.isdir(dest):
            raise ValueError('destination must exist and be a directory')
        if self.aoi is None:
            aoi_file = path.join(dest, 'aoi.geojson')
            if not path.exists(aoi_file):
                raise ValueError('no aoi provided and no aoi.geojson file')
            with open(aoi_file) as fp:
                try:
                    self.aoi = json.loads(fp.read())
                except ValueError:
                    msg = '%s does not contain valid JSON' % aoi_file
                    raise ValueError(msg)

    def _read_sync_file(self):
        if path.exists(self.sync_file):
            with open(self.sync_file) as fp:
                sync = json.loads(fp.read())
        else:
            sync = {}
        return sync

    def init(self, limit=-1):
        sync = self._read_sync_file()
        if 'latest' in sync:
            self.filters['acquired.gt'] = sync['latest']

        resp = self.client.get_scenes_list(scene_type=self.scene_type,
                                           intersects=self.aoi,
                                           count=100,
                                           order_by='acquired asc',
                                           **self.filters)
        self._scenes = resp
        count = resp.get()['count']
        self.remaining = count if limit < 0 else limit
        return self.remaining

    def sync(self, callback):
        def _callback(arg):
            if not isinstance(arg, int):
                self.remaining -= 1
                callback(arg.name, self.remaining)
        write_callback = write_to_file(self.destination, _callback)
        for page in self._scenes.iter():
            features = page.get()['features'][:self.remaining]
            if not features:
                break
            self._sync_features(features, write_callback)
            if self.remaining <= 0:
                break
        if self.latest:
            sync = self._read_sync_file()
            sync['latest'] = strf_timestamp(self.latest)
            with open(self.sync_file, 'wb') as fp:
                fp.write(json.dumps(sync, indent=2).encode('utf-8'))

    def _sync_features(self, features, callback):
        ids = [f['id'] for f in features]

        futures = self.client.fetch_scene_geotiffs(
            ids, self.scene_type, callback=callback
        )
        for f in features:
            metadata = os.path.join(self.destination,
                                    '%s_metadata.json' % f['id'])
            with open(metadata, 'wb') as fp:
                fp.write(json.dumps(f, indent=2).encode('utf-8'))
        self.await_futures(futures)
        self.transferred += sum([len(r.get_body()) for r in futures])
        recent = max([
            strp_timestamp(f['properties']['acquired']) for f in features]
        )
        self.latest = max(self.latest, recent) if self.latest else recent
