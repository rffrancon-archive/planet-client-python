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
from mock import MagicMock
from mock import DEFAULT
from planet import api
from planet.api.models import Scenes
from planet.api.sync import _SyncTool
from _common import read_fixture


client = MagicMock(name='client', spec=api.Client)


def test_sync_tool(tmpdir):
    # test non-existing destination
    try:
        _SyncTool(client, 'should-not-exist', None, None)
    except ValueError as ve:
        assert str(ve) == 'destination must exist and be a directory'

    # test existing destination, no aoi.geojson
    td = tmpdir.mkdir('sync-dest')
    try:
        _SyncTool(client, td.strpath, None, None)
    except ValueError as ve:
        assert str(ve) == 'no aoi provided and no aoi.geojson file'

    # test existing destination, invalid aoi.geojson
    aoi_file = td.join('aoi.geojson')
    aoi_file.write('not geojson')
    try:
        _SyncTool(client, td.strpath, None, None)
    except ValueError as ve:
        assert str(ve) == '%s does not contain valid JSON' % aoi_file

    td.remove(True)


def test_sync_tool_init(tmpdir):
    td = tmpdir.mkdir('sync-dest')
    aoi = read_fixture('aoi.geojson')
    st = _SyncTool(client, td.strpath, aoi, 'ortho')
    search_json = json.loads(read_fixture('search.geojson'))
    response = MagicMock(spec=Scenes)
    response.get.return_value = search_json
    client.get_scenes_list.return_value = response

    # init w/ no limit should return count from response
    count = st.init()
    assert search_json['count'] == count

    # expect limiting
    count = st.init(limit=10)
    assert 10 == count

    # create a stored 'latest' date and ensure it's used
    latest = '2015-01-25T18:29:09.155671+00:00'
    td.join('sync.json').write(json.dumps({
        'latest': latest
    }))
    st.init()
    args = client.get_scenes_list.call_args
    # args are ((arguments),(kw))
    assert args[1]['acquired.gt'] == latest

    td.remove(True)


def test_sync_tool_sync(tmpdir):
    td = tmpdir.mkdir('sync-dest')
    aoi = read_fixture('aoi.geojson')
    st = _SyncTool(client, td.strpath, aoi, 'ortho')
    search_json = json.loads(read_fixture('search.geojson'))

    class Page:
        def get(self):
            return search_json

    class FakeScenes:
        def iter(self):
            return iter([Page()])

    class FakeBody:
        def __init__(self, val, name):
            self.val = val
            self.name = name

        def __len__(self):
            return len(self.val)

    class FakeGeoTiff:
        def __init__(self, val):
            self.body = FakeBody(val, val)
            self.name = val

        def await(self):
            pass

        def get_body(self):
            return self.body

        def write(self, f, cb):
            cb(self)

    st._scenes = FakeScenes()

    called_back = []

    def callback(name, remaining):
        called_back.append((name, remaining))

    # base - no items remaining, does nothing
    st.remaining = 0
    st.sync(callback)
    assert st.latest is None

    # 5 items to get
    client.fetch_scene_geotiffs.reset_mock()
    st.remaining = 5
    responses = [
        FakeGeoTiff(str(i)) for i in range(st.remaining)
    ]
    client.fetch_scene_geotiffs.return_value = responses

    def run_callbacks(ids, scene_type, callback):
        for r in responses:
            callback(r)
        return DEFAULT

    client.fetch_scene_geotiffs.side_effect = run_callbacks
    st.sync(callback)
    first_five = search_json['features'][:5]
    # should be 5 metadata files
    files = [
        td.join('%s_metadata.json' % f['id']) for f in first_five
    ]
    assert len(first_five) == 5
    assert all([f.exists() for f in files])
    # and tiff requests were made
    args = client.fetch_scene_geotiffs.call_args
    assert args[0] == ([f['id'] for f in first_five], 'ortho')
    # callbacks should be made - arguments are 'tiff', remaining
    assert called_back == [(str(i), 4 - i) for i in range(5)]

    td.remove(True)
