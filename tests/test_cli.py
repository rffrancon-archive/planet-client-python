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

'''
Command line specific tests - the client should be completely mocked and the
focus should be on asserting any CLI logic prior to client method invocation

lower level lib/client tests go in the test_mod suite
'''

import os
import json

from click import ClickException
from click.testing import CliRunner

from mock import MagicMock

import planet
from planet import api
from planet.api import models
from planet import scripts
from _common import read_fixture
from _common import clone


client = MagicMock(name='client', spec=api.Client)
scripts.client = lambda: client
runner = CliRunner()


def assert_success(result, expected):
    assert result.exit_code == 0
    assert json.loads(result.output) == json.loads(expected)


def assert_cli_exception(cause, expected):
    def thrower():
        raise cause
    try:
        scripts.call_and_wrap(thrower)
        assert False, 'did not throw'
    except ClickException as ex:
        assert str(ex) == expected


def test_build_conditions():
    workspace = json.loads(read_fixture('workspace.json'))
    c = scripts.build_conditions(workspace)
    assert c['image_statistics.snr.gt'] == 10
    assert c['sat.off_nadir.lte'] == 25
    assert c['sat.alt.gte'] == 200
    assert c['sat.alt.lte'] == 650
    assert c['age.lte'] == 63072000


def test_exception_translation():
    assert_cli_exception(api.exceptions.BadQuery('bogus'), 'BadQuery: bogus')
    assert_cli_exception(api.exceptions.APIException('911: alert'),
                         "Unexpected response: 911: alert")


def test_version_flag():

    results = runner.invoke(scripts.cli, ['--version'])
    assert results.output == "%s\n" % planet.__version__


def test_workers_flag():
    assert 'workers' not in scripts.client_params
    runner.invoke(scripts.cli, ['--workers', '19', 'search'])
    assert 'workers' in scripts.client_params
    assert scripts.client_params['workers'] == 19


def test_api_key_flag():
    runner.invoke(scripts.cli, ['-k', 'shazbot', 'search'])
    assert 'api_key' in scripts.client_params
    assert scripts.client_params['api_key'] == 'shazbot'


def test_search():

    expected = read_fixture('search.geojson')

    response = MagicMock(spec=models.JSON)
    response.get_raw.return_value = expected

    client.get_scenes_list.return_value = response

    result = runner.invoke(scripts.cli, ['search'])

    assert_success(result, expected)


def test_search_by_aoi():

    aoi = read_fixture('search-by-aoi.geojson')
    expected = read_fixture('search-by-aoi.geojson')

    response = MagicMock(spec=models.JSON)
    response.get_raw.return_value = expected

    client.get_scenes_list.return_value = response

    # input kwarg simulates stdin
    result = runner.invoke(scripts.cli, ['search'], input=aoi)

    assert_success(result, expected)


def test_metadata():

    # Read in fixture
    expected = read_fixture('20150615_190229_0905.geojson')

    # Construct a response from the fixture
    response = MagicMock(spec=models.JSON)
    response.get_raw.return_value = expected

    # Construct the return response for the client method
    client.get_scene_metadata.return_value = response

    result = runner.invoke(scripts.cli, ['metadata', '20150615_190229_0905'])

    assert_success(result, expected)


def test_download():

    result = runner.invoke(scripts.cli, ['download', '20150615_190229_0905'])
    assert result.exit_code == 0


def test_init():
    # monkey patch the storage file
    test_file = '.test_planet_json'
    api.utils._planet_json_file = lambda: test_file
    client.login.return_value = {
        'api_key': 'SECRIT'
    }
    try:
        result = runner.invoke(scripts.cli, ['init',
                                             '--email', 'bil@ly',
                                             '--password', 'secret'])
        assert result.exit_code == 0
        with open(test_file) as fp:
            data = json.loads(fp.read())
        assert data['key'] == 'SECRIT'
    finally:
        os.unlink(test_file)


def _set_workspace(workspace, *args, **kw):
    response = MagicMock(spec=models.JSON)
    response.get_raw.return_value = '{"status": "OK"}'

    client.set_workspace.return_value = response
    client.set_workspace.reset_mock()
    args = ['set-workspace'] + list(args)
    if workspace:
        args += [json.dumps(workspace)]
    result = runner.invoke(scripts.cli, args, input=kw.get('input', None))
    assert result.exit_code == kw.get('expected_status', 0)


def test_workspace_create_no_id():

    workspace = json.loads(read_fixture('workspace.json'))
    workspace.pop('id')
    expected = clone(workspace)
    _set_workspace(workspace)
    client.set_workspace.assert_called_once_with(expected, None)


def test_workspace_create_from_existing():

    workspace = json.loads(read_fixture('workspace.json'))
    expected = clone(workspace)
    _set_workspace(workspace, '--create')
    client.set_workspace.assert_called_once_with(expected, None)


def test_workspace_update_from_existing_with_id():

    workspace = json.loads(read_fixture('workspace.json'))
    expected = clone(workspace)
    _set_workspace(workspace, '--id', '12345')
    client.set_workspace.assert_called_once_with(expected, '12345')


def test_workspace_update_stdin():
    workspace = json.loads(read_fixture('workspace.json'))
    expected = clone(workspace)
    _set_workspace(workspace)
    client.set_workspace.assert_called_once_with(expected, workspace['id'])
