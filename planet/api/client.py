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

import base64
import json
from .dispatch import RequestsDispatcher
from . import auth
from .exceptions import InvalidIdentity
from . import models
from .utils import check_status


class Client(object):

    def __init__(self, api_key=None, base_url='https://api.planet.com/v0/',
                 workers=4):
        api_key = api_key or auth.find_api_key()
        self.auth = api_key and auth.APIKey(api_key)
        self.base_url = base_url
        self.dispatcher = RequestsDispatcher(workers)

    def _url(self, path):
        if path.startswith('http'):
            url = path
        else:
            url = self.base_url + path
        return url

    def _request(self, path, body_type=models.JSON, params=None, auth=None):
        return models.Request(self._url(path), auth or self.auth, params,
                              body_type)

    def _get(self, path, body_type=models.JSON, params=None, callback=None):
        request = self._request(path, body_type, params)
        response = self.dispatcher.response(request)
        if callback:
            response.get_body_async(callback)
        return response

    def _download_many(self, paths, params, callback):
        return [self._get(path, params=params, callback=callback)
                for path in paths]

    def login(self, identity, credentials):
        result = self.dispatcher.session.post(self._url('auth/login'), {
            'email': identity,
            'password': credentials
        }).result()
        if result.status_code == 400:
            # do our best to get something out to the user
            msg = result.text
            try:
                msg = json.loads(result.text)['message']
            finally:
                raise InvalidIdentity(msg)
        jwt = result.text
        payload = jwt.split('.')[1]
        rem = len(payload) % 4
        if rem > 0:
            payload += '=' * (4 - rem)
        payload = base64.urlsafe_b64decode(payload.encode('utf-8'))
        return json.loads(payload.decode('utf-8'))

    def get_scenes_list(self, scene_type='ortho', order_by=None, count=None,
                        intersects=None, **filters):
        params = {
            'order_by': order_by,
            'count': count,
            'intersects': intersects
        }
        params.update(**filters)
        return self._get('scenes/%s' % scene_type,
                         models.Scenes, params=params).get_body()

    def get_scene_metadata(self, scene_id, scene_type='ortho'):
        """
        Get metadata for a given scene.

        .. todo:: Generalize to accept multiple scene ids.
        """
        return self._get('scenes/%s/%s' % (scene_type, scene_id)).get_body()

    def fetch_scene_geotiffs(self, scene_ids, scene_type='ortho',
                             product='visual', callback=None):
        params = {
            'product': product
        }
        paths = ['scenes/%s/%s/full' % (scene_type, sid) for sid in scene_ids]
        return self._download_many(paths, params, callback)

    def fetch_scene_thumbnails(self, scene_ids, scene_type='ortho', size='md',
                               fmt='png', callback=None):
        params = {
            'size': size,
            'format': fmt
        }
        paths = ['scenes/%s/%s/thumb' % (scene_type, sid) for sid in scene_ids]
        return self._download_many(paths, params, callback)

    def list_mosaics(self):
        """
        List all mosaics.

        .. todo:: Pagination
        """
        return self._get('mosaics').get_body()

    def get_mosaic(self, name):
        """
        Get metadata for a given mosaic.

        :param name:
            Mosaic name as returned by `list_mosaics`.
        """
        return self._get('mosaics/%s' % name).get_body()

    def get_workspaces(self):
        return self._get('workspaces').get_body()

    def get_workspace(self, id):
        return self._get('workspaces/%s' % id).get_body()

    def set_workspace(self, workspace, id=None):
        if id:
            workspace['id'] = id
            url = 'workspaces/%s' % id
            method = 'PUT'
        else:
            'id' in workspace and workspace.pop('id')
            url = 'workspaces/'
            method = 'POST'
        result = self.dispatcher.dispatch_request(method, self.base_url + url,
                                                  data=json.dumps(workspace),
                                                  auth=self.auth)
        check_status(result)
        return models.JSON(result, self.dispatcher)
