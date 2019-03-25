# -*- coding: utf-8 -*-
import requests
import json


def post_json(url, data):
    resp = requests.post(url, data=json.dumps(data))
    return resp.json()
