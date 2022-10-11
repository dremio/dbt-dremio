from dataclasses import dataclass
from typing import Optional
from abc import ABC, abstractmethod

from dbt.events import AdapterLogger
logger = AdapterLogger("dremio")

@dataclass
class DremioAuthentication:
    username: Optional[str] = None

    @classmethod
    def build(cls, username: None, password: None, pat: None):
        if password != None:
            return DremioPasswordAuthentication(username, password, token = None)
        return DremioPatAuthentication(username, pat)
    
    @classmethod
    def build_headers(cls, authorization_field):
        headers = {'Content-Type':'application/json', 'Authorization': '{authorization_token}'.format(authorization_token=authorization_field)}
        return headers

    @abstractmethod
    def get_headers(self):
        pass

@dataclass
class DremioPasswordAuthentication(DremioAuthentication):
    password: Optional[str] = None
    token: Optional[str] = None

    def get_headers(self):
        authorization_field = '_dremio{authorization_token}'.format(authorization_token=self.token)
        return self.build_headers(authorization_field)

@dataclass
class DremioPatAuthentication(DremioAuthentication):
    pat: Optional[str] = None

    def get_headers(self):
        authorization_field = 'Bearer {authorization_token}'.format(authorization_token=self.pat)
        return self.build_headers(authorization_field)


