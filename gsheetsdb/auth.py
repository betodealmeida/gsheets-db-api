from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import json

from google.oauth2 import service_account


# Google API scopes for authentication
# https://developers.google.com/chart/interactive/docs/spreadsheets
SCOPES = ['https://spreadsheets.google.com/feeds']


def get_credentials_from_auth(
    service_account_file=None,
    service_account_info=None,
    subject=None,
):
    if service_account_file:
        with open(service_account_file) as fp:
            service_account_info = json.load(fp)

    if not service_account_info:
        return None

    credentials = service_account.Credentials.from_service_account_info(
        service_account_info, scopes=SCOPES, subject=subject)

    return credentials
