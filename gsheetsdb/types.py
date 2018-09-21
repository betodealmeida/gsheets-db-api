from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from enum import Enum


class Type(Enum):
    STRING = 'string'
    NUMBER = 'number'
    BOOLEAN = 'boolean'
    DATE = 'date'
    DATETIME = 'datetime'
    TIMEOFDAY = 'timeofday'
