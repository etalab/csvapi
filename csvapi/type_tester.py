import re

from agatesql import table as agatesqltable
from agate.data_types.base import DataType
from agate.type_tester import TypeTester
from agate.data_types.boolean import Boolean
from agate.data_types.date import Date
from agate.data_types.date_time import DateTime
from agate.data_types.number import Number
from agate.data_types.text import Text
from agate.data_types.time_delta import TimeDelta
from agate.exceptions import CastError
from sqlalchemy.types import VARCHAR


class Time(DataType):
    def __init__(self, **kwargs):
        super(Time, self).__init__(**kwargs)

    def cast(self, d):
        if re.match(r"^(?:[01]\d|2[0-3]|\d):[0-5]\d$", d):
            # Zero pad hour (case like 9:41)
            if len(d) == 4:
                return Text().cast(f"0{d}")
            return Text().cast(d)
        raise CastError('Can not parse value "%s" as time.' % d)


agatesqltable.SQL_TYPE_MAP[Time] = VARCHAR


def agate_tester():
    return TypeTester(
        types=[Boolean(), Number(), Time(), TimeDelta(), Date(), DateTime(), Text()]
    )
