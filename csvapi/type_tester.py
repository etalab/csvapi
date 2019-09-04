import re

from agate.data_types.base import DataType
from agate.data_types.boolean import Boolean
from agate.data_types.date import Date
from agate.data_types.date_time import DateTime
from agate.data_types.number import Number
from agate.data_types.text import Text
from agate.data_types.time_delta import TimeDelta
from agate.exceptions import CastError
from agate.type_tester import TypeTester

from agatesql import table as agatesqltable

from sqlalchemy.types import VARCHAR

from stdnum.fr.siren import is_valid as is_valid_siren
from stdnum.fr.siret import is_valid as is_valid_siret


class Time(DataType):
    # Detect an hour minute string.
    # Examples: 12:20, 9:50, 23:30
    def __init__(self, **kwargs):
        super(Time, self).__init__(**kwargs)

    def cast(self, d):
        if re.match(r"^(?:[01]\d|2[0-3]|\d):[0-5]\d$", d):
            # Zero pad hour (case like 9:41)
            if len(d) == 4:
                return Text().cast(f"0{d}")
            return Text().cast(d)
        raise CastError('Can not parse value "%s" as time.' % d)


class SirenSiret(DataType):
    # Detect a SIREN or SIRET number
    def __init__(self):
        super(SirenSiret, self).__init__()

    def cast(self, d):
        if is_valid_siret(d) or is_valid_siren(d):
            return Text().cast(d)
        raise CastError('Can not parse value "%s" as a SIREN or SIRET.' % d)


agatesqltable.SQL_TYPE_MAP[Time] = VARCHAR
agatesqltable.SQL_TYPE_MAP[SirenSiret] = VARCHAR


def agate_tester():
    return TypeTester(
        types=[
            Boolean(),
            SirenSiret(),
            Number(),
            Time(),
            TimeDelta(),
            Date(),
            DateTime(),
            Text(),
        ]
    )
