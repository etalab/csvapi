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
        if d is None:
            return d
        if re.match(r"^(?:[01]\d|2[0-3]|\d):[0-5]\d$", str(d)):
            return Text().cast(d)
        raise CastError('Can not parse value "%s" as time.' % d)


class SirenSiret(DataType):
    # Detect a SIREN or SIRET number
    def __init__(self):
        super(SirenSiret, self).__init__()

    def cast(self, d):
        if d is None:
            return d
        if is_valid_siret(d) or is_valid_siren(d):
            return Text().cast(d)
        raise CastError('Can not parse value "%s" as a SIREN or SIRET.' % d)


# agatesql needs to know the SQL equivalent of a type.
# Tell agatesql how our custom types should be converted in SQL.
#
# Reference:
# https://github.com/wireservice/agate-sql/blob/7466073d81289323851c21817ea33170e36ce2a5/agatesql/table.py#L21-L28
agatesqltable.SQL_TYPE_MAP[Time] = VARCHAR
agatesqltable.SQL_TYPE_MAP[SirenSiret] = VARCHAR


def agate_tester():
    # Override the original list of type checkers present in agate
    # to detect types.
    #
    # Original list here:
    # https://github.com/wireservice/agate/blob/e3078dca8b3566e8408e65981f79918c2f36f9fe/agate/type_tester.py#L64-L71
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
