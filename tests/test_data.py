"""
"""
from mock import patch
from nose import tools as nt

import rogers.data as d


def test_init():
    import rogers.data.features_pb2


def test_feature():
    f = d.feature('asdf')
    nt.assert_is_not_none(f)
    nt.assert_equal(d.value(f.value), 'asdf')
    nt.assert_false(f.value.HasField("list_value"))


def test_feature_list():
    f = d.feature(['asdf', 'asdf'])
    nt.assert_is_not_none(f)
    nt.assert_equal(d.value(f.value), ['asdf', 'asdf'])


def test_feature_struct():
    my_dict = {'asdf': 1, 'foo': 'bar'}
    f = d.feature(my_dict)
    nt.assert_is_not_none(f)
    nt.assert_equal(d.value(f.value), my_dict)


def test_feature_type():
    f = d.feature(1, var_type=d.Feature.Variable.ORDINAL)
    nt.assert_is_not_none(f)
    nt.assert_equal(f.type, d.Feature.Variable.ORDINAL)


def test_feature_model():
    f = d.feature(1, var_mode=d.Feature.Modality.BYTES)
    nt.assert_is_not_none(f)
    nt.assert_equal(f.mode, d.Feature.Modality.BYTES)
