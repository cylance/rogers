"""
"""
from mock import patch

import rogers.data as d


def test_init():
    import rogers.data.features_pb2


def test_feature():
    f = d.feature('asdf')
    assert f is not None
    assert d.value(f.value) == 'asdf'
    assert f.value.HasField("list_value") is False


def test_feature_list():
    f = d.feature(['asdf', 'asdf'])
    assert f is not None
    assert d.value(f.value) == ['asdf', 'asdf']


def test_feature_struct():
    my_dict = {'asdf': 1, 'foo': 'bar'}
    f = d.feature(my_dict)
    assert f is not None
    assert d.value(f.value) == my_dict


def test_feature_type():
    f = d.feature(1, var_type=d.Feature.Variable.ORDINAL)
    assert f is not None
    assert f.type == d.Feature.Variable.ORDINAL


def test_feature_model():
    f = d.feature(1, var_mode=d.Feature.Modality.BYTES)
    assert f is not None
    assert f.mode == d.Feature.Modality.BYTES
