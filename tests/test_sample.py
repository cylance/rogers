"""
"""
from mock import patch


def test_init():
    import rogers.sample


def test_pe():
    from rogers.sample import Sample
    from rogers.sample.pe import PE

    s = PE()
    msg = s.serialize()
    s1 = Sample.deserialize(msg)
    assert s.features == s1.features
