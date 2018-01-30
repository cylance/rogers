""" Wrappers for generated PB
"""

from .features_pb2 import Feature, Features, Value, List, Struct


def feature(val, var_type=Feature.Variable.CATEGORICAL, var_mode=Feature.Modality.STATIC):
    """ Create feature message
    :param val:
    :param var_type:
    :param var_mode:
    :return:
    """
    f = Feature()
    f.mode = var_mode
    f.type = var_type
    f.value.CopyFrom(to_value(val))
    return f


def to_value(val):
    """ Convert python type to value
    :param val:
    :return value
    """
    v = Value()
    if isinstance(val, list):
        l = List()
        l.value.extend([to_value(i) for i in val])
        v.list_value.CopyFrom(l)
    elif isinstance(val, dict):
        s = Struct()
        for k in val.keys():
            s.value[k].CopyFrom(to_value(val[k]))
        v.struct_value.CopyFrom(s)
    elif isinstance(val, str):
        v.string_value = val
    elif isinstance(val, bool):
        v.bool_value = val
    elif isinstance(val, int):
        v.int_value = val
    elif isinstance(val, float):
        v.double_value = val
    else:
        raise TypeError("unsupported type %s" % type(val))
    return v


def value(val):
    """ Convert value to python type
    :param val:
    :return:  list, string, number, bool, or none value
    """
    if val.HasField("string_value"):
        return val.string_value
    elif val.HasField("int_value"):
        return val.int_value
    elif val.HasField("bool_value"):
        return val.bool_value
    elif val.HasField("double_value"):
        return val.double_value
    elif val.HasField("struct_value"):
        return {k: value(val.struct_value.value[k]) for k in val.struct_value.value}
    else:
        return [value(v) for v in val.list_value.value]
