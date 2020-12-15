# -*- coding: utf-8 -*-

import numpy as np


def is_empty(val):
    if isinstance(val, float):
        return np.isnan(val)
    else:
        return not val
