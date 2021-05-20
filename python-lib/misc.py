# -*- coding: utf-8 -*-

import numpy as np


def is_empty(val):
    return np.isnan(val) if isinstance(val, float) else not val
