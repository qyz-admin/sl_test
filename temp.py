# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import numpy as np

s = pd.Series([27.2, 27.65, 27.70, 28])

datas = pd.date_range('20190401', periods=4)
s2 = pd.Series([27.2, 27.65, 27.70, 28], index=datas)
s2.name = '海底捞股价'
