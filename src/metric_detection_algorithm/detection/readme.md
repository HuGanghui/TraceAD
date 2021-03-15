调用

```python
from test import metric_test
seq = [
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
        [0.0, 1.0, 0.2, 0.5, 0.7],
    ]
    kpi_names = ['system.cpu.i_dle','system.load.5','system.load.1.pct','system.load.1','system.cpu.pct_usage','system.load.norm.1','system.cpu.user']
    ori_system = 'a'

res_kpi_name = metric_test(seq, kpi_names, ori_system)

"""

:param seq:     	 list|多变量时序，窗口长度为5，长度为kpi数量

:param kpi_names:  list|kpi的名的集合，长度同seq

:param ori_system: str |a or b

:return: kpi根因指标集合

  """
```

