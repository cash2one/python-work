
from os.path import dirname
from os.path import join
import numpy as np


class Bunch(dict):
    """Container object for datasets: dictionary-like object that
       exposes its keys as attributes."""

    def __init__(self, **kwargs):
        dict.__init__(self, kwargs)
        self.__dict__ = self

def load_data(path,n_class=10):

    module_path = dirname(__file__)
    data = np.loadtxt(join(module_path, 'data', path) )

    target = data[:, -1]*10000

    flat_data = data[:, :-1]
    images = flat_data.view()
    #images.shape = (1000000-1,4)

    if n_class < 10:
        idx = target < n_class
        flat_data, target = flat_data[idx], target[idx]
        images = images[idx]

    return Bunch(data=flat_data.astype(np.int64),
                 target=target.astype(np.int64),
                 target_names=np.arange(100000),
                 images=images,
                 DESCR="desc")





