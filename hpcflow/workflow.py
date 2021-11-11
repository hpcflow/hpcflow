from abc import ABC, abstractmethod
from pathlib import Path


class PersistentDataFormat(ABC):

    @abstractmethod
    def write_workflow(self, workflow):
        pass

    @classmethod
    def load_workflow(cls, path):
        data_format_cls = cls.get_format(path).get_format(path)
        data_format_cls.load_workflow(path)

    @staticmethod
    def get_format(path):
        path = Path(path)
        return DATA_FORMAT_LOOKUP[path.suffix.lstrip('.')]


class ZarrDataFormat(PersistentDataFormat):

    def __init__(self):
        pass

    def write_workflow(self, workflow):
        pass

    @classmethod
    def load_workflow(cls, path):
        print('loading Zarr workflow')

    def add_task(self, task):
        pass

    def get_tasks(self):
        pass


class HDF5DataFormat(PersistentDataFormat):

    def __init__(self, path):
        pass

    def write_workflow(self, workflow):
        pass

    @classmethod
    def load_workflow(cls, path):
        print('loading HDF5 workflow')


class JSONDataFormat(PersistentDataFormat):

    def __init__(self):
        pass

    def write_workflow(self, workflow):
        pass

    @classmethod
    def load_workflow(cls, path):
        print('loading JSON workflow')


DATA_FORMAT_LOOKUP = {
    'zarr': ZarrDataFormat,
    'hdf5': HDF5DataFormat,
    'h5': HDF5DataFormat,
    'hdf': HDF5DataFormat,
    'json': JSONDataFormat,
}


class WorkflowTemplate:
    """Represents a non-persistent workflow, from which a persistent `Workflow` can be made."""

    def __init__(self, tasks):
        self.tasks = tasks

    def make_workflow(self, data_format='zarr'):

        data_format_cls = DATA_FORMAT_LOOKUP[data_format.lower()]
        data_format = data_format_cls()

        # Write a persistent workflow file:
        path = None

        # Load workflow from persistent file:
        workflow = Workflow(path)

        return workflow


class Workflow:

    # Question: how much data should be stored in-memory?
    # - if we don's store anything, then every interaction would require disk/network IO => not so good
    # - but don't want to store everything
    # - so need to decide optimal balance
    # - for everything stored in memory, need to decide strategy for updating persistent data on workflow changes
    # - and doing so in a way compatible with concurrent writes (for Zarr) of different element data
    # - perhaps ensure the workflow alwyas has a consistent number of e.g. elements objects, but they might not be
    #   populated with data yet? Elements could have a state property (not stored in memory), which checks if the data is available

    def __init__(self, path):

        data = PersistentDataFormat.load_workflow(path)

        self.path = path
        self.data = data

    @property
    def tasks(self):
        return self.data.get_tasks()
