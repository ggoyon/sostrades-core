# Create a dataset connector with format hdf5

This guide will walk you through the steps to create a dataset connector with format hdf5.

## Implement the hdf5 dataset connector class

1. Create a new file `sostrades_core/datasets/datasets_connectors/hdf5_datasets_connector.py`.
2. Implement the hdf5 dataset connector class.
3. Include methods for reading and writing hdf5 datasets.
4. Use h5py library for hdf5 operations.

## Update the dataset connector factory

1. Open the file `sostrades_core/datasets/datasets_connectors/datasets_connector_factory.py`.
2. Add hdf5 connector to `DatasetConnectorType` enum.
3. Update `get_connector` method to handle hdf5 connector.

## Update the dataset connector manager

1. Open the file `sostrades_core/datasets/datasets_connectors/datasets_connector_manager.py`.
2. Add hdf5 connector to `register_connector` method.
3. Update `get_connector` method to handle hdf5 connector.

## Example usage

Here is an example of how to use the hdf5 dataset connector:

```python
from sostrades_core.datasets.datasets_connectors.datasets_connector_factory import DatasetsConnectorFactory, DatasetConnectorType

# Create an instance of the hdf5 dataset connector
connector = DatasetsConnectorFactory.get_connector(
    connector_identifier="hdf5_connector",
    connector_type=DatasetConnectorType.HDF5,
    file_path="path/to/hdf5/file.h5"
)

# Define the dataset identifier
dataset_identifier = "example_dataset"

# Define the data to write
data_to_write = {
    "data1": [1, 2, 3],
    "data2": [4, 5, 6]
}

# Define the data types
data_types_dict = {
    "data1": "list",
    "data2": "list"
}

# Write the data to the dataset
connector.write_dataset(
    dataset_identifier=dataset_identifier,
    values_to_write=data_to_write,
    data_types_dict=data_types_dict,
    create_if_not_exists=True,
    override=True
)

# Read the data from the dataset
data_to_get = {
    "data1": "list",
    "data2": "list"
}
read_data = connector.get_values(
    dataset_identifier=dataset_identifier,
    data_to_get=data_to_get
)

print(read_data)
```

This example demonstrates how to create an instance of the hdf5 dataset connector, write data to a dataset, and read data from a dataset.
