# Create a dataset connector with format hdf5

This guide will walk you through the steps to create a new dataset connector with format hdf5.

## Step 1: Implement the hdf5 dataset connector class

Create a new file `sostrades_core/datasets/datasets_connectors/hdf5_datasets_connector.py` and implement the hdf5 dataset connector class. Inherit from `AbstractDatasetsConnector` and implement the following methods:
- `_get_values`
- `_write_values`
- `_get_values_all`
- `_write_dataset`
- `get_datasets_available`

## Step 2: Update the dataset connector factory

Update the file `sostrades_core/datasets/datasets_connectors/datasets_connector_factory.py` to include the hdf5 connector. Add `HDF5` to the `DatasetConnectorType` enum and add an import statement for `hdf5_datasets_connector`.

## Step 3: Update the dataset connector manager

Update the file `sostrades_core/datasets/datasets_connectors/datasets_connector_manager.py` to include the hdf5 connector. Add `HDF5` to the `register_connector` method.

## Step 4: Add l0 test for hdf5 dataset connector

Create a new file `sostrades_core/tests/l0_test_93_hdf5_datasets.py` and add l0 test for hdf5 dataset connector.

## Step 5: Ensure code is documented using Google style

Ensure that the code is documented using Google style.

## Step 6: Ensure ruff and pylint pass

Ensure that ruff passes all rules and pylint passes.
