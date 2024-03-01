# Wrap a model

## Definition
A SosWrap is a model wrapper for SoSTrades application

Here is the minimal working example of a SoSWrap :
```python
class MyCustomWrap(SoSWrapp):

```

## Base class
The wrap should inherit from

```{eval-rst}
.. autoclass:: sostrades_core.execution_engine.sos_wrapp::SoSWrapp
```

## Ontology data

* `label` : 
* `type` : 
* `source` : 
* `validated` : 
* `validated_by` : 
* `last_modification_date` : 
* `category` : 
* `definition` : 
* `icon` : 
* `version` : 

## Maturity

## DESC_IN
Description of inputs

* `type` : mandatory could be : `'float'`, `'int'`, `'dict'`, `'dataframe'`, `'bool'`
* `subtype_descriptor` (or `dataframe_descriptor`) : if the variable is a dict/list (or dataframe), gives the types (or descriptor) of the sub-elements (or columns).
* `default` : if the variable has a default value. The default must be the same type as the type!!!
* `unit` : (string) unity of the variable used for the ontology
* `user_level` : (optional) to filter the display in the GUI  (1=Standard by default, 2=Advanced, 3=Expert)
* Other options are available …

## Dataframe Descriptors
Here is an example dataframe descriptor.
Tuples define type, range or possible values, editable or not.

```python
TransportChoiceData = {
    "var_name": "transport_choice",
    "type": "dataframe",
    "dataframe_descriptor" : {
        Years : ('int', YearsBoundaries, True),
        ProductName : ('string', None, True),
        TypeName : ('string', TransportPossibleValues, True),
        PercentageName : ('float', None, True),
    }
}
```

## Subtype descriptor for dicts
Here is an example of dict subtype descriptors.

```python
"dict_of_dict_in" : {"type": "dict", ProxyDiscipline.SUBTYPE: {"dict": {"dict": "float"}}, "user_level": 1}
"dict_of_dataframe_in" : {"type": "dict", ProxyDiscipline.SUBTYPE: {"dict": {"dataframe"}}, "user_level": 1}
```


## Run method

```python
# Method that runs the model
def run(self):
    """
    Method that runs the model
    """
    # get input of discipline
    param_in = self.get_sosdisc_inputs()

    # performs the "computation"
    x = param_in['x']
    a = param_in['a']
    b = param_in['b']

    y = a * x + b

    output_values = {'y': y}

    # store data
    self.store_sos_outputs_values(output_values)
```

* The function get_sosdisc_inputs(variable name) returns the value of the variable in the data manager. It can be used without arguments : return a dict with all keys and values of the DESC_IN
* The core of the model can be written here or loaded from an external model 
* Output values are stored in a dictionary {variable_name : value} with the value coming from the model
* The dictionary is sent to the data manager with the function store_sos_output_values(dict_values)