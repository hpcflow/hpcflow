Contributor how to guides
=========================

Adding class methods to the ``ValueSequence`` and ``MultiPathSequence`` classes
-------------------------------------------------------------------------------

Adding class methods to :py:class:`hpcflow.app.ValueSequence`
#############################################################

``ValueSequence`` exposes class methods that can be used to generate sequences of values (i.e. multiple elements) within a task. Within a YAML workflow template, the requirement to generate a sequence via a class method is written using a double-colon syntax as follows:

.. code-block:: yaml

    tasks:
      - objective: t1
        sequences:
          - path: inputs.p1
            values::from_range:
              start: 0
              stop: 10
              step: 1

In the case above, we are telling |app_name| to use the method :py:meth:`hpcflow.app.ValueSequence.from_range` to generate multiple elements for the task. The inner block containing ``start``, ``stop``, and ``step`` is then passed as a ``dict`` of keyword arguments to that class method.

Follow these steps to add a new sequence-generating class method to :py:class:`hpcflow.app.ValueSequence`:

1. Consider how to name your new method. For consistency with existing methods, please consider a name that is prefixed by ``from...``. For this example, let's consider a method named ``from_my_new_method``.

2. Add a new method that generates a list of values using your new approach, named ``_values_from_my_new_method``` (i.e. your new method name, prefixed by ``_values_``). If your new technique is parametrised by two arguments, ``arg_1`` (an integer), and ``arg_2`` (a list of strings), the signature of this method should look like this (the ``**kwargs`` is optional—consider if it is necessary to include):

   .. code-block:: python
 
       @classmethod
       def _values_from_my_new_method(
           cls,
           arg_1: int,
           arg_2: list[str],
           **kwargs,
       ) -> Self:
           pass # implementation here

3. Add a new method that can be used within the API:

   .. code-block:: python
 
       @classmethod
       def from_my_new_method(
           cls,
           path: str,
           arg_1: int,
           arg_2: list[str],
           nesting_order: float = 0,
           label: str | int | None = None,          
           **kwargs,
       ) -> Self:
           """
           Build a sequence from ...
           """
           args = {"arg_1": arg_1, "arg_2": arg_2, **kwargs}
           values = cls._values_from_my_new_method(**args)
           obj = cls(values=values, path=path, nesting_order=nesting_order, label=label)
           obj._values_method = "from_my_new_method"
           obj._values_method_args = args
           return obj
           
   Note that in addition to ``arg_1`` and ``arg_2``, this method signature must include some positional and keyword arguments: ``path``, ``nesting_order``, and ``label``, which  should be passed directly to the constructor. We use the method added previously (``_values_from_my_new_method``, in this case) to generate the values, and then pass those values  on to the constructor. Note also that after object construction, we must assign two attributes:
 
   - ``_values_method`` which should be the name of this method
   - ``_values_method_args``, which should be a mapping of argument names and values used to parametrise the value-generating method (``_values_from_my_new_method``, in this case).

4. Write some tests. Please include somewhere within ``hpcflow/tests/unit`` at least one test to convince yourself that your new method generates the correct sequence of values:

   .. code-block:: python

    from hpcflow.app import app as hf

    def test_sequence_from_my_new_method():
        seq = hf.ValueSequence.from_my_new_method(path="inputs.p1", arg_1=9, arg_2=['a', 'b'])
        
        # check the expected number of values generated:
        assert len(seq.values) == 2
        
        # check the expected values, if possible:
        assert seq.values == ["val_a", "val_b"]

        # check the correct attributes are set:
        assert seq._values_method == "from_my_new_method"
        assert seq._values_method_args == {"arg_1": 9, "arg_2": ['a', 'b']}


Adding class methods to :py:class:`hpcflow.app.MultiPathSequence`
#################################################################

``MultiPathSequence`` exposes class methods that can be used to generate multiple sequences of values (i.e. multiple elements) within a task, corresponding to multiple paths (i.e. inputs or resources). Within a YAML workflow template, the requirement to generate a multi-path sequence via a class method is written using a double-colon syntax as follows:

.. code-block:: yaml

    tasks:
      - objective: t1
        multi_path_sequences:
          - paths: [inputs.p1, inputs.p2]
            values::from_latin_hypercube:
              num_samples: 5

In the case above, we are telling |app_name| to use the method :py:meth:`hpcflow.app.MultiPathSequence.from_latin_hypercube` to generate five elements for the task, by combining five values for the input ``p1`` with five values for the input ``p2``, where all ten values are generated at the same time via a Latin hypercube sampling. The inner block containing ``num_samples``, is passed as a ``dict`` of keyword arguments to that class method.

The same process as above can be used for adding new class methods to ``MultiPathSequence``, with two exceptions. Firstly, a ``paths`` postitional argument (note: plural) must be specified in the values-generating method as defined in step 2. above. Thus the method should look like this:

.. code-block:: python

    @classmethod
    def _values_from_my_new_method(
        cls,
        paths: Seqeuence[str], # <- note additional `paths` argument
        arg_1: int,
        arg_2: list[str],
        **kwargs,
    ) -> Self:
        pass # implementation here

The reason for including the ``paths`` argument is so we can know, for example, for how many paths the ``MultiPathSequence`` should generate values for.

Secondly, the ``path`` (note: singular) argument in the public-facing method (``from_my_new_method`` in this case) should be replaced by ``paths`` (note: plural), which should have the type annotation: ``Sequence[str]``.
