import copy

import numpy as np


class Parameter:

    def __init__(self, name):
        self.name = name


class Input(Parameter):
    def __init__(self, name):
        super().__init__(name)

    def __repr__(self):
        return f'<Input({self.name})>'

    def __eq__(self, other):
        """Two Inputs are considered equal if their names are equal."""
        if isinstance(other, Input):
            if self.name == other.name:
                return True
        return False


class Output(Parameter):
    def __init__(self, name):
        super().__init__(name)

    def __repr__(self):
        return f'<Output({self.name})>'


class ParameterValue:
    """ParameterValue"""

    def __init__(self, parameter, value):
        self.parameter = parameter
        self.value = value


class InputValue:

    def __init__(self, input_parameter, value):
        self.input_parameter = input_parameter
        self.value = value

    def __repr__(self):
        return f'<InputValue(input={self.input_parameter.name}, value={self.value})>'

    def hydrate(self):
        # Maybe this method would be used to fully load the value from persistent?
        # And also a dehydrate method to remove value from memory, and/or sync changes
        # to persistent?

        # Thinking in terms of importing parameter values. How would a parameter value be
        # loaded into the new workflow (in-memory and persistent)?
        pass


class InputValueSequence:

    def __init__(self, parameter, values, base_value=None, address=None):

        if base_value is not None:
            if not isinstance(base_value, InputValue):
                base_value = InputValue(parameter, base_value)

        self.parameter = parameter
        self.values = values
        self.base_value = base_value
        self.address = address

        if (address is not None and base_value is None) or (
                base_value is not None and address is None):
            msg = (f'Either specify both of `base` and `address`, or neither.')
            raise ValueError(msg)

        self.check_address_exists(address)

    def __repr__(self):
        return f'<InputValueSequence(parameter={self.parameter}, base_value={self.base_value}, values={self.values}, address={self.address})>'

    def check_address_exists(self, address):
        """Check a given nested dict/list "address" resolves to somewhere within
        `self.base_value`."""
        if address:
            sub_val = self.base_value.value
            for i in address:
                try:
                    sub_val = sub_val[i]
                except (IndexError, KeyError, TypeError):
                    msg = (f'Address {self.address} does not exist in the base '
                           f'value: {self.base_value.value}')
                    raise ValueError(msg)

    def resolve_value(self, value):

        if not self.base:
            return copy.deepcopy(value)

        base_value = copy.deepcopy(self.base_value.value)
        sub_val = base_value
        for i in self.address[:-1]:
            sub_val = sub_val[i]
        sub_val[self.address[-1]] = value

        return base_value

    @classmethod
    def from_range(cls, parameter, start, stop, step=1, base_value=None, address=None):
        if isinstance(step, int):
            return cls(
                parameter,
                base_value=base_value,
                values=list(np.arange(start, stop, step)),
                address=address,
            )
        else:
            # Use linspace for non-integer step, as recommended by Numpy:
            return cls.from_linear_space(
                parameter,
                start,
                stop,
                base_value=base_value,
                num=int((stop - start) / step),
                address=address,
                endpoint=False,
            )

    @classmethod
    def from_linear_space(cls, parameter, start, stop, base_value=None, num=50, address=None, **kwargs):
        values = list(np.linspace(start, stop, num=num, **kwargs))
        return cls(parameter, values, base_value=base_value, address=address)
