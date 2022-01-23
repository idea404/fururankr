from typing import Optional, Tuple

from sqlalchemy import Column
from structlog import get_logger

logger = get_logger()


class MixIn:
    """Add some functionalities to the Base class via a mixin"""

    def to_dict(self, excludes=()):
        """ORM object to Python dict"""
        return {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k not in excludes
        }

    @classmethod
    def from_dict(cls, dct):
        """Python dict to ORM object"""
        return cls(**dct)

    def _get_all_attributes(self) -> Tuple:
        """Use Python attributes names to select which attributes to update"""
        return tuple([k for k in self.__dict__.keys() if not k.startswith("_")])

    def _get_attributes_for_update(self) -> Tuple:
        """Use columns names mapper to select which attributes to update,
        exclude PKs, FKs, onupdates and relationships.

        Every model could override this method to expose to update its attributes."""
        return tuple(
            [
                cp.key
                for cp in self.__mapper__.column_attrs
                if cp.columns
                and not cp.columns[0].primary_key
                and not cp.columns[0].foreign_keys
                and cp.columns[0].onupdate is None
                and cp.key not in [k for k in self.__mapper__.relationships.keys()]
            ]
        )

    def update_with(self, other, excludes=(), update_nones=True) -> bool:
        """
        Given an instance, update it via an object of the same class.
        This method works by writing all the values in `other` into the attributes in `self`. If it is
         intended to exclude the values that are `None` to be set, use `update_nones=False`. This is
         used by some models to avoid reversing values to None. Default behaviour is full update from `other`
         to `self`.

        Use with care. Set the flags properly according to the behaviour expected for the particular model.
         Check usage of defaulting with `missing` property in schemas. If some properties default to `None`, be sure
         to use the right options in this method for the behaviour expected. Check also `update_with_dict` for
         an alternative.

        Args:
          other (cls): an object of type(cls)
          excludes (sequence): a list of attributes to explicitly exclude from the update
          update_nones (bool): flag to include/exclude `None` values updates
        """
        if isinstance(other, self.__class__):
            was_updated = False
            for attribute in self._get_attributes_for_update():
                if attribute not in excludes:
                    value = getattr(other, attribute)
                    if (update_nones is True and value is None) or value is not None:
                        if getattr(self, attribute) != value:
                            setattr(self, attribute, value)
                            was_updated = True
            return was_updated
        else:
            raise ValueError(f'"other" argument should be a {self.__class__}')

    def update_with_dict(self, other: dict, excludes=()) -> bool:
        """
        Same as `update_with` but takes dictionary as input. Only keys set in `other` are updated in `self`.

        Args:
          other (dict):
          excludes (tuple):

        Returns:
          (cls): an instance of class with modified fields
        """
        was_updated = False
        for k, v in other.items():
            if hasattr(self, k) and k not in excludes and getattr(self, k) != v:
                # If the attribute is a string, get max length from the field definition and truncate the string as appropriate
                field_type = self._get_field_type(k)
                if field_type is not None and field_type.type.python_type == str:
                    v = v[: field_type.type.length] if v is not None else None

                setattr(self, k, v)
                was_updated = True
        return was_updated

    def _get_field_type(self, field_name: str) -> Optional[Column]:
        try:
            return self.__mapper__.column_attrs[field_name].columns[0]
        except KeyError:
            return None

    def has_differences_with(self, other, excludes=()) -> bool:
        """
        Given an instance, check if contains the same values as another of the same type

        Args:
          other (cls): an object of type(cls)
          excludes (sequence): a list of attributes to explicitly exclude from the comparison
        """
        keys = self._get_attributes_for_update()

        for attribute in keys:
            if attribute not in excludes:
                value = getattr(other, attribute)
                if hasattr(self, attribute):
                    if getattr(self, attribute) != value:
                        return True

        return False

    def has_differences_with_dict(self, other, excludes=()) -> bool:
        """
        Given an instance, check if contains the same values as the ones in a dictionary

        Args:
          other (dict): a dictionary
          excludes (sequence): a list of attributes to explicitly exclude from the comparison
        """
        keys = other.keys()

        for attribute in keys:
            if attribute not in excludes:
                value = other[attribute]
                if hasattr(self, attribute):
                    if getattr(self, attribute) != value:
                        return True

        return False

    @classmethod
    def match_by_id(cls, id_, lst, key="id"):
        """Find an instance matching the current object in a list of objects"""
        try:
            return [a for a in lst if getattr(a, key) == id_][0]
        except IndexError:
            msg = f"Cannot find matching object {id_} in list {lst}"
            logger.error(msg)
            raise IndexError(msg)

    def get_update_dict(self, excludes=()):
        """ORM object to Python dict"""
        update_attributes = [
            a
            for a in self._get_attributes_for_update()
            if not a.startswith("_") and a not in excludes
        ]
        return {k: v for k, v in self.__dict__.items() if k in update_attributes}
