"""Base models for the package"""

import abc
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


def make_model_config(**kwargs: Any) -> ConfigDict:
    """Return a Pydantic config"""
    return ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        validate_default=True,
        extra="forbid",
        use_enum_values=False,
        arbitrary_types_allowed=True,
        populate_by_name=True,
        **kwargs,  # type: ignore
    )


class InfraSysBaseModel(BaseModel):
    """Base class for all Infrastructure Systems models"""

    model_config = make_model_config()


class InfraSysBaseModelWithIdentifers(InfraSysBaseModel, abc.ABC):
    """Base class for all Infrastructure Systems types with IDs"""

    id: Optional[int] = Field(
        default=None,
        description="Unique identifier, assigned when added to a system.",
        repr=False,
    )

    @classmethod
    def example(cls) -> "InfraSysBaseModelWithIdentifers":
        """Return an example instance of the model.

        Raises
        ------
        NotImplementedError
            Raised if the model does not implement this method.
        """
        msg = f"{cls.__name__} does not implement example()"
        raise NotImplementedError(msg)

    @property
    def label(self) -> str:
        """Provides a description of an instance."""
        class_name = self.__class__.__name__
        name = getattr(self, "name", "") or str(self.id)
        return make_label(class_name, name)


def make_label(class_name: str, name: str) -> str:
    """Make a string label of an instance."""
    return f"{class_name}.{name}"


def get_class_and_name_from_label(label: str) -> tuple[str, str | int]:
    """Return the class and name from a label.
    If the name is a stringified ID, it will be converted to an int.
    """
    class_name, name = label.split(".", maxsplit=1)
    name_or_id: str | int = name
    try:
        name_or_id = int(name)  # TODO DT
    except ValueError:
        pass

    return class_name, name_or_id
