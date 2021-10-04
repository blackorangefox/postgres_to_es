import dataclasses
import datetime
import typing
import uuid


@dataclasses.dataclass
class Person:
    id: uuid.UUID
    name: str
    updated_at: datetime

    @classmethod
    def from_dict(cls, dict_: dict[str, typing.Any]) -> "Person":
        if dict_:
            return cls(id=dict_["id"], name=dict_["name"], updated_at=dict_["p_updated_at"])
        return None

    @classmethod
    def from_dict_list(cls, iterable: typing.Iterable[dict]) -> list["Person"]:
        if iterable:
            return [cls.from_dict(it) for it in iterable]
        return []


@dataclasses.dataclass
class Movie:
    id: uuid.UUID
    title: str
    description: str
    imdb_rating: float
    genres: list[str]
    writers: list[Person]
    actors: list[Person]
    director: list[Person]
    updated_at: datetime

    @classmethod
    def from_dict(cls, dict_: dict[str, typing.Any]) -> "Movie":
        return cls(
            id=dict_["id"],
            title=str(dict_["title"]),
            description=str(dict_["description"]),
            imdb_rating=float(dict_["imdb_rating"]),
            genres=list(map(str, dict_["genres"])),
            writers=Person.from_dict_list(dict_["writers"]),
            actors=Person.from_dict_list(dict_["actors"]),
            director=Person.from_dict_list(dict_["directors"]),
            updated_at=dict_["updated_at"],
        )

@dataclasses.dataclass
class Genre:
    id: uuid.UUID
    name: str
    updated_at: datetime

    @classmethod
    def from_dict(cls, dict_: dict[str, typing.Any]) -> "Genre":
        if dict_:
            return cls(id=dict_["id"], name=dict_["name"], updated_at=dict_["updated_at"])
        return None

    @classmethod
    def from_dict_list(cls, iterable: typing.Iterable[dict]) -> list["Genre"]:
        if iterable:
            return [cls.from_dict(it) for it in iterable]
        return []
