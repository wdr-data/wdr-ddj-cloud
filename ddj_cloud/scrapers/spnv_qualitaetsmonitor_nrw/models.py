from __future__ import annotations

import abc
from typing import Annotated, Any, Literal, Never, TypeAlias

from annotated_types import Len  # noqa: TCH002
from pydantic import BaseModel, Field


class Filters:
    class ItemBase(BaseModel, abc.ABC):
        class Config:
            arbitrary_types_allowed = True

        status: str
        selected: bool
        children: Annotated[list[Never], Len(max_length=0)]

    class ItemInt(ItemBase):
        title: int

    class ItemString(ItemBase):
        title: str

    class TargetBase(BaseModel, abc.ABC): ...

    class TargetYear(TargetBase):
        target: Literal["year"]
        items: list[Filters.ItemInt]

    class TargetQuarter(TargetBase):
        target: Literal["quarter"]
        items: list[Filters.ItemInt]

    class TargetRegion(TargetBase):
        target: Literal["region"]
        items: list[Filters.ItemString]

    class TargetEvu(TargetBase):
        target: Literal["evu"]
        items: list[Filters.ItemString]

    class TargetProductType(TargetBase):
        target: Literal["product_type"]
        items: list[Filters.ItemString]

    class TargetLines(TargetBase):
        target: Literal["lines"]
        items: list[Filters.ItemString]

    class TargetComplexity(TargetBase):
        target: Literal["complexity"]
        items: list[Filters.ItemInt]

    Target: TypeAlias = (
        TargetYear
        | TargetQuarter
        | TargetRegion
        | TargetEvu
        | TargetProductType
        | TargetLines
        | TargetComplexity
    )

    class Data(BaseModel):
        targets: list[
            Annotated[
                Filters.Target,
                Field(discriminator="target"),
            ]
        ]

    @staticmethod
    def from_json(json: list[dict[str, Any]]) -> Data:
        return Filters.Data.model_validate({"targets": json})

    # class Data(BaseModel):
    #     year: Filters.TargetYear
    #     quarter: Filters.TargetQuarter
    #     region: Filters.TargetRegion
    #     evu: Filters.TargetEvu
    #     product_type: Filters.TargetProductType
    #     lines: Filters.TargetLines
    #     complexity: Filters.TargetComplexity

    # @staticmethod
    # def from_json(json: list[dict[str, Any]]) -> Data:
    #     data_dict: dict[str, Filters.TargetBase] = {}

    #     for item in json:
    #         for model in Filters.TargetBase.__subclasses__():
    #             with contextlib.suppress(ValidationError):
    #                 data_dict[item["target"]] = model.model_validate(item)

    #     return Filters.Data.model_validate(data_dict)


class Results:
    class ColumnBase(BaseModel, abc.ABC):
        year: int

    class ColumnOverallRanking(ColumnBase):
        quarters: list[float]

    class ColumnComplexity(ColumnBase):
        quarters: list[int]

    class ColumnPunctuality(ColumnBase):
        quarters: list[float]

    class ColumnReliability(ColumnBase):
        quarters: list[float]

    class ColumnTrainFormation(ColumnBase):
        quarters: list[float]

    class ColumnPassengers(ColumnBase):
        quarters: list[int]

    Column: TypeAlias = (
        ColumnOverallRanking
        | ColumnComplexity
        | ColumnPunctuality
        | ColumnReliability
        | ColumnTrainFormation
        | ColumnPassengers
    )

    class Data(BaseModel):
        id: int = Field(alias="_id")
        evu: str = Field(alias="_evu")
        evutooltip: str = Field(alias="_evutooltip")
        producttype: str = Field(alias="_producttype")
        client: str = Field(alias="_client")
        fullname: str = Field(alias="_fullname")
        subnet: str = Field(alias="_subnet")
        runtime: str = Field(alias="_runtime")
        line_stations: str = Field(alias="_line_stations")
        line: str
        ranking: int
        overall_ranking: Results.ColumnOverallRanking
        complexity: Results.ColumnComplexity
        punctuality: Results.ColumnPunctuality
        reliability: Results.ColumnReliability
        train_formation: Results.ColumnTrainFormation
        passengers: Results.ColumnPassengers
