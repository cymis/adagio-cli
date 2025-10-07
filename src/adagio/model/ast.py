import typing as t
from pydantic import BaseModel, Field


class TypeASTUnion(BaseModel):
    type: t.Literal['union']
    members: list['TypeAST']


# Intersections exist, but are immediately reduced (or otherwise empty)
class TypeASTIntersection(BaseModel):
    type: t.Literal['intersection']
    members: list['TypeAST']


class TypeASTExpression(BaseModel):
    type: t.Literal['expression']
    builtin: bool
    name: str
    predicate: t.Optional['TypeASTPredicate']
    fields: list['TypeAST']


TypeASTPredicate = t.Annotated[
    t.Union['TypeASTPredicateChoices', 'TypeASTPredicateRange',
            'TypeASTPredicateProperties'],
    Field(discriminator='name')
]


class TypeASTPredicateBase(BaseModel):
    type: t.Literal['predicate']


class TypeASTPredicateChoices(TypeASTPredicateBase):
    name: t.Literal['Choices']
    choices: list[str | bool]


class TypeASTPredicateRange(TypeASTPredicateBase):
    name: t.Literal['Range']
    range: tuple[int, int] | tuple[float, float]
    inclusive: tuple[bool, bool]


class TypeASTPredicateProperties(TypeASTPredicateBase):
    name: t.Literal['Properties']
    include: list[str]
    exclude: list[str]

TypeAST = t.Annotated[
    t.Union[TypeASTUnion, TypeASTIntersection, TypeASTExpression],
    Field(discriminator='type')
]

