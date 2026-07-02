import typing as t
from pydantic import BaseModel, Field, field_validator


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
    range: tuple[int | None, int | None] | tuple[float | None, float | None]
    inclusive: tuple[bool, bool]

    @field_validator('range', mode='before')
    @classmethod
    def validate_range_bounds(cls, value):
        if not isinstance(value, (list, tuple)) or len(value) != 2:
            return value
        lower, upper = value
        if lower is None and upper is None:
            raise ValueError('Range must include at least one bound.')
        bounds = [bound for bound in (lower, upper) if bound is not None]
        if len(bounds) == 2 and type(bounds[0]) is not type(bounds[1]):
            raise ValueError('Range bounds must use the same numeric type.')
        return value


class TypeASTPredicateProperties(TypeASTPredicateBase):
    name: t.Literal['Properties']
    include: list[str]
    exclude: list[str]

TypeAST = t.Annotated[
    t.Union[TypeASTUnion, TypeASTIntersection, TypeASTExpression],
    Field(discriminator='type')
]
