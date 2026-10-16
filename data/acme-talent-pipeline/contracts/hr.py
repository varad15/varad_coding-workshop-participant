"""
Contracts for the PostgreSQL HR source tables.

Each model mirrors one table in backend/_source/schema.sql: same columns,
NOT NULL -> required field, CHECK constraints -> Field/Literal constraints.
A few domain rules beyond schema.sql are added where the business meaning is
unambiguous (marked "domain rule").
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

NonEmpty = Field(min_length=1)


class _Contract(BaseModel):
    """Base settings: reject unknown columns (schema drift), trim strings."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Team(_Contract):
    """teams(team_id PK, name NOT NULL, department NOT NULL)."""

    team_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    department: str = Field(min_length=1)


class Competency(_Contract):
    """competencies(... category CHECK IN ('Technical','Leadership','Domain'))."""

    competency_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    category: Literal["Technical", "Leadership", "Domain"]


class Employee(_Contract):
    """employees(employee_id PK, names/email/team_id/job_level/hire_date NOT NULL)."""

    employee_id: int = Field(gt=0)
    first_name: str = Field(min_length=1)
    last_name: str = Field(min_length=1)
    email: str = Field(pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")  # domain rule: valid email shape
    team_id: int = Field(gt=0)
    job_level: str = Field(min_length=1)
    hire_date: date


class Project(_Contract):
    """projects(... outcome_status CHECK IN (...), outcome_score NUMERIC(5,2))."""

    project_id: int = Field(gt=0)
    name: str = Field(min_length=1)
    team_id: int = Field(gt=0)
    outcome_status: Literal["on-time", "delayed", "at-risk", "cancelled"]
    outcome_score: Decimal = Field(ge=0, le=100, max_digits=5, decimal_places=2)  # domain rule: 0-100 score


class PerformanceReview(_Contract):
    """performance_reviews(... rating CHECK BETWEEN 1 AND 5 ...)."""

    review_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    review_period: str = Field(pattern=r"^\d{4}-H[12]$")  # domain rule: half-year period, e.g. 2025-H1
    rating: int = Field(ge=1, le=5)
    manager_comment: str | None = None
    submitted_date: date


class CompetencyAssessment(_Contract):
    """competency_assessments(... proficiency/required CHECK BETWEEN 1 AND 5 ...)."""

    assessment_id: int = Field(gt=0)
    employee_id: int = Field(gt=0)
    competency_id: int = Field(gt=0)
    proficiency_level: int = Field(ge=1, le=5)
    required_level: int = Field(ge=1, le=5)
    assessed_date: date


class TalentReview(_Contract):
    """talent_review(employee_id PK, ratings CHECK IN (...))."""

    employee_id: int = Field(gt=0)
    review_cycle: str = Field(pattern=r"^\d{4}-H[12]$")
    potential_rating: Literal["Low", "Medium", "High"]
    promotion_readiness: Literal["Ready Now", "1-2 Years", "Not Ready"]
    attrition_risk: Literal["Low", "Medium", "High"]


# Table name -> (contract, primary key). Load order follows schema.sql.
HR_TABLES: dict[str, tuple[type[_Contract], str]] = {
    "teams": (Team, "team_id"),
    "competencies": (Competency, "competency_id"),
    "employees": (Employee, "employee_id"),
    "projects": (Project, "project_id"),
    "performance_reviews": (PerformanceReview, "review_id"),
    "competency_assessments": (CompetencyAssessment, "assessment_id"),
    "talent_review": (TalentReview, "employee_id"),
}

# Child table -> [(column, parent table, parent key)] from schema.sql REFERENCES.
HR_FOREIGN_KEYS: dict[str, list[tuple[str, str, str]]] = {
    "employees": [("team_id", "teams", "team_id")],
    "projects": [("team_id", "teams", "team_id")],
    "performance_reviews": [("employee_id", "employees", "employee_id")],
    "competency_assessments": [
        ("employee_id", "employees", "employee_id"),
        ("competency_id", "competencies", "competency_id"),
    ],
    "talent_review": [("employee_id", "employees", "employee_id")],
}
