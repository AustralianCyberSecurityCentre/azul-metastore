"""Query information associated with an Opensearch query."""

from pydantic import BaseModel


class IngestError(BaseModel):
    """Error when processing document into opensearch.

    Can be an opensearch error or an application error.
    """

    doc: dict | BaseModel
    error_type: str
    error_reason: str
