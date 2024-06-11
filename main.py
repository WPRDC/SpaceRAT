from sqlalchemy import select, create_engine
from sqlalchemy.orm import Session

from profiler.config import init_db
from profiler.core import answer_question
from profiler.model import Question, Region, Geography, TimeAxis
from profiler.helpers import print_record


# file_engine = create_engine("sqlite:///spacerat.db")

if __name__ == "__main__":
    engine = init_db()

    with Session(engine) as session:
        question = session.scalars(select(Question)).first()
        print(question)
        shadyside = (
            session.scalars(select(Geography).where(Geography.id.ilike("neighborhood")))
            .first()
            .get_region("shadyside")
        )
        result = answer_question(
            question, shadyside, TimeAxis(resolution="month", domain="current")
        )

        print_record(result)
