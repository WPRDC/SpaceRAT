from spacerat.core import SpaceRAT
from spacerat.helpers import print_records

if __name__ == "__main__":
    # initializing without args will use an in-memory sql db to hold the model,and will load the model
    # from files found in ./model relative to the current working directory (not necessarily this file)
    rat = SpaceRAT()

    question = rat.get_question("fair-market-assessed-value")

    neighborhoods = rat.get_geog("neighborhood")

    shadyside = neighborhoods.get_region("shadyside")
    bloomfield = neighborhoods.get_region("bloomfield")

    print(question.name)

    print_records(rat.answer_question(question, shadyside))

    print_records(rat.answer_question(question, bloomfield))

    print_records(rat.answer_question(question, shadyside, variant="residential"))
