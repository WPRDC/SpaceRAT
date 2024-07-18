from sqlalchemy import create_engine

from spacerat.core import SpaceRAT

if __name__ == "__main__":
    # initializing without args will use an in-memory sql db to hold the model,and will load the model
    # from files found in ./model relative to the current working directory (not necessarily this file)
    rat = SpaceRAT(engine=create_engine("sqlite:///spacerat.db"))

    # assessment value across neighborhoods
    rat.answer_question("fair-market-assessed-value", "neighborhood")
