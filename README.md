# 🚀🐀 SpaceRAT
**Spatial**-**R**elational **A**ggregation **T**oolkit


## Example uses

```python
from spacerat.core import SpaceRAT
from spacerat.helpers import print_records
# initializing without args will use an in-memory sql db to hold the model,and will load the model
# from files found in ./model relative to the current working directory (not necessarily this file)
rat = SpaceRAT()

question = rat.get_question("fair-market-assessed-value")

neighborhoods = rat.get_geog("neighborhood")

shadyside = neighborhoods.get_region("shadyside")
bloomfield = neighborhoods.get_region("bloomfield")

print(question)
# Question(id='fair-market-assessed-value', name='Fair Market Assessed Value', datatype='continuous')

# Get stats on fair market assessment value for Shadyside at current time. 
print_records(rat.answer_question(question, shadyside))
# 2024-06-01T00:00:00
#   - mean: 516796.628695209
#   - mode: 250000.0
#   - min: 0.0
#   - first_quartile: 147300.0
#   - median: 260000.0
#   - third_quartile: 426775.0
#   - max: 126459400.0
#   - stddev: 2985002.463899841
#   - sum: 2027909971.0
#   - n: 3924


# Same thing but for Bloomfield
print_records(rat.answer_question(question, bloomfield))
# 2024-06-01T00:00:00
#   - mean: 228085.7476367803
#   - mode: 150000.0
#   - min: 0.0
#   - first_quartile: 66950.0
#   - median: 102700.0
#   - third_quartile: 171100.0
#   - max: 74945100.0
#   - stddev: 1596977.2907357663
#   - sum: 796247345.0
#   - n: 3491

# Shadyside again but only Residential parcels
print_records(rat.answer_question(question, shadyside, variant="residential"))
# 2024-06-01T00:00:00
#   - mean: 300894.54238310707
#   - mode: 200000.0
#   - min: 0.0
#   - first_quartile: 146000.0
#   - median: 245000.0
#   - third_quartile: 383350.0
#   - max: 2500000.0
#   - stddev: 238512.67504200496
#   - sum: 997465408.0
#   - n: 3315
```