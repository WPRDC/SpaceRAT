from profiler import db
from profiler.model import Question, TimeAxis, DataType, Region
from profiler.types import QuestionResultsRow


def _aggregate_fields(datatype: DataType) -> str:
    if datatype == "continuous":
        return """
          AVG(value)                                            as mean,
          MODE() WITHIN GROUP (ORDER BY value)                  as mode,
    
          MIN(value)                                            as min,
          percentile_cont(0.25) WITHIN GROUP ( ORDER BY value ) as first_quartile,
          percentile_cont(0.5) WITHIN GROUP ( ORDER BY value )  as median,
          percentile_cont(0.75) WITHIN GROUP ( ORDER BY value ) as third_quartile,
          MAX(value)                                            as max,
    
          stddev_pop(value)                                     as stddev,
    
          SUM(value)                                            as sum,
          COUNT(*)                                              as n
        """

    # discrete can only do mode and count
    return """
      MODE() WITHIN GROUP (ORDER BY value) as mode,
      COUNT(value)                         as n
    """


def answer_question(
    question: Question, region: Region, time_axis: TimeAxis
) -> QuestionResultsRow:
    """Finds result for question at geog across points in time axis."""
    # parse geog str

    # find subgeog that works for this question, could be region if it directly answers it
    subgeog = region.geog_level.get_subgeography_for_question(question)

    # get set of regions of with subgeog type that fit within region
    region_list = list(region.get_subregions(subgeog))
    no_spatial_agg = bool(len(region_list) == 1)

    # get query to get question table at this level
    geog = region_list[0].geog_level
    question_source = question.get_source(geog)
    source_query = question_source.source_query

    #  truncate time to spatial resolution to ensure clean aggregation
    temporal_resolution = question_source.source.temporal_resolution

    # determine fields to request based on datatype
    if no_spatial_agg:
        fields = "MIN(value) as value"  # will only have one result so same as value
    else:
        fields = _aggregate_fields(question.datatype)

    # query the datastore for the question, aggregating data from smaller subregions if necessary
    # returns a set of records representing the answers for the question for the region across time
    # with a granularity specified in the questions `temporal resolution`

    region_ids = [r.feature_id for r in region_list]
    group_by = "time"
    time_selection = "time"

    time_filter = (
        f""" AND "time" {time_axis.domain_filter}""" if time_axis.domain_filter else ""
    )

    # roll up time
    #   if possible (not aggregating spatially)
    #   and requested (based on time axis resolution vs question's temporal resolution)
    if no_spatial_agg and temporal_resolution != time_axis.resolution:
        group_by += ", region"
        time_selection = "MIN(time) as start_time, MAX(time) as end_time"

    values: list[QuestionResultsRow] = db.query(
        f"""
      SELECT {time_selection}, {fields}
      FROM (SELECT date_trunc('{temporal_resolution}', "time") as "time", 
                   "value"
             FROM ({source_query}) as base      
             WHERE "region" = ANY(%s) {time_filter}) as filtered
      GROUP BY {group_by}
      """,
        (region_ids,),
    )

    return values[0]
