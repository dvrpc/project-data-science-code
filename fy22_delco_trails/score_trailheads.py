import geopandas as gpd

# Read the geojson file with the query results
gdf = gpd.read_file("./results.geojson")

# Make new columns for all the scores, setting them all to zero by defauly
gdf["score_votes"] = 0
gdf["score_population"] = 0


# Iterate over every row in the dataset
for idx, row in gdf.iterrows():

    # Votes
    # -----
    # A: get the raw vote data for this row
    votes = row["votes"]

    # B: apply the scoring logic
    if votes == 5:
        vote_score = 25
    elif votes == 4:
        vote_score = 15
    elif 2 <= votes <= 3:
        vote_score = 10
    elif votes == 1:
        vote_score = 5

    # C: add the score to the row in the new column
    gdf["score_votes"] = vote_score

    # Population
    # ----------
    pop = row["population"]
    if pop > 80000:
        pop_score = 3
    elif 30000 < pop <= 80000:
        pop_score = 2
    # etc...

    # TODO:
    # Park distance
    # IPD
    # Employment
    # Jobs
    # Destinations ?? activity centers ??
    # Colleges
    # Public and private schools
    # Transit


# After iterating over all rows, write to new file
gdf.to_file("./results_scored.geojson")
