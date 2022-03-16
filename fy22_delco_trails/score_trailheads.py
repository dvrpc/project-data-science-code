import geopandas as gpd

# Read the geojson file with the query results
gdf = gpd.read_file("G:\Shared drives\FY22 Delaware County Bicycle Network Planning\Prioritization Analysis\analysis outputs\results.geojson")

# Make new columns for all the scores, setting them all to zero by defauly
gdf["score_votes"] = 0
gdf["score_population"] = 0
gdf["score_park"] = 0
gdf["score_ipd"] = 0
gdf["score_employment"] = 0
gdf["score_job"] = 0
gdf["score_destination"] = 0
gdf["score_activitycenter"] = 0
gdf["score_food"] = 0
gdf["score_health"] = 0
gdf["score_college"] = 0
gdf["score_school"] = 0
gdf["score_transit"] = 0



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
    gdf.at[idx, "score_votes"] = vote_score

    # Population
    # ----------
    pop = row["population"]
    if pop > 80000:
        pop_score = 3
    elif 30000 < pop <= 80000:
        pop_score = 2
    elif 15000 < pop <= 30000:
        pop_score = 1

    gdf.at[idx, "score_population"] = pop_score

    # TODO:
    # Park distance
    park = row["park_dist"]

    if park == 0:
        park_score =  3
    elif 1 <= park <= 200:
        park_score = 2
    elif 200 < park <= 800:
        park_score = 1

    gdf.at[idx, "score_park"] = park_score

    # IPD 
    ipd = row["ipd_class"]

    if ipd == "Well Above Average":
        ipd_score =  7
    elif ipd == "Above Average":
        ipd_score = 5
    elif ipd == "Average":
        ipd_score = 3
    elif ipd == "Well Below Average":
        ipd_score = 1
    elif ipd == "Below Average":
        ipd_score = 1

    gdf.at[idx, "score_ipd"] = ipd_score

    # Employment
    emp = row["num_employers"]

    if emp > 3000:
        emp_score =  3
    elif 2000 < emp <= 3000:
        emp_score = 2
    elif 500 < emp <= 2000:
        emp_score = 1

    gdf.at[idx, "score_employment"] = emp_score

    # Jobs
    jobs = row["num_jobs"]

    if jobs > 15000:
        jobs_score =  3
    elif 10000 < jobs <= 15000:
        jobs_score = 2
    elif 5000 < jobs <= 10000:
        jobs_score = 1


    gdf.at[idx, "score_jobs"] = jobs_score

    # Destinations
    dest = row["number_of_destination_types"]

    if 5 <= dest <= 6:
        dest_score =  3
    elif 3 <= dest <= 4:
        dest_score = 2
    elif 1 <= dest <= 2:
        dest_score = 1


    gdf.at[idx, "score_destination"] = dest_score

    # Activity centers for seniors or disabled

    act = row["places_activitycenterforseniorsordisabled"]

    if 8 <= act <= 10:
        act_score =  3
    elif 4 <= act <= 7:
        act_score = 2
    elif 1 <= act <= 3:
        act_score = 1


    gdf.at[idx, "score_activitycenter"] = act_score

    # Food 

    food = row["places_foodstore"]

    if food > 34:
        food_score =  3
    elif 10 < food <= 34:
        food_score = 2
    elif 1 <= food <= 10:
        food_score = 1


    gdf.at[idx, "score_food"] = food_score

    # Health
   
    health = row["places_healthfacility"]

    if health > 9:
        health_score =  3
    elif 4 < food <= 9:
        health_score = 2
    elif 1 <= health <= 4:
        health_score = 1


    gdf.at[idx, "score_health"] = health_score

    # Colleges - This might not be the best way to handle it.
    college = row["places_school_college_university"]
    if 1 <= college <= 4:
        college_score = 4

    gdf.at[idx, "score_college"] = college_score 

    # Public and private schools - how do I referenced the sum of both fields?
    school = row["places_school"]

    if school > 9:
        school_score =  5
    elif 8 <= school <= 9:
        school_score = 4
    elif 5 <= school <= 7:
        school_score = 3
    elif 3 <= school <= 4:
        school_score= 2
    elif 1 <= school <= 2:
        school_score= 1
       
    gdf.at[idx, "score_school"] = school_score 
    
    # Transit

    transit = row["number_of_transit_routes"]

    if transit > 12:
       transit_score =  5
    elif 10 <= transit <= 12:
        transit_score = 4
    elif 8 <= transit <= 9:
        transit_score = 3
    elif 4 <= transit <= 7:
        transit_score= 2
    elif 1 <= transit <= 3:
        transit_score= 1
       
    gdf.at[idx, "score_transit"] = transit_score 

# After iterating over all rows, write to new file
gdf.to_file("./results_scored.geojson")
