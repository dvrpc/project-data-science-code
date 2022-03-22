from pathlib import Path
import geopandas as gpd

data_folder = Path(
    r"G:\Shared drives\FY22 Delaware County Bicycle Network Planning\Prioritization Analysis\analysis outputs"
)

# Read the geojson file with the query results
gdf = gpd.read_file(data_folder / "results.geojson")

# Make new columns for all the scores, setting them all to zero by defauly
gdf["s_votes"] = 0
gdf["s_population"] = 0
gdf["s_park"] = 0
gdf["s_ipd"] = 0
gdf["s_employment"] = 0
gdf["s_job"] = 0
gdf["s_destination"] = 0
gdf["s_activitycenter"] = 0
gdf["s_food"] = 0
gdf["s_health"] = 0
gdf["s_college"] = 0
gdf["s_school"] = 0
gdf["s_transit"] = 0


# Iterate over every row in the dataset
for idx, row in gdf.iterrows():

    # Votes
    # -----
    # A: get the raw vote data for this row
    votes = row["votes"]

    # B: apply the scoring logic
    if votes >= 4:
        vote_score = 25
    elif votes == 3:
        vote_score = 15
    elif votes == 2:
        vote_score = 10
    elif votes == 1:
        vote_score = 5
    else:
        vote_score = 0

    # C: add the score to the row in the new column
    gdf.at[idx, "s_votes"] = vote_score

    # Population
    # ----------
    pop = float(row["population"])
    if pop > 80000:
        pop_score = 3
    elif 30000 < pop <= 80000:
        pop_score = 2
    elif 15000 < pop <= 30000:
        pop_score = 1
    else:
        pop_score = 0

    gdf.at[idx, "s_population"] = pop_score

    # Park distance
    park = float(row["park_dist"])

    if park == 0:
        park_score = 3
    elif 0 < park <= 200:
        park_score = 2
    elif 200 < park <= 800:
        park_score = 1
    else:
        park_score = 0

    gdf.at[idx, "s_park"] = park_score

    # IPD
    ipd = row["ipd_class"]

    if ipd == "Well Above Average":
        ipd_score = 7
    elif ipd == "Above Average":
        ipd_score = 5
    elif ipd == "Average":
        ipd_score = 3
    elif ipd in ["Well Below Average", "Below Average"]:
        ipd_score = 1
    else:
        ipd_score = 0

    gdf.at[idx, "s_ipd"] = ipd_score

    # Employment
    emp = float(row["num_employers"])

    if emp > 3000:
        emp_score = 3
    elif 2000 < emp <= 3000:
        emp_score = 2
    elif 500 < emp <= 2000:
        emp_score = 1
    else:
        emp_score = 0

    gdf.at[idx, "s_employment"] = emp_score

    # Jobs
    jobs = float(row["num_jobs"])

    if jobs > 15000:
        jobs_score = 3
    elif 10000 < jobs <= 15000:
        jobs_score = 2
    elif 5000 < jobs <= 10000:
        jobs_score = 1
    else:
        jobs_score = 0

    gdf.at[idx, "s_jobs"] = jobs_score

    # Destinations
    dest = float(row["number_of_destination_types"])

    if 5 <= dest <= 6:
        dest_score = 3
    elif 3 <= dest <= 4:
        dest_score = 2
    elif 1 <= dest <= 2:
        dest_score = 1
    else:
        dest_score = 0

    gdf.at[idx, "s_destination"] = dest_score

    # Activity centers for seniors or disabled

    act = row["places_activitycenterforseniorsordisabled"]
    if act is None:
        act = 0
    else:
        act = float(act)

    if 8 <= act <= 10:
        act_score = 3
    elif 4 <= act <= 7:
        act_score = 2
    elif 1 <= act <= 3:
        act_score = 1
    else:
        act_score = 0

    gdf.at[idx, "s_activitycenter"] = act_score

    # Food

    food = row["places_foodstore"]
    if food is None:
        food = 0
    else:
        food = float(food)

    if food > 34:
        food_score = 3
    elif 10 < food <= 34:
        food_score = 2
    elif 1 <= food <= 10:
        food_score = 1
    else:
        food_score = 0

    gdf.at[idx, "s_food"] = food_score

    # Health

    health = row["places_healthfacility"]
    if health is None:
        health = 0
    else:
        health = float(health)

    if health > 9:
        health_score = 3
    elif 4 < food <= 9:
        health_score = 2
    elif 1 <= health <= 4:
        health_score = 1
    else:
        health_score = 0

    gdf.at[idx, "s_health"] = health_score

    # Colleges
    colleges = row["higher_ed_college"]
    other_higher_ed = row["higher_ed_other"]

    if colleges:
        colleges = float(colleges)
    else:
        colleges = 0

    if other_higher_ed:
        other_higher_ed = float(other_higher_ed)
    else:
        other_higher_ed = 0

    if colleges > 0:
        college_score = 4
    elif colleges == 0 and other_higher_ed > 0:
        college_score = 3
    else:
        college_score = 0

    gdf.at[idx, "s_college"] = college_score

    # Public and private schools
    public_school = row["places_school_public"]
    private_school = row["places_school_private"]

    if public_school is None:
        public_school = 0
    if private_school is None:
        private_school = 0

    school = float(public_school) + float(private_school)
    if school > 9:
        school_score = 5
    elif 8 <= school <= 9:
        school_score = 4
    elif 5 <= school <= 7:
        school_score = 3
    elif 3 <= school <= 4:
        school_score = 2
    elif 1 <= school <= 2:
        school_score = 1
    else:
        school_score = 0

    gdf.at[idx, "s_school"] = school_score

    # Transit

    transit = float(row["number_of_transit_routes"])

    if transit > 12:
        transit_score = 5
    elif 10 <= transit <= 12:
        transit_score = 4
    elif 8 <= transit <= 9:
        transit_score = 3
    elif 4 <= transit <= 7:
        transit_score = 2
    elif 1 <= transit <= 3:
        transit_score = 1
    else:
        transit_score = 0

    gdf.at[idx, "s_transit"] = transit_score

# Sum up the final score
gdf["final_score"] = (
    gdf["s_votes"]
    + gdf["s_population"]
    + gdf["s_park"]
    + gdf["s_ipd"]
    + gdf["s_employment"]
    + gdf["s_job"]
    + gdf["s_destination"]
    + gdf["s_activitycenter"]
    + gdf["s_food"]
    + gdf["s_health"]
    + gdf["s_college"]
    + gdf["s_school"]
    + gdf["s_transit"]
)


# After iterating over all rows, write to new file
gdf.to_file(data_folder / "results_scored.geojson", driver="GeoJSON")

df = gdf.drop(columns=["geometry"])
df.to_csv(data_folder / "scored_results_table.csv")
