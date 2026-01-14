from pyspark.sql.functions import col, mean

from jobs.read_data import read_parquet_data
from jobs.write import write_csv


def get_reviews():
    listing_data = read_parquet_data("./output/extracted_data/listing")

    average_review_score_value = (listing_data
        .groupby("city")
        .agg(
            mean("review_scores_value"),
            mean("review_scores_rating"),
            mean("review_scores_accuracy"),
            mean("review_scores_cleanliness"),
            mean("review_scores_checkin"),
        ))

    write_csv(average_review_score_value, "./output/reviews")


if __name__ == '__main__':
    print("Running Review Job")
    get_reviews()
    print("Review Job Completed")

COLUMNS = [
    "id",
    "host_acceptance_rate",
    "host_response_rate",
    "host_response_time",
    "review_scores_value",
    "review_scores_accuracy",
    "review_scores_cleanliness",
    "review_scores_checkin",
    "review_scores_communication",
    "review_scores_location",
    "review_scores_value",
    "number_of_reviews",
    "number_of_reviews_ltm",
    "number_of_reviews_l30d",
    "number_of_reviews_ly",
]