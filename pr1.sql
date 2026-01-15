5. Pig Latin Scripts for Crop Production Dataset
Dataset Loading
crop_prod = LOAD 'Datasets/crop_production.csv'
USING PigStorage(',')
AS (
    State_Name:chararray,
    District_Name:chararray,
    Crop_Year:int,
    Season:chararray,
    Crop:chararray,
    Area:float,
    Production:float
);


Explanation:
Loads the crop production dataset from a CSV file and assigns proper schema (field names and data types).

DESCRIBE crop_prod;


Displays the structure of the dataset.

(a) Calculate total production of each crop
Script
total_production = GROUP crop_prod BY Crop;

sum_production = FOREACH total_production
GENERATE
    group AS Crop,
    SUM(crop_prod.Production) AS Total_Production;

DUMP sum_production;

Explanation

GROUP crop_prod BY Crop groups all records belonging to the same crop.

SUM(crop_prod.Production) calculates total production of each crop.

DUMP displays the result.

Output format:

(Crop, Total_Production)

(b) Find the average production per year for each crop
Script
grouped_by_crop_year = GROUP crop_prod BY (Crop, Crop_Year);

average_production = FOREACH grouped_by_crop_year
GENERATE
    group.Crop AS Crop,
    group.Crop_Year AS Crop_Year,
    AVG(crop_prod.Production) AS Avg_Production;

DUMP average_production;

Explanation

Groups data by both Crop and Crop_Year.

AVG() calculates average production for each crop in each year.

Extracts crop name and year from the group key.

Output format:

(Crop, Crop_Year, Avg_Production)

(c) Filter all crops grown in Karnataka
Script
specific_state = FILTER crop_prod BY State_Name == 'Karnataka';

unique_crops = GROUP specific_state BY Crop;

DUMP unique_crops;

Explanation

Filters records where the state is Karnataka.

Groups by crop to identify distinct crops grown in Karnataka.

Only crop names (recommended for exam)
unique_crops = FOREACH (GROUP specific_state BY Crop)
GENERATE group AS Crop;

DUMP unique_crops;


Output format:

(Crop)

(d) Calculate the total area used for each crop in the year 2010
Script
specific_year = FILTER crop_prod BY Crop_Year == 2010;

total_area = GROUP specific_year BY Crop;

sum_area = FOREACH total_area
GENERATE
    group AS Crop,
    SUM(specific_year.Area) AS Total_Area;

DUMP sum_area;
