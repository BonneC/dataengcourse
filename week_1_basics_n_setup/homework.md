## Week 1 Homework

In this homework we'll prepare the environment 
and practice with terraform and SQL


## Question 1. Google Cloud SDK

Install Google Cloud SDK. What's the version you have? 

To get the version, run `gcloud --version`

Google Cloud SDK 391.0.0

## Google Cloud account 

Create an account in Google Cloud and create a project.


## Question 2. Terraform 

Now install terraform and go to the terraform directory (`week_1_basics_n_setup/1_terraform_gcp/terraform`)

After that, run

* `terraform init`
* `terraform plan`
* `terraform apply` 

Apply the plan and copy the output (after running `apply`) to the form.

It should be the entire output - from the moment you typed `terraform init` to the very end.

## Prepare Postgres 

Run Postgres and load data as shown in the videos

We'll use the yellow taxi trips from January 2021:

```bash
wget https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv
```

You will also need the dataset with zones:

```bash 
wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv
```

Download this data and put it to Postgres

## Question 3. Count records 

How many taxi trips were there on January 15?

Consider only trips that started on January 15.

```sql
SELECT COUNT(*) FROM yellow_taxi_data
WHERE EXTRACT(DAY FROM tpep_pickup_datetime) = 15;
```
**ANSWER**: 88707

```sql
select count(*)
from yellow_taxi_data
where tpep_pickup_datetime::date = '2022-1-15';
```

## Question 4. Largest tip for each day

Find the largest tip for each day. 
On which day it was the largest tip in January?
**ANSWER**: day: 29 tip:888.88
Use the pick up time for your calculations.

(note: it's not a typo, it's "tip", not "trip")
### Query for largest tip for EACH day:
```sql
SELECT
    EXTRACT(DAY FROM tpep_pickup_datetime) as day,
    MAX(tip_amount) as max_tip
FROM yellow_taxi_data
GROUP BY day
ORDER BY 
    day ASC,
    max_tip DESC;
```

### QUERY for Largest tip ever:

```sql
SELECT
    EXTRACT(DAY FROM tpep_pickup_datetime) as day,
    MAX(tip_amount) as max_tip
FROM yellow_taxi_data
GROUP BY day
ORDER BY 
    max_tip DESC
LIMIT 1;
```

```sql
select date_trunc('day', tpep_pickup_datetime) as pickup_day,
max(tip_amount) as max_tip
from yellow_taxi_data
group by 1
order by max_tip desc;
limit 1
```

## Question 5. Most popular destination

What was the most popular destination for passengers picked up 
in central park on January 14?

Use the pick up time for your calculations.

Enter the zone name (not id). If the zone name is unknown (missing), write "Unknown" 
```sql
select coalesce(dozones."Zone", 'Unknown') as zone,
count(*) as cant_trips
from yellow_taxi_data as taxi
inner join zones as puzones
on taxi."PULocationID" = puzones."LocationID"
left join zones as dozones
on taxi."DOLocationID" = dozones."LocationID"
where puzones."Zone" ilike '%central park%'
and tpep_pickup_datetime::date = '2022-01-14'
group by zone
order by cant_trips desc
limit 1;
```
**ANSWER:** Upper East Side South

## Question 6. Most expensive locations

What's the pickup-dropoff pair with the largest 
average price for a ride (calculated based on `total_amount`)?

Enter two zone names separated by a slash

For example:

"Jamaica Bay / Clinton East"

If any of the zone names are unknown (missing), write "Unknown". For example, "Unknown / Clinton East". 

```sql
select concat(coalesce(puzones."Zone", 'Unknown'),'/', coalesce(dozones."Zone", 'Unknown')) as zone,
avg(total_amount) as total
from yellow_taxi_data as taxi
left join zones as puzones
on taxi."PULocationID" = puzones."LocationID"
left join zones as dozones
on taxi."DOLocationID" = dozones."LocationID"
group by zone
order by total desc
limit 1;
```
**ANSWER:** zone: Marine Park/Floyd Bennett Field/NV avg amount: 688.35

## Submitting the solutions

* Form for submitting: https://forms.gle/yGQrkgRdVbiFs8Vd7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 26 January (Wednesday), 22:00 CET


## Solution

Here is the solution to questions 3-6: [video](https://www.youtube.com/watch?v=HxHqH2ARfxM&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb)

