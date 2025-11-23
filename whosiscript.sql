LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/whosis.csv'
INTO TABLE whosis
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

use whois_data;

ALTER TABLE whosis MODIFY Low VARCHAR(20);
ALTER TABLE whosis MODIFY high VARCHAR(20);


select * from whosis;

-- create a new table to work with
create table whosis1 like whosis;
insert whosis1 select * from whosis;

select*from whosis1; 




-- find and delete duplicates
with whosisduplicate as (
select *, row_number() over(partition by id, indicatorcode, spatialdimtype, spatialdim, timedimtype, parentlocationcode, parentlocation, dim1type, dim1, timedim, `value`, numericvalue, low, high, `date`, timedimensionvalue, timedimensionbegin, timedimensionend) as data_id from whosis1
)
select * from whosisduplicate
where data_id > 1;

-- delete irrelevant empty columns 
alter table whosis1 
drop column dim2type,
drop column dim2,
drop dim3type,
drop dim3,
drop datasourcedimtype,
drop datasourcedim,
drop comments;

select*from whosis1; 

-- spot empty cells and populate
#create a new table
create table whosis3 like whosis;
insert whosis3 select * from whosis;

alter table whosis3
drop column dim2type,
drop column dim2,
drop dim3type,
drop dim3,
drop datasourcedimtype,
drop datasourcedim,
drop comments;

update whosis1
join whosis3 on whosis1.id = whosis3.id
set whosis1.parentlocationcode = whosis3.spatialdim,
whosis1.spatialdim = whosis3.spatialdim
where whosis1.spatialdimtype in('region', 'worldbankincomegroup','global')
;
drop table whosis2;

UPDATE whosis1
SET spatialdim = NULL
WHERE spatialdimtype in('region', 'worldbankincomegroup','global');

select * from whosis1 where spatialdimtype in('region', 'worldbankincomegroup','global');
drop table whosis3;
select * from whosis1;

#remove unwanted cells
select * from whosis1 where spatialdimtype is null;
update whosis1 
set spatialdimtype =  null
where spatialdimtype = 'country';

#populate parentlocation
select distinct parentlocationcode, parentlocation from whosis1 where parentlocation is null or parentlocation = ''
order by 1;
select distinct parentlocationcode, parentlocation from whosis1 where parentlocation is not null or parentlocation !='';

update whosis1
set parentlocation = 
case
parentlocationcode
when 'afr' then 'Africa'
when 'sear' then 'South-East Asia'
when 'eur' then 'Europe'
when 'wpr' then 'Western Pacific'
when 'amr' then 'America'
when 'emr' then 'Eastern Mediterranean'
else parentlocation 
end
where parentlocation is null or parentlocation = '' ;

select parentlocationcode, parentlocation from whosis1;

-- rearrange columns
ALTER TABLE whosis1
MODIFY timedim int AFTER timedimtype;
alter table whosis1 
drop column timedimensionvalue;
alter table whosis1 
drop column timedimtype,
drop column timedim;

select * from whosis1 where dim1 != 'sex_mle' and  dim1 != 'sex_fmle';

update whosis1
set timedimensionbegin = date(timedimensionbegin),
timedimensionend = date(timedimensionend);

ALTER TABLE whosis1
MODIFY timedimensionbegin DATE;
ALTER TABLE whosis1
MODIFY timedimensionend DATE;

select * from whosis1 where `date` is null or '';
select parentlocation, row_number()over(partition by parentlocation) as region_count
 from whosis1
order by parentlocation desc;

WITH numbered AS (
    SELECT parentlocation,
           ROW_NUMBER() OVER(PARTITION BY parentlocation ORDER BY parentlocation) AS rn
    FROM whosis1
)
SELECT parentlocation, MAX(rn) AS region_count
FROM numbered
GROUP BY parentlocation
ORDER BY region_count DESC;

#select parentlocationcode,parentlocation from whosis1 where parentlocationcode='global';

select * from whosis1;
describe whosis1;


