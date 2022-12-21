# Flink log join

## Assignment

You have to process 3 files that are located in the `source` directory in the Flink framework(1.13 or greater) ideally in Scala or Java. Each file contains parts of the data. `clicks.csv` contains information about the click, `imps.csv` contains information about the impression `feedback.csv` contains additional information. Datasets contains a unique combination (`timestamp`, `random_id`) which is common for each data-set. This combination allows joining data together.

### Note

Data in all `sources` are sorted by `timestamp`.

### Questions to think about

- What if the source of the data would be endless?
- What to do for resiliency?
- What could you do if data is not always sorted?
- What to do when there is more data in one source than in other sources?


**Please do not fork this repo. Thank you.**

## Result
The expected result is one log with joined data with a click (set to true) and another log with joined data without a click (click set to false) see `expected-result`.

## Example

### Attributes
- `timestamp` = timestamp in us
- `random_id` = signed int
- other `id`s  = unsigned int
- `feedtag` = byte
- `clicked` = boolean
- `url_hash` = long
- `domain_hash` = long

`clicks.csv`
```
timestamp,random_id
1649785352762381,1724389159
1649785352762831,1055949134
...
```

`imps.csv`
```
timestamp,random_id,user_id,campaign_id,group_id,ad_id
1649785352762381,1724389159,748,16,4607,16217
1649785352762831,1055949134,1958,406,5626,53946
...
```

`feedback.csv`
```
timestamp,random_id,url_hash,domain_hash,zone_id,feedtag
1649785352762381,1724389159,13470129417977618685,11737601434122288137,1352,3
1649785352041609,3223495286,764363182377071515,5384052823623324490,1352,1
...
```

`result_click.csv`
```
timestamp,random_id,user_id,campaign_id,group_id,ad_id,url_hash,domain_hash,zone_id,feedtag,clicked
1649785352762381,1724389159,748,16,4607,16217,13470129417977618685,11737601434122288137,1352,3,true
1649785352762831,1055949134,1958,406,5626,53946,15830714610378756911,15021523854882141607,4574,3,true
...
```

`result_non_click.csv`
```
timestamp,random_id,user_id,campaign_id,group_id,ad_id,url_hash,domain_hash,zone_id,feedtag,clicked
1649785352797902,2320347770,937,59,7751,31998,16594798157065964581,312204679170904088,5470,1,false
1649785352798186,3339103949,4593,739,205,96148,16817926543743970086,16998124278590009912,6507,1,false
...
```
