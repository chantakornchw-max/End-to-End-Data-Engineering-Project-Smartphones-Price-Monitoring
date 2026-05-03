# End-to-End Data Engineering Project: Smartphones Price Monitoring

ctk
---

**fluk chantakorn**

_ฉันทกร_

## quotes
>They said "Stay where you're valued" then i come to you
>
>**ctk**


## insert image
[micky mouse](https://charactercommunity.fandom.com/wiki/Mickey_Mouse)


## insert image
![micky mouse](https://static.wikia.nocookie.net/charactercommunity/images/d/d9/Mickey.png/revision/latest?cb=20200505132139)


## List
- NFC
    - New york Giants
    - Washington Commanders
    - Philadelphia Eagles
    - Dallas Cowboys

## create table
|Name|Position|
|:----:|:----:|
|Lebron James|PF|
|Kevin Durant|PF|
---

## insert code block
```M Language 
dim_date = 
	VAR startYear = YEAR (MIN(tSuperstore[Order_date]) ) 
	VAR endYear = YEAR(MAX(tSuperstore[Order_date]) )
	RETURN
	ADDCOLUMNS (
	CALENDAR(
	DATE(startYear,1,1),
	DATE(endYear,12,31)
	),
	"Year", YEAR([Date]),
    "Quater", "Q" & FORMAT([Date], "q"),
    "QuarterID", QUARTER([Date]),
	"Month", FORMAT([Date], "mmm"),
	"MonthID", MONTH([Date]),
	"MonthYear", FORMAT([Date], "mmm yyyy"),
	"MonthYearID", INT(FORMAT([Date], "yyyymm")), 
	"QuarterYear", "Q" & FORMAT([Date], "q yyyy"),
	"QuarterYearID", INT(FORMAT([Date], "yyyyq")),
    "Days of Week", FORMAT([Date], "ddd"),
    "DayOfWeekID", WEEKDAY([Date], 1)
	)
```