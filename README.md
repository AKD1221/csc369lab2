Andy Do
CSC 369 Lab 2
Professor Migler

Did not change any of the reduce phases since all of them require just summing up the values given from the map phase. Outlined below are the key and values that we emit from our map phase as well as anything extra we do such as sorting by ascending.

Changed a few of the dates in the given acces.log file to test for Calendar month/year for part5.

Part 1:
- Key is sa[6] which becomes URL path
- Value is just 1 since we want count of each URL path
- Sorted by ascending by using a second mapreduce and calling AccessLog2

Part 2:
- Key is sa[8] which is the HTTP response code
- Value is just 1 since we want count
- Sorted by ascending with AccessLog2

Part 3:
- Key is hardcoded ip address which in this case is "64.242.88.10"
- Value is sa[9] which is the number of bytes sent

Part 4:
- Hardcoded URL of "/robots.txt"
- Key is sa[0] which is the IPv4 that
- Value is 1 which is used for count
- Sorted by ascending with AccessLog2

Part 5:
- Convert our datetime that we are given to extract the month number and the year number
- Our Key becomes a string of year + month (eg. Mar/2004 becomes 200403)
- Value is 1 which is used for count

Part 6:
- Convert our datetime to get the day of the week 
- Our Key becomes the day of the week as a string
- Our value is the number of bytes sent
- Sorted by ascending with AccessLog2
