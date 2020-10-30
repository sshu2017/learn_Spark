cat fakefriends.csv | cut -d"," -f3,4 | awk -f average_by_key1.awk 
