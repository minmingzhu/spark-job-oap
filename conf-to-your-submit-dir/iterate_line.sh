last_number=-1
while read -r number
do
	if ((number-last_number != 1));then
		echo "non-consecutive number is $last_number"
	fi
	last_number=$number
done < "$1"	
