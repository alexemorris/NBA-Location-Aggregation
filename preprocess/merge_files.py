
import glob
import csv
players = glob.glob('~/moments/*csv')
rows = []
for i in players:
	with open(i, 'r') as my_file:
		filereader = csv.reader(my_file, delimiter = ',') rows.extend(list(filereader))
		With open('~/moments/all_moments.csv', 'w') as output: 
			writer = csv.writer(output, delimiter =',') writer.writerows(rows)
